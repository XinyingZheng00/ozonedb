#ifdef OZONEDB_ENABLE_S3
#include "s3_storage.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <cstring>
#include <iostream>
#include <sstream>

namespace ozonedb {

namespace {

std::atomic<int> g_aws_init_refcount{0};
std::mutex g_aws_init_mtx;
Aws::SDKOptions g_aws_options;

void ensureAwsInit() {
  std::lock_guard<std::mutex> lock(g_aws_init_mtx);
  if (g_aws_init_refcount.fetch_add(1) == 0) {
    Aws::InitAPI(g_aws_options);
  }
}

void maybeAwsShutdown() {
  std::lock_guard<std::mutex> lock(g_aws_init_mtx);
  if (g_aws_init_refcount.fetch_sub(1) == 1) {
    Aws::ShutdownAPI(g_aws_options);
  }
}

// Wrap a raw byte pointer as an AWS IOStream without extra copies on the
// upload path. The AWS SDK takes a std::shared_ptr<Aws::IOStream> for
// PutObject bodies; a stringstream backed by the buffer is the simplest
// way to hand it the bytes without rolling a custom PreallocatedStreamBuf.
std::shared_ptr<Aws::IOStream> makeBodyStream(unsigned char const* data, size_t size) {
  auto stream = Aws::MakeShared<Aws::StringStream>("S3Body");
  stream->write(reinterpret_cast<char const*>(data), static_cast<std::streamsize>(size));
  return stream;
}

}  // namespace

S3Storage::S3Storage(std::string const& endpoint,
                     std::string const& region,
                     std::string const& bucket,
                     std::string const& access_key,
                     std::string const& secret_key,
                     bool use_path_style,
                     std::string const& db_path)
    : Storage(db_path), bucket_(bucket) {
  ensureAwsInit();
  owns_aws_sdk_ = true;

  Aws::Client::ClientConfiguration cfg;
  cfg.region = region.empty() ? Aws::String("us-east-1") : Aws::String(region.c_str());
  if (!endpoint.empty()) {
    cfg.endpointOverride = Aws::String(endpoint.c_str());
    // MinIO / R2 / Wasabi / SeaweedFS: the endpoint is an HTTP host, not
    // an AWS service DNS name. Force path-style addressing so the client
    // sends `http://host:port/bucket/key` instead of
    // `http://bucket.host:port/key`. Real AWS users should set
    // use_path_style=false and leave endpoint empty.
    if (endpoint.rfind("http://", 0) == 0) {
      cfg.scheme = Aws::Http::Scheme::HTTP;
      cfg.verifySSL = false;
    }
  }

  if (!access_key.empty()) {
    Aws::Auth::AWSCredentials creds(Aws::String(access_key.c_str()),
                                    Aws::String(secret_key.c_str()));
    client_ = std::make_unique<Aws::S3::S3Client>(
        creds, cfg,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing=*/!use_path_style);
  } else {
    // Fall through to the default credential provider chain (env vars,
    // instance profile, etc.). Useful for real AWS deployments.
    client_ = std::make_unique<Aws::S3::S3Client>(
        cfg,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing=*/!use_path_style);
  }
}

S3Storage::~S3Storage() {
  // Flush any pending batched writes. This mirrors ~AzureBlobStorage's
  // cached_file drain and ~CorfuDBStorage's destructor flush path.
  std::unique_lock<std::mutex> lock(mtx_);
  if (!cached_file_.empty()) {
    std::vector<std::string> keys;
    keys.reserve(cached_file_.size());
    for (auto const& kv : cached_file_) keys.push_back(kv.first);
    lock.unlock();
    for (auto const& k : keys) {
      flushLocked(k);
    }
  }
  client_.reset();
  if (owns_aws_sdk_) maybeAwsShutdown();
}

std::string S3Storage::keyFor(std::string const& fileName) const {
  if (storage_path.empty()) return fileName;
  if (storage_path.back() == '/' || fileName.front() == '/') {
    return storage_path + fileName;
  }
  return storage_path + "/" + fileName;
}

void S3Storage::createDirectory(std::string /*name*/) {
  // S3 has no directory concept; no-op.
}

Status S3Storage::flushLocked(std::string const& fileName) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto it = cached_file_.find(fileName);
  if (it == cached_file_.end()) return Status::kNotFound;
  std::vector<unsigned char> payload = std::move(it->second);
  cached_file_.erase(it);
  lock.unlock();

  Aws::S3::Model::PutObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  req.SetBody(makeBodyStream(payload.data(), payload.size()));

  auto outcome = client_->PutObject(req);
  if (!outcome.IsSuccess()) {
    std::cerr << "[s3] PutObject failed for " << keyFor(fileName) << ": "
              << outcome.GetError().GetMessage() << "\n";
    return Status::kFailure;
  }
  return Status::kSuccess;
}

Status S3Storage::append(std::string const& fileName, unsigned char* const& data, int length) {
  // S3 has no native append. Callers that truly need atomic append
  // (log / metadata / task logs) must route those files to a different
  // backend — S3Storage is the "regular storage" tier for SSTables per
  // paper §3.5. Implement append as "buffer + immediate flush" so any
  // legacy call site that doesn't distinguish log vs sstable still works.
  //
  // flushLocked MOVES the buffer out and erases the map entry, so a naive
  // second append would PutObject only the new bytes and silently
  // truncate everything written before it. Keep the accumulated bytes and
  // re-put the whole object each time, which is the only way to emulate
  // append on a store that has no append.
  //
  // That makes append O(object size) per call. It is correct, not fast.
  // Do not route a log file here.
  std::vector<unsigned char> snapshot;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (sealed_files_.count(fileName)) return Status::kSealed;
    auto& buf = cached_file_[fileName];
    buf.insert(buf.end(), data, data + length);
    snapshot = buf;  // flushLocked moves the buffer out
  }
  Status s = flushLocked(fileName);
  if (s != Status::kSuccess) return s;
  // flushLocked erased the entry; restore it so the next append re-puts
  // the whole object instead of just its own bytes.
  {
    std::lock_guard<std::mutex> lk(mtx_);
    cached_file_[fileName] = std::move(snapshot);
  }
  return Status::kSuccess;
}

Status S3Storage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (sealed_files_.count(fileName)) return Status::kSealed;
  auto& buf = cached_file_[fileName];
  buf.insert(buf.end(), data, data + length);
  return Status::kSuccess;
}

Status S3Storage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  // For SSTable writes we just accumulate and let flush() transfer the
  // whole object in one PutObject. No timer-based partial flushes — one
  // SSTable is one object.
  return appendNoFlush(fileName, data, length);
}

Status S3Storage::flush(std::string const& fileName) {
  return flushLocked(fileName);
}

Status S3Storage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  Aws::S3::Model::GetObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  auto outcome = client_->GetObject(req);
  if (!outcome.IsSuccess()) return Status::kFailure;

  auto& body = outcome.GetResult().GetBody();
  body.seekg(0, std::ios::end);
  std::streampos total = body.tellg();
  body.seekg(0, std::ios::beg);
  size = static_cast<size_t>(total);
  data = new unsigned char[size];
  body.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
  return Status::kSuccess;
}

Status S3Storage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  Aws::S3::Model::GetObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  std::ostringstream range;
  range << "bytes=" << a << "-" << (a + length - 1);
  req.SetRange(Aws::String(range.str().c_str()));
  auto outcome = client_->GetObject(req);
  if (!outcome.IsSuccess()) return Status::kFailure;

  auto& body = outcome.GetResult().GetBody();
  data = new unsigned char[length];
  body.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(length));
  return Status::kSuccess;
}

size_t S3Storage::size(std::string fileName) {
  {
    // Pending bytes in the local buffer haven't been PUT yet; callers
    // reading a just-written file must still see the full size.
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = cached_file_.find(fileName);
    if (it != cached_file_.end() && !it->second.empty()) {
      // If we have a local buffer, the remote object may not exist yet.
      // Return buffer size as an optimistic upper bound — callers about
      // to read will flush first.
      return it->second.size();
    }
  }
  Aws::S3::Model::HeadObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  auto outcome = client_->HeadObject(req);
  if (!outcome.IsSuccess()) return 0;
  return static_cast<size_t>(outcome.GetResult().GetContentLength());
}

void S3Storage::seal(std::string fileName) {
  // Flush any pending batched data first so readers observe the final
  // object on the next GetObject. S3 objects are immutable once PUT, so
  // the "seal marker" is just the fact that we stop accepting appends.
  flushLocked(fileName);
  std::lock_guard<std::mutex> lk(mtx_);
  sealed_files_.insert(fileName);
}

bool S3Storage::isSealed(std::string fileName) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (sealed_files_.count(fileName)) return true;
  }
  // For files written by a different process, treat "object exists on S3
  // with no local append buffer" as sealed — SSTables are write-once.
  return exist(fileName);
}

void S3Storage::remove(std::string fileName) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    cached_file_.erase(fileName);
    sealed_files_.erase(fileName);
  }
  Aws::S3::Model::DeleteObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  client_->DeleteObject(req);
}

bool S3Storage::exist(std::string fileName) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (cached_file_.count(fileName)) return true;
  }
  Aws::S3::Model::HeadObjectRequest req;
  req.SetBucket(Aws::String(bucket_.c_str()));
  req.SetKey(Aws::String(keyFor(fileName).c_str()));
  auto outcome = client_->HeadObject(req);
  return outcome.IsSuccess();
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_S3
