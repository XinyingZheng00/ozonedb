#include "checkpoint.h"
#include "protobuf/record.pb.h"
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>

namespace ozonedb {
namespace checkpoint {

namespace {

std::string parentOf(std::string const& key) {
  auto pos = key.rfind('/');
  return pos == std::string::npos ? std::string() : key.substr(0, pos);
}

// One object = appendNoFlush + flush. On S3 that is a single PutObject of
// the whole payload. On FileStorage it is a write to a stream opened in
// append mode, so a key must never be written twice with different bytes
// -- every key below carries covered_addr, except LATEST, whose reader
// takes the last line for exactly this reason.
Status putObject(Storage& store, std::string const& key,
                 unsigned char const* data, size_t len) {
  std::string parent = parentOf(key);
  if (!parent.empty()) store.createDirectory(parent);
  if (len > 0) {
    Status s = store.appendNoFlush(key, const_cast<unsigned char*>(data),
                                   static_cast<int>(len));
    if (s != Status::kSuccess) return s;
  } else {
    // An empty payload still needs an object, or the reader cannot tell
    // "empty file" from "missing object". One zero byte would change the
    // size, so write nothing and let the manifest's size=0 stand in.
    return Status::kSuccess;
  }
  Status s = store.flush(key);
  return s == Status::kSuccess ? Status::kSuccess : Status::kFailure;
}

Status getObject(Storage& store, std::string const& key, std::vector<unsigned char>& out) {
  if (!store.exist(key)) return Status::kNotFound;
  unsigned char* data = nullptr;
  size_t size = 0;
  Status s = store.read(key, data, size);
  if (s != Status::kSuccess) {
    delete[] data;
    return s;
  }
  out.assign(data, data + size);
  delete[] data;
  return Status::kSuccess;
}

}  // namespace

size_t State::liveBytes() const {
  size_t total = 0;
  for (auto const& kv : files) total += kv.second.size();
  return total;
}

std::string latestKey(std::string const& dir) { return dir + "/LATEST"; }

std::string manifestKey(std::string const& dir, long covered_addr) {
  return dir + "/" + std::to_string(covered_addr) + "/manifest";
}

std::string fileKey(std::string const& dir, long covered_addr, std::string const& name) {
  return dir + "/" + std::to_string(covered_addr) + "/files/" + name;
}

Status write(Storage& store, std::string const& dir, State const& state) {
  if (state.covered_addr < 0) return Status::kFailure;

  ::CheckpointManifest manifest;
  manifest.set_covered_addr(state.covered_addr);
  manifest.set_prev_covered_addr(state.prev_covered_addr);
  manifest.set_creator(state.creator);
  manifest.set_created_unix_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  // 1. File objects. Every file with a buffer, plus every sealed name that
  //    has none (a SEAL without an APPEND), so the sealed set round-trips.
  for (auto const& kv : state.files) {
    Status s = putObject(store, fileKey(dir, state.covered_addr, kv.first),
                         kv.second.data(), kv.second.size());
    if (s != Status::kSuccess) {
      std::cerr << "[checkpoint] write: file object failed for " << kv.first << "\n";
      return Status::kFailure;
    }
    auto* f = manifest.add_files();
    f->set_name(kv.first);
    f->set_size(static_cast<int64_t>(kv.second.size()));
    f->set_has_buffer(true);
    auto sit = state.sealed.find(kv.first);
    if (sit != state.sealed.end()) {
      f->set_sealed(true);
      f->set_sealed_at_addr(sit->second);
    }
  }
  for (auto const& kv : state.sealed) {
    if (state.files.count(kv.first)) continue;
    auto* f = manifest.add_files();
    f->set_name(kv.first);
    f->set_size(0);
    f->set_has_buffer(false);
    f->set_sealed(true);
    f->set_sealed_at_addr(kv.second);
  }
  for (auto const& name : state.removed) manifest.add_removed(name);

  // 2. Manifest.
  std::string serialized;
  if (!manifest.SerializeToString(&serialized)) return Status::kFailure;
  Status s = putObject(store, manifestKey(dir, state.covered_addr),
                       reinterpret_cast<unsigned char const*>(serialized.data()),
                       serialized.size());
  if (s != Status::kSuccess) {
    std::cerr << "[checkpoint] write: manifest failed for " << state.covered_addr << "\n";
    return Status::kFailure;
  }

  // 3. LATEST, last. Only now does the checkpoint exist for a joiner.
  std::string latest = std::to_string(state.covered_addr) + "\n";
  s = putObject(store, latestKey(dir),
                reinterpret_cast<unsigned char const*>(latest.data()), latest.size());
  if (s != Status::kSuccess) {
    std::cerr << "[checkpoint] write: LATEST failed for " << state.covered_addr << "\n";
    return Status::kFailure;
  }
  return Status::kSuccess;
}

Status readLatestAddr(Storage& store, std::string const& dir, long& covered_addr, bool& found) {
  found = false;
  covered_addr = -1;
  std::vector<unsigned char> bytes;
  Status s = getObject(store, latestKey(dir), bytes);
  if (s == Status::kNotFound) return Status::kSuccess;
  if (s != Status::kSuccess) return s;
  // Last non-empty line wins: an append-only backend (FileStorage) keeps
  // every value ever written to this key.
  std::string text(bytes.begin(), bytes.end());
  size_t end = text.find_last_not_of("\r\n ");
  if (end == std::string::npos) return Status::kSuccess;  // empty file: no checkpoint
  size_t start = text.find_last_of("\r\n ", end);
  std::string last = text.substr(start == std::string::npos ? 0 : start + 1,
                                 end - (start == std::string::npos ? 0 : start + 1) + 1);
  try {
    covered_addr = std::stol(last);
  } catch (std::exception const&) {
    std::cerr << "[checkpoint] LATEST holds no address: '" << last << "'\n";
    return Status::kFailure;
  }
  found = covered_addr >= 0;
  return Status::kSuccess;
}

Status read(Storage& store, std::string const& dir, long covered_addr, State& state,
            bool with_files) {
  state = State{};
  std::vector<unsigned char> bytes;
  Status s = getObject(store, manifestKey(dir, covered_addr), bytes);
  if (s != Status::kSuccess) return s;
  ::CheckpointManifest manifest;
  if (!manifest.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    std::cerr << "[checkpoint] manifest for " << covered_addr << " does not parse\n";
    return Status::kFailure;
  }
  state.covered_addr = manifest.covered_addr();
  state.prev_covered_addr = manifest.prev_covered_addr();
  state.creator = manifest.creator();
  for (auto const& f : manifest.files()) {
    bool has_buffer = f.has_has_buffer() ? f.has_buffer() : true;
    if (has_buffer && with_files) {
      std::vector<unsigned char>& buf = state.files[f.name()];
      if (f.size() > 0) {
        s = getObject(store, fileKey(dir, covered_addr, f.name()), buf);
        if (s != Status::kSuccess) {
          std::cerr << "[checkpoint] file object missing: " << f.name()
                    << " (checkpoint " << covered_addr << ")\n";
          return Status::kFailure;
        }
        if (static_cast<int64_t>(buf.size()) != f.size()) {
          std::cerr << "[checkpoint] file object size mismatch: " << f.name()
                    << " manifest=" << f.size() << " object=" << buf.size() << "\n";
          return Status::kFailure;
        }
      }
    }
    if (f.has_sealed() && f.sealed()) {
      state.sealed[f.name()] = f.has_sealed_at_addr() ? f.sealed_at_addr() : -1;
    }
  }
  for (auto const& name : manifest.removed()) state.removed.insert(name);
  return Status::kSuccess;
}

Status readLatest(Storage& store, std::string const& dir, State& state, bool& found) {
  long addr = -1;
  Status s = readLatestAddr(store, dir, addr, found);
  if (s != Status::kSuccess || !found) return s;
  return read(store, dir, addr, state);
}

Status remove(Storage& store, std::string const& dir, long covered_addr) {
  std::vector<unsigned char> bytes;
  Status s = getObject(store, manifestKey(dir, covered_addr), bytes);
  if (s == Status::kNotFound) return Status::kSuccess;  // already gone
  if (s != Status::kSuccess) return s;
  ::CheckpointManifest manifest;
  if (manifest.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    for (auto const& f : manifest.files()) {
      if (f.size() > 0) store.remove(fileKey(dir, covered_addr, f.name()));
    }
  }
  store.remove(manifestKey(dir, covered_addr));
  return Status::kSuccess;
}

}  // namespace checkpoint
}  // namespace ozonedb
