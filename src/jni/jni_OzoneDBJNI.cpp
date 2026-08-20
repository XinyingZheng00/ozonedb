#include "jni_OzoneDBJNI.h"
#include "db.h"
// These were previously pulled in transitively by the log4cxx headers.
// getCurrentTimestamp() below needs all four, so include them directly.
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

thread_local ozonedb::DB* db_instance = nullptr;
class EventListenerOzonedb : public ozonedb::EventListener {
  private:
  std::string getCurrentTimestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto now_time_t = system_clock::to_time_t(now);
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%d %H:%M:%S")
        << ':' << std::setw(3) << std::setfill('0') << ms.count();
    return oss.str();
  }

 public:
//keep the interface in case we need to differentiate between log and sstable compaction
  void onLogCompactionStart() {
    std::cout << getCurrentTimestamp() << " - Compaction started: " << std::endl;
  }

  void onLogCompactionCompletion(int time_ms) {
    std::cout << getCurrentTimestamp() << " - Compaction completed: " << std::endl;
  }

  void onSSTableCompactionStart() {
    std::cout << getCurrentTimestamp() << " - Compaction started: " << std::endl;
  }

  void onSSTableCompactionCompletion(int time_ms, int source_level) {
    std::cout << getCurrentTimestamp() << " - Compaction completed: " << std::endl;
  }

  // void onViewUpdate() {
  //   std::cout << getCurrentTimestamp() << " - Viewupdate completed" << std::endl;
  // };

  void onNewTail() {
    std::cout << getCurrentTimestamp() << " - NewTail completed: " << std::endl;
  };

};


JNIEXPORT void JNICALL Java_jni_OzoneDBJNI_openDB(JNIEnv* env, jobject obj, jstring configPath) {
  char const* nativeConfigPath = env->GetStringUTFChars(configPath, 0);
  ozonedb::Status status = ozonedb::DB::openDB(db_instance, std::string(nativeConfigPath));
  db_instance->setEventListener(new EventListenerOzonedb());
  env->ReleaseStringUTFChars(configPath, nativeConfigPath);

  if (status != ozonedb::Status::kSuccess) {
    std::cerr << "Failed to open database" << std::endl;
  }
}

JNIEXPORT void JNICALL Java_jni_OzoneDBJNI_closeDB(JNIEnv* env, jobject obj) {
  ozonedb::Status status = ozonedb::DB::closeDB(db_instance);
  if (status != ozonedb::Status::kSuccess) {
    std::cerr << "Failed to close database" << std::endl;
  }
}

JNIEXPORT void JNICALL Java_jni_OzoneDBJNI_put(JNIEnv* env, jobject obj, jstring key, jbyteArray value) {
  char const* nativeKey = env->GetStringUTFChars(key, 0);
  // char const* nativeValue = env->GetStringUTFChars(value, 0);
  jsize length = env->GetArrayLength(value);
  jbyte *byteArrayPtr = env->GetByteArrayElements(value, nullptr);
  std::string str(reinterpret_cast<char*>(byteArrayPtr), length);
  ozonedb::Status status = db_instance->put(std::string(nativeKey), str);
  env->ReleaseStringUTFChars(key, nativeKey);
   env->ReleaseByteArrayElements(value, byteArrayPtr, 0);

  if (status != ozonedb::Status::kSuccess) {
    std::cerr << "Failed to put key-value pair" << std::endl;
  }
}

JNIEXPORT jbyteArray JNICALL Java_jni_OzoneDBJNI_get(JNIEnv* env, jobject obj, jstring key) {
  char const* nativeKey = env->GetStringUTFChars(key, 0);
  std::string const* value;
  // `guard` keeps the underlying Record alive for the duration of the
  // JNI byte-array copy below — without it, a concurrent compaction or
  // LRU eviction could free the bytes mid-copy.
  std::shared_ptr<Record> guard;
  ozonedb::Status status = db_instance->get(std::string(nativeKey), value, guard);
  env->ReleaseStringUTFChars(key, nativeKey);

  if (status != ozonedb::Status::kSuccess) {
    std::cerr << "Failed to get value" << std::endl;
    return nullptr;
  }
  jbyteArray byteArray = env->NewByteArray(value->length());
  env->SetByteArrayRegion(byteArray, 0, value->length(), reinterpret_cast<const jbyte*>(value->c_str()));
  return byteArray;
}

JNIEXPORT void JNICALL Java_jni_OzoneDBJNI_remove(JNIEnv* env, jobject obj, jstring key) {
  char const* nativeKey = env->GetStringUTFChars(key, 0);
  ozonedb::Status status = db_instance->remove(std::string(nativeKey));
  env->ReleaseStringUTFChars(key, nativeKey);

  if (status != ozonedb::Status::kSuccess) {
    std::cerr << "Failed to remove key" << std::endl;
  }
}

// Returns null when the key is absent; otherwise an 8-byte big-endian
// version (the key's global log address; -1 encodes "unwritten")
// followed by the value bytes. One array keeps the (value, version)
// pair atomic across the JNI boundary.
JNIEXPORT jbyteArray JNICALL Java_jni_OzoneDBJNI_getVersioned(JNIEnv* env, jobject obj, jstring key) {
  char const* nativeKey = env->GetStringUTFChars(key, 0);
  std::string value;
  int64_t version = -1;
  ozonedb::Status status = db_instance->getVersioned(std::string(nativeKey), value, version);
  env->ReleaseStringUTFChars(key, nativeKey);

  if (status != ozonedb::Status::kSuccess) {
    return nullptr;
  }
  jbyteArray byteArray = env->NewByteArray(8 + value.length());
  jbyte header[8];
  for (int i = 0; i < 8; i++) {
    header[i] = static_cast<jbyte>((static_cast<uint64_t>(version) >> (8 * (7 - i))) & 0xff);
  }
  env->SetByteArrayRegion(byteArray, 0, 8, header);
  env->SetByteArrayRegion(byteArray, 8, value.length(), reinterpret_cast<const jbyte*>(value.data()));
  return byteArray;
}

// Returns the key's new version (>= 0) when the conditional put is
// accepted, -2 when it loses the version check (caller should re-read
// and retry), -1 on any other failure.
JNIEXPORT jlong JNICALL Java_jni_OzoneDBJNI_casPut(JNIEnv* env, jobject obj, jstring key,
                                                   jbyteArray value, jlong expectedVersion) {
  char const* nativeKey = env->GetStringUTFChars(key, 0);
  jsize length = env->GetArrayLength(value);
  jbyte* byteArrayPtr = env->GetByteArrayElements(value, nullptr);
  std::string str(reinterpret_cast<char*>(byteArrayPtr), length);
  int64_t newVersion = -1;
  ozonedb::Status status = db_instance->compareAndPut(
      std::string(nativeKey), static_cast<int64_t>(expectedVersion), str, newVersion);
  env->ReleaseStringUTFChars(key, nativeKey);
  env->ReleaseByteArrayElements(value, byteArrayPtr, 0);

  if (status == ozonedb::Status::kSuccess) {
    return static_cast<jlong>(newVersion);
  }
  if (status == ozonedb::Status::kCasConflict) {
    return -2;
  }
  std::cerr << "casPut failed" << std::endl;
  return -1;
}
