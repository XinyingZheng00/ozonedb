#include "jni_OzoneDBJNI.h"
#include "db.h"
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>
#include <iostream>

using namespace log4cxx;
using namespace log4cxx::helpers;
LoggerPtr logger;

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
  BasicConfigurator::configure();  // Configure the logger
  logger = Logger::getLogger("event");
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
  ozonedb::Status status = db_instance->get(std::string(nativeKey), value);
  env->ReleaseStringUTFChars(key, nativeKey);

  if (status != ozonedb::Status::kSuccess) {
    // std::cerr << "Failed to get value" << std::endl;
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
