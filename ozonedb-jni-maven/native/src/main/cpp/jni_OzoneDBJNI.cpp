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
 public:
  
  void onLogCompactionStart() {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);;
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Log Compaction Started at "
                                  << std::to_string(now));
  };
  void onLogCompactionCompletion(int time) {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);;
    LOG4CXX_INFO(logger,getpid() << ":"
                                  << "Log Compaction Completed"
                                  << " time: " << time
                                  << " at "
                                  << std::to_string(now));
  };
  void onSSTableCompactionStart() {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);;
    LOG4CXX_INFO(logger, getpid() << ":"
                                  <<"SST Compaction Started at "
                                  << std::to_string(now) );
  };
  void onSSTableCompactionCompletion(int time, int source_level) {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Level "
                                  << source_level 
                                  << " SST Compaction Completed"
                                  << " time: " << time
                                  << " at "
                                  << std::to_string(now));
  };

  void onViewUpdate() {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);;
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "View update Completed at "
                                  <<std::to_string(now) );
  };
  void onNewTail() {
    long long now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);;
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "New Tail Completed at "
                                  << std::to_string(now));
  };
};


JNIEXPORT void JNICALL Java_jni_OzoneDBJNI_openDB(JNIEnv* env, jobject obj, jstring configPath) {
  BasicConfigurator::configure();  // Configure the logger
  logger = Logger::getLogger("event");
  char const* nativeConfigPath = env->GetStringUTFChars(configPath, 0);
  ozonedb::Status status = ozonedb::DB::openDB(db_instance, std::string(nativeConfigPath));
  // db_instance->setEventListener(new EventListenerOzonedb());
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
