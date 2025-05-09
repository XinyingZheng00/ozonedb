#include "jni_LazyKVJNI.h"
#include "lazy_kv_client.h"
#include <iostream>
#include <map>
#include <string>
using namespace lazylog;

static LazyKV* kv = nullptr;  // multiple threads should access the same kv instance
static std::once_flag kv_init_once;
static std::once_flag kv_cleanup_once;

extern "C" {

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_init(JNIEnv* env, jobject, jstring be_path, jstring dl_client_path, jstring rdma_path) {
  std::call_once(kv_init_once, [&]() {
    kv = new LazyKV();
    char const* be_path_cstr = env->GetStringUTFChars(be_path, 0);
    char const* dl_client_path_cstr = env->GetStringUTFChars(dl_client_path, 0);
    char const* rdma_path_cstr = env->GetStringUTFChars(rdma_path, 0);

    std::string be(be_path_cstr);
    std::string dl_client(dl_client_path_cstr);
    std::string rdma(rdma_path_cstr);

    kv->Init(be, dl_client, rdma);

    env->ReleaseStringUTFChars(be_path, be_path_cstr);
    env->ReleaseStringUTFChars(dl_client_path, dl_client_path_cstr);
    env->ReleaseStringUTFChars(rdma_path, rdma_path_cstr);
  });
}

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_insert(JNIEnv* env, jobject, jstring jkey, jbyteArray jvalue) {
  char const* nativeKey = env->GetStringUTFChars(jkey, 0);
  std::string key(nativeKey);
  jsize length = env->GetArrayLength(jvalue);
  jbyte* byteArrayPtr = env->GetByteArrayElements(jvalue, nullptr);
  std::string str(reinterpret_cast<char*>(byteArrayPtr), length);
  kv->Insert(key, str);
  env->ReleaseStringUTFChars(jkey, nativeKey);
  env->ReleaseByteArrayElements(jvalue, byteArrayPtr, 0);
}

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_update(JNIEnv* env, jobject, jstring jkey, jbyteArray jvalue) {
  Java_jni_LazyKVJNI_insert(env, nullptr, jkey, jvalue);
}

JNIEXPORT jbyteArray JNICALL Java_jni_LazyKVJNI_read(JNIEnv* env, jobject, jstring jkey) {
  char const* nativeKey = env->GetStringUTFChars(jkey, 0);
  std::string key(nativeKey);
  std::string const* value;
  kv->Read(key, value);
  env->ReleaseStringUTFChars(jkey, nativeKey);
  jbyteArray byteArray = env->NewByteArray(value->length());
  env->SetByteArrayRegion(byteArray, 0, value->length(), reinterpret_cast<jbyte const*>(value->c_str()));
  return byteArray;
}

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_cleanup(JNIEnv*, jobject) {
  LOG(INFO) << "Cleaning up LazyKVJNI";
  std::call_once(kv_cleanup_once, [&]() {
    kv->Cleanup();
    delete kv;
    kv = nullptr;
  });
  LOG(INFO) << "Cleaning up LazyKVJNI finisged";
}
}
