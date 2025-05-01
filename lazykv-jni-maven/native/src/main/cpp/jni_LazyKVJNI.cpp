#include "jni_LazyKVJNI.h"
#include "lazy_kv_client.h"
#include <iostream>
#include <map>
#include <string>

static LazyKV* kv = nullptr;  // todo: multiple thread problem

extern "C" {

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_init(JNIEnv* env, jobject, jstring jcli, jstring jwr, jstring jrd, jint jport, jint jmsg) {
  if (kv == nullptr) {
    kv = new LazyKV();
    char const* client_uri_cstr = env->GetStringUTFChars(jcli, 0);
    char const* wr_uri_cstr = env->GetStringUTFChars(jwr, 0);
    char const* rd_uri_cstr = env->GetStringUTFChars(jrd, 0);

    std::string client_uri(client_uri_cstr);
    std::string wr_uri(wr_uri_cstr);
    std::string rd_uri(rd_uri_cstr);

    uint8_t phy_port = static_cast<uint8_t>(jport);
    int msg_size = static_cast<int>(jmsg);

    kv->Init(client_uri, wr_uri, rd_uri, phy_port, msg_size);

    env->ReleaseStringUTFChars(jcli, client_uri_cstr);
    env->ReleaseStringUTFChars(jwr, wr_uri_cstr);
    env->ReleaseStringUTFChars(jrd, rd_uri_cstr);
  }
}

JNIEXPORT jint JNICALL Java_jni_LazyKVJNI_insert(JNIEnv* env, jobject, jstring jtable, jstring jkey, jobject jmap) {
  // std::string key = env->GetStringUTFChars(jkey, 0);
  // std::vector<DB::Field> fields;

  // jclass mapClass = env->GetObjectClass(jmap);
  // jmethodID entrySet = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
  // jobject entrySetObj = env->CallObjectMethod(jmap, entrySet);

  // jclass setClass = env->FindClass("java/util/Set");
  // jmethodID iterator = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
  // jobject iter = env->CallObjectMethod(entrySetObj, iterator);

  // jclass iterClass = env->FindClass("java/util/Iterator");
  // jmethodID hasNext = env->GetMethodID(iterClass, "hasNext", "()Z");
  // jmethodID next = env->GetMethodID(iterClass, "next", "()Ljava/lang/Object;");

  // jclass entryClass = env->FindClass("java/util/Map$Entry");
  // jmethodID getKey = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
  // jmethodID getValue = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

  // while (env->CallBooleanMethod(iter, hasNext)) {
  //   jobject entry = env->CallObjectMethod(iter, next);
  //   jstring k = (jstring)env->CallObjectMethod(entry, getKey);
  //   jstring v = (jstring)env->CallObjectMethod(entry, getValue);
  //   char const* key_cstr = env->GetStringUTFChars(k, 0);
  //   char const* val_cstr = env->GetStringUTFChars(v, 0);
  //   fields.push_back({key_cstr, val_cstr});
  //   env->ReleaseStringUTFChars(k, key_cstr);
  //   env->ReleaseStringUTFChars(v, val_cstr);
  // }

  // return (jint)kv->Insert("", key, fields);
}

JNIEXPORT jint JNICALL Java_jni_LazyKVJNI_read(JNIEnv* env, jobject, jstring jtable, jstring jkey, jobject jmap) {
  // std::string key = env->GetStringUTFChars(jkey, 0);
  // std::vector<DB::Field> result;

  // int status = (int)kv->Read("", key, nullptr, result);

  // jclass mapClass = env->GetObjectClass(jmap);
  // jmethodID put = env->GetMethodID(mapClass, "put",
  //                                  "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

  // for (auto& pair : result) {
  //   jstring k = env->NewStringUTF(pair.first.c_str());
  //   jstring v = env->NewStringUTF(pair.second.c_str());
  //   env->CallObjectMethod(jmap, put, k, v);
  // }

  // return status;
}

JNIEXPORT void JNICALL Java_jni_LazyKVJNI_cleanup(JNIEnv*, jobject) {
  if (kv) {
    kv->Cleanup();
    delete kv;
    kv = nullptr;
  }
}
}
