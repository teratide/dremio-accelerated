#include "JavaMemoryPool.h"
#include "Assertions.h"

static jclass jmemorypool_class;
static jmethodID jmemorypool_class_allocate_method;
static jmethodID jmemorypool_class_reallocate_method;
static jmethodID jmemorypool_class_free_method;
static jmethodID jmemorypool_class_bytes_allocated_method;
static jmethodID jmemorypool_class_max_memory_method;

void JNI_OnLoad_JavaMemoryPool(JNIEnv *env, void *reserved) {

    jclass local_jmemorypool_class =
            env->FindClass("nl/tudelft/ewi/abs/nonnenmacher/JavaMemoryPoolServer");
    jmemorypool_class = (jclass) env->NewGlobalRef(local_jmemorypool_class);
    env->DeleteLocalRef(local_jmemorypool_class);

    jmemorypool_class_allocate_method = env->GetMethodID(
            jmemorypool_class, "allocate",
            "(J)J");

    jmemorypool_class_reallocate_method = env->GetMethodID(
            jmemorypool_class, "reallocate",
            "(JJ)J");

    jmemorypool_class_free_method = env->GetMethodID(
            jmemorypool_class, "free",
            "(J)V");

    jmemorypool_class_bytes_allocated_method = env->GetMethodID(
            jmemorypool_class, "bytesAllocated",
            "()J");

    jmemorypool_class_max_memory_method = env->GetMethodID(
            jmemorypool_class, "maxMemory",
            "()J");
}

void JNI_OnUnload_JavaMemoryPool(JNIEnv *env, void *reserved) {
    env->DeleteGlobalRef(jmemorypool_class);
}

std::string JavaMemoryPool::backend_name() const {
    return "javamemorypool";
}

arrow::Status JavaMemoryPool::Allocate(int64_t size, uint8_t **out) {
    jlong ret = env_->CallLongMethod(jmemorypool_, jmemorypool_class_allocate_method, size);
    *out = reinterpret_cast<uint8_t*>(ret);
    return arrow::Status::OK();
}

arrow::Status JavaMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t **ptr) {
    jlong address = reinterpret_cast<jlong>(*ptr);
    jlong ret = env_->CallLongMethod(jmemorypool_, jmemorypool_class_reallocate_method, address, new_size);
    *ptr = reinterpret_cast<uint8_t*>(ret);
    return arrow::Status::OK();
}

void JavaMemoryPool::Free(uint8_t *buffer, int64_t size) {
    jlong address = reinterpret_cast<jlong>(buffer);
    env_->CallVoidMethod(jmemorypool_, jmemorypool_class_free_method, address);
}

int64_t JavaMemoryPool::bytes_allocated() const {
    return env_->CallLongMethod(jmemorypool_, jmemorypool_class_bytes_allocated_method);
}

int64_t JavaMemoryPool::max_memory() const {
    return env_->CallLongMethod(jmemorypool_, jmemorypool_class_max_memory_method);
}