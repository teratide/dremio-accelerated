#ifndef SPARK_EXAMPLE_JAVAMEMORYPOOL_H
#define SPARK_EXAMPLE_JAVAMEMORYPOOL_H

#include <arrow/api.h>
#include <jni.h>

void JNI_OnLoad_JavaMemoryPool(JNIEnv *env, void *reserved);

void JNI_OnUnload_JavaMemoryPool(JNIEnv *env, void *reserved);

///
/// \brief Memory pool, which allocates buffer by doing a callback into java.
///
/// All buffers allocated with this memory pool, can be accessed from the java code.
///
class JavaMemoryPool : public arrow::MemoryPool {
public:
    explicit JavaMemoryPool(JNIEnv *env, jobject jmemorypool) : arrow::MemoryPool(),
                                                                env_(env),
                                                                jmemorypool_(env_->NewGlobalRef(jmemorypool)) {};
    ~JavaMemoryPool() override {
            env_->DeleteGlobalRef(jmemorypool_);
    };

    arrow::Status Allocate(int64_t size, uint8_t **out) override;

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t **ptr) override;

    void Free(uint8_t *buffer, int64_t size) override;

    int64_t bytes_allocated() const override;

    int64_t max_memory() const override;

    std::string backend_name() const override;

private:
    JNIEnv *env_;
    jobject jmemorypool_;
};


#endif //SPARK_EXAMPLE_JAVAMEMORYPOOL_H
