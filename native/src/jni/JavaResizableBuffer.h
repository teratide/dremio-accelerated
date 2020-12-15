//
// Created by Fabian Nonnenmacher on 14.05.20.
//

#ifndef SPARK_EXAMPLE_JAVARESIZABLEBUFFER_H
#define SPARK_EXAMPLE_JAVARESIZABLEBUFFER_H

#include <jni.h>
#include <arrow/api.h>

using arrow::Status;

void JNI_OnLoad_JavaResizableBuffer(JNIEnv *env, void *reserved);

void JNI_OnUnload_JavaResizableBuffer(JNIEnv *env, void *reserved);


///
/// \brief Resizable buffer which resizes by doing a callback into java.
///
/// Copied from: https://github.com/apache/arrow/blob/apache-arrow-0.17.1/cpp/src/gandiva/jni/jni_common.cc#L674
///
class JavaResizableBuffer : public arrow::ResizableBuffer {
public:
    JavaResizableBuffer(JNIEnv *env, jobject jexpander, int32_t vector_idx, uint8_t *buffer,
                        int32_t len)
            : ResizableBuffer(buffer, len),
              env_(env),
              jexpander_(jexpander),
              vector_idx_(vector_idx) {
        size_ = 0;
    }

    Status Resize(const int64_t new_size, bool shrink_to_fit) override;

    Status Reserve(const int64_t new_capacity) override {
        return Status::NotImplemented("reserve not implemented");
    }

private:
    JNIEnv *env_;
    jobject jexpander_;
    int32_t vector_idx_;
};

#endif //SPARK_EXAMPLE_JAVARESIZABLEBUFFER_H
