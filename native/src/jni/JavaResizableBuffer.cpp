#include "JavaResizableBuffer.h"

static jclass gandiva_exception_;
static jclass vector_expander_class_;
static jclass vector_expander_ret_class_;
static jmethodID vector_expander_method_;
static jfieldID vector_expander_ret_address_;
static jfieldID vector_expander_ret_capacity_;

void JNI_OnLoad_JavaResizableBuffer(JNIEnv *env, void *reserved) {

    jclass localExceptionClass =
            env->FindClass("org/apache/arrow/gandiva/exceptions/GandivaException");
    gandiva_exception_ = (jclass) env->NewGlobalRef(localExceptionClass);
    env->ExceptionDescribe();
    env->DeleteLocalRef(localExceptionClass);

    jclass local_expander_class =
            env->FindClass("org/apache/arrow/gandiva/evaluator/VectorExpander");
    vector_expander_class_ = (jclass) env->NewGlobalRef(local_expander_class);
    env->DeleteLocalRef(local_expander_class);

    vector_expander_method_ = env->GetMethodID(
            vector_expander_class_, "expandOutputVectorAtIndex",
            "(IJ)Lorg/apache/arrow/gandiva/evaluator/VectorExpander$ExpandResult;");

    jclass local_expander_ret_class =
            env->FindClass("org/apache/arrow/gandiva/evaluator/VectorExpander$ExpandResult");
    vector_expander_ret_class_ = (jclass) env->NewGlobalRef(local_expander_ret_class);
    env->DeleteLocalRef(local_expander_ret_class);

    vector_expander_ret_address_ =
            env->GetFieldID(vector_expander_ret_class_, "address", "J");
    vector_expander_ret_capacity_ =
            env->GetFieldID(vector_expander_ret_class_, "capacity", "J");
}

void JNI_OnUnload_JavaResizableBuffer(JNIEnv *env, void *reserved) {
    env->DeleteGlobalRef(gandiva_exception_);
    env->DeleteGlobalRef(vector_expander_class_);
    env->DeleteGlobalRef(vector_expander_ret_class_);
}

Status JavaResizableBuffer::Resize(const int64_t new_size, bool shrink_to_fit) {

    if (shrink_to_fit == true) {
        return Status::NotImplemented("shrink not implemented");
    }

    if (ARROW_PREDICT_TRUE(new_size < capacity())) {
        // no need to expand.
        size_ = new_size;
        return Status::OK();
    }

    // callback into java to expand the buffer
    jobject ret =
            env_->CallObjectMethod(jexpander_, vector_expander_method_, vector_idx_, new_size);
    if (env_->ExceptionCheck()) {
        env_->ExceptionDescribe();
        env_->ExceptionClear();
        return Status::OutOfMemory("buffer expand failed in java");
    }

    jlong ret_address = env_->GetLongField(ret, vector_expander_ret_address_);
    jlong ret_capacity = env_->GetLongField(ret, vector_expander_ret_capacity_);
    //DCHECK_GE(ret_capacity, new_size);

    data_ = mutable_data_ = reinterpret_cast<uint8_t *>(ret_address);
    size_ = new_size;
    capacity_ = ret_capacity;
    return Status::OK();
}
