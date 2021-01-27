//
// Collection of converters from/to java objects
//

#ifndef SPARK_EXAMPLE_CONVERTERS_H
#define SPARK_EXAMPLE_CONVERTERS_H

#include <jni.h>
#include <arrow/api.h>
#include <iostream>

using arrow::Status;

std::string get_java_string(JNIEnv *env, jstring java_string);

std::vector<std::string> get_java_string_array(JNIEnv *env, jobjectArray jstringArr);

std::vector<int> get_java_int_array(JNIEnv *env, jintArray java_field_indices);

// Copied from https://github.com/apache/arrow/blob/apache-arrow-0.17.0/cpp/src/gandiva/jni/jni_common.cc#L517
Status make_record_batch_with_buf_addrs(std::shared_ptr<arrow::Schema> schema, int,
                                        jlong *, jlong *,
                                        int,
                                        std::shared_ptr<arrow::RecordBatch> *);

Status copy_record_batch_ito_buffers(JNIEnv* env, jobject jexpander,
                                     std::shared_ptr<arrow::RecordBatch> recordBatch,
                                     jlong *out_buf_addrs, jlong *out_buf_sizes,
                                     int out_bufs_len);

#endif //SPARK_EXAMPLE_CONVERTERS_H
