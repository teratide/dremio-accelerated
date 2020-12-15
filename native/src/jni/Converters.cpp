#include "Converters.h"
#include <iostream>
#include <arrow/api.h>
#include "JavaResizableBuffer.h"
#include "Assertions.h"

using namespace std;


string get_java_string(JNIEnv *env, jstring java_string) {
    const jsize strLen = env->GetStringUTFLength(java_string);
    const char *charBuffer = env->GetStringUTFChars(java_string, nullptr);
    string str(charBuffer, strLen);
    env->ReleaseStringUTFChars(java_string, charBuffer);
    env->DeleteLocalRef(java_string);
    return str;
}

vector<string> get_java_string_array(JNIEnv *env, jobjectArray jstringArr) {
    vector<string> stringVec;

    // Get length
    int len = env->GetArrayLength(jstringArr);

    for (int i = 0; i < len; i++) {
        // Cast array element to string
        jstring jstr = (jstring) (env->GetObjectArrayElement(jstringArr, i));

        // Convert Java string to std::string
        const jsize strLen = env->GetStringUTFLength(jstr);
        const char *charBuffer = env->GetStringUTFChars(jstr, nullptr);
        string str(charBuffer, strLen);

        // Push back string to vector
        stringVec.push_back(str);

        // Release memory
        env->ReleaseStringUTFChars(jstr, charBuffer);
        env->DeleteLocalRef(jstr);
    }
    return stringVec;
}

std::vector<int> get_java_int_array(JNIEnv *env, jintArray java_int_array) {
    int len = env->GetArrayLength(java_int_array);
    int *java_ints = (int *) env->GetIntArrayElements(java_int_array, nullptr);

    vector<int> int_vec;
    int_vec.reserve(len);
    for (int i = 0; i < len; i++) {
        int_vec.push_back(java_ints[i]);
    }
    return int_vec;
}


Status make_record_batch_with_buf_addrs(std::shared_ptr<arrow::Schema> schema, int num_rows,
                                        jlong *in_buf_addrs, jlong *in_buf_sizes,
                                        int in_bufs_len,
                                        std::shared_ptr<arrow::RecordBatch> *batch) {
    std::vector<std::shared_ptr<arrow::ArrayData>> columns;
    auto num_fields = schema->num_fields();
    int buf_idx = 0;
    int sz_idx = 0;

    for (int i = 0; i < num_fields; i++) {
        auto field = schema->field(i);
        std::vector<std::shared_ptr<arrow::Buffer>> buffers;

        bool nullable = field->nullable();

        if (buf_idx >= in_bufs_len) {
            return Status::Invalid("insufficient number of in_buf_addrs");
        }

        if (!nullable) {
            jlong validity_addr = in_buf_addrs[buf_idx++];
            jlong validity_size = in_buf_sizes[sz_idx++];
            auto validity = std::shared_ptr<arrow::Buffer>(
                    new arrow::Buffer(reinterpret_cast<uint8_t *>(validity_addr), validity_size));
            buffers.push_back(validity);
        } else { //if Field is not nullable ignore validity buffer
            buffers.push_back(nullptr);
        }

        if (buf_idx >= in_bufs_len) {
            return Status::Invalid("insufficient number of in_buf_addrs");
        }
        jlong value_addr = in_buf_addrs[buf_idx++];
        jlong value_size = in_buf_sizes[sz_idx++];
        auto data = std::shared_ptr<arrow::Buffer>(
                new arrow::Buffer(reinterpret_cast<uint8_t *>(value_addr), value_size));
        buffers.push_back(data);

        if (arrow::is_binary_like(field->type()->id())) {
            if (buf_idx >= in_bufs_len) {
                return Status::Invalid("insufficient number of in_buf_addrs");
            }

            // add offsets buffer for variable-len fields.
            jlong offsets_addr = in_buf_addrs[buf_idx++];
            jlong offsets_size = in_buf_sizes[sz_idx++];
            auto offsets = std::shared_ptr<arrow::Buffer>(
                    new arrow::Buffer(reinterpret_cast<uint8_t *>(offsets_addr), offsets_size));
            buffers.push_back(offsets);
        }

        std::shared_ptr<arrow::ArrayData> array_data;
        if (nullable) {
            array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
        } else {
            array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers), 0);
        }

        columns.push_back(array_data);
    }
    *batch = arrow::RecordBatch::Make(schema, num_rows, columns);
    return Status::OK();
}


Status copy_record_batch_ito_buffers(JNIEnv *env, jobject jexpander,
                                     shared_ptr<arrow::RecordBatch> recordBatch,
                                     jlong *out_buf_addrs, jlong *out_buf_sizes,
                                     int out_bufs_len) {

    const shared_ptr<arrow::Schema> &schema = recordBatch->schema();
    auto num_fields = schema->num_fields();

    int buf_idx = 0;
    int variable_width_vector_idx = 0;

    for (int i = 0; i < num_fields; i++) {
        auto field = schema->field(i);
        auto column = recordBatch->column(i);

        ASSERT(buf_idx <= out_bufs_len, "insufficient number of in_buf_addrs");

        // Copy validity buffer
        auto validity_buffer = column->data()->buffers[0];

        if (validity_buffer == nullptr) { //validity buffer not defined -> all values are no null
            memset((void *) out_buf_addrs[buf_idx], 0xFF, out_buf_sizes[buf_idx]);

        } else {
            ASSERT(validity_buffer->size() <= out_buf_sizes[buf_idx],
                   "Validity buffer of field '" + field->name() +
                   "' cannot be copied, because it has the wrong size.");

            memcpy((void *) out_buf_addrs[buf_idx], (void *) validity_buffer->address(), validity_buffer->size());
        }
        buf_idx++;

        ASSERT(buf_idx <= out_bufs_len, "insufficient number of in_buf_addrs");

        // copy value buffer
        auto value_buffer = column->data()->buffers[1];
        // std::cout << "ValueBuffer: "  << value_buffer->size() << std::endl;
        // std::cout << "Out Buffer: "  << out_buf_sizes[buf_idx] << std::endl;

        ASSERT(value_buffer->size() <= out_buf_sizes[buf_idx],
               "Value buffer of field '" + field->name() +
               "' cannot be copied, because it has the wrong size.");

        memcpy((void *) out_buf_addrs[buf_idx], (void *) value_buffer->address(), value_buffer->size());
        buf_idx++;
//        std::cout << "Value buffer of field '" << field->name() << "' copied." << std::endl;;

        if (arrow::is_binary_like(field->type()->id())) { // field with variable width
            ASSERT(buf_idx <= out_bufs_len, "insufficient number of in_buf_addrs");

            // copy offset buffer
            auto offset_buffer = column->data()->buffers[2];
            auto resizable_buffer = std::make_shared<JavaResizableBuffer>(env, jexpander, variable_width_vector_idx,
                                                                          reinterpret_cast<uint8_t *>(out_buf_addrs[buf_idx]),
                                                                          out_buf_sizes[buf_idx]);
            ASSERT_OK(resizable_buffer->Resize(offset_buffer->size(), false));

            memcpy((void *) resizable_buffer->address(), (void *) offset_buffer->address(),
                   offset_buffer->size());

            variable_width_vector_idx++;
            buf_idx++;
        }
    }
    return Status::OK();
}
