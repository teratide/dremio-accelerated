#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include <arrow/api.h>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "com_dremio_sabot_op_filter_FilterTemplateAccelerated.h"  // Generated

using namespace std;

JNIEXPORT jint JNICALL Java_com_dremio_sabot_op_filter_FilterTemplateAccelerated_doNativeEval(JNIEnv *env, jobject, jint recordCount, jlongArray inAddresses, jlongArray inSizes, jlong outAddress, jlong outSize) {

    // Sanity check to see if inAddresses and inSizes have the same length
    ASSERT(env->GetArrayLength(inAddresses) == env->GetArrayLength(inSizes),
      "mismatch in array length of in buffer addresses and sizes");

    // Obtain pointers to the input arrays through the JNI environment
    jlong *in_buf_addrs = env->GetLongArrayElements(inAddresses, 0);
    jlong *in_buf_sizes = env->GetLongArrayElements(inSizes, 0);

    // TODO: This could be done without using a schema and by constructing buffers
    // but I did not manage to make an arrow::StringArray with the constructed buffers
    // so for now this stays
    std::shared_ptr<arrow::Field> field_a;
    std::shared_ptr<arrow::Schema> schema;
    field_a = arrow::field("Company", arrow::utf8(), true);
    schema = arrow::schema({field_a});
    std::shared_ptr<arrow::RecordBatch> inBatch;
    ASSERT_OK(make_record_batch_with_buf_addrs(schema, recordCount, in_buf_addrs, in_buf_sizes, recordCount, &inBatch));
    auto strings = std::static_pointer_cast<arrow::StringArray>(inBatch->column(0));

    // Construct the output buffer
    auto out_buf = arrow::Buffer::Wrap(reinterpret_cast<uint8_t *>(outAddress), (size_t) outSize);
//    auto out_buf = std::shared_ptr<arrow::Buffer>(new arrow::Buffer(reinterpret_cast<uint8_t *>(outAddress), outSize));
    auto out_values = reinterpret_cast<int32_t *>(out_buf->mutable_data());

    // Loop over all records and write to SV if company name matches filter
    int sv_index = 0; // Starting index of the output selection vector
    for (int i = 0; i < recordCount; i++) {
      if (strings->GetString(i) == "Dispatch Taxi Affiliation") {
        std::cout << "HIT, writing index " << i << " to selection vector position " << sv_index << " & " << outAddress << " ..." << std::endl;
         out_values[sv_index] = i;  // FIXME: This causes a segfault
//        int32_t val = (int32_t) i;
//        memcpy(out_values + sv_index * 4, &val, 4);
        std::cout << "\t index written" << std::endl;
        sv_index++; // Increment the SV index to keep track of number of matches
      }
    }

    return sv_index;
}
