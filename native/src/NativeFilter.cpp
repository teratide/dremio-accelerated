#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include <re2/re2.h>
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

    // The output SV is an array of int32's so we can access it using a simple pointer
    auto out_values = reinterpret_cast<int32_t *>(outAddress);


    RE2 re(".*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*");

    // Loop over all records and write to SV if company name matches filter
    int sv_index = 0; // Starting index of the output selection vector
    for (int i = 0; i < recordCount; i++) {
      if (RE2::FullMatch(strings->GetString(i), re)) {   // Hardcoded string to prove the implementation works
        out_values[sv_index] = i;
        sv_index++; // Increment the SV index to keep track of number of matches
      }
    }

    // Return the number of matches
    return sv_index;
}
