#include <mutex>
#include <memory>
#include <jni.h>    // JNI header provided by JDK
#include <iostream> // C++ standard IO header
#include <arrow/api.h>
#include "tidre.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "com_dremio_sabot_op_filter_FilterTemplateAccelerated.h"  // Generated

using Tidre = tidre::Tidre<8>;
using namespace std;

JNIEXPORT jint JNICALL Java_com_dremio_sabot_op_filter_FilterTemplateAccelerated_doTidreEval(JNIEnv *env, jobject, jint recordCount, jlongArray inAddresses, jlongArray inSizes, jlong outAddress, jlong outSize) {

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
    auto int32_offset_buffer_ptr = reinterpret_cast<int32_t *>(in_buf_addrs[2]);
    auto char_data_buffer_ptr = reinterpret_cast<int32_t *>(in_buf_addrs[1]);

    // Start eval using tidre
    static std::mutex mutex;

    const std::lock_guard<std::mutex> lock(mutex);
    static std::shared_ptr<Tidre> t = nullptr;
    if (!t) {
      auto status = Tidre::Make(&t, "aws", 1, 8, 2, 3);
      if (!status.ok()) {
        t = nullptr;
        std::cout << "Status not OK after initializing Tidre" << std::endl;
      }
    }
    size_t number_of_matches = 0;
    auto status = t->RunRaw(
      int32_offset_buffer_ptr,
      char_data_buffer_ptr,
      recordCount,
      out_values,
      recordCount*4 /* or output buffer size if smaller */,
      &number_of_matches,
      nullptr,
      0
    );
    if (!status.ok()) {
      std::cout << "Status not OK after running Tidre" << std::endl;
    }

    // Return the number of matches
    return (int) number_of_matches;
}
