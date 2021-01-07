#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include <arrow/api.h>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "com_dremio_sabot_op_filter_FilterTemplateAccelerated.h"  // Generated

using namespace std;

int trivialCpuVersion(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    std::cout << "Hit trivialCpuVersion() with record_batch->num_rows() = " << record_batch->num_rows() << std::endl;

    auto strings = std::static_pointer_cast<arrow::StringArray>(record_batch->column(1));

    auto selectionVector = std::static_pointer_cast<arrow::Int16Array>(record_batch->column(2));

    std::cout << "Starting filter match operation, showing value in first record" << std::endl;
    int hit = 0;

    for (int i = 0; i < record_batch->num_rows(); i++) {

        if (i < 1) {
          std::cout << "Company:\t" << strings->GetString(i) << std::endl;
        }

        if (strings->GetString(i) == "Dispatch Taxi Affiliation") {
            // add index to selectionVector builder
            char val = (char) i;
            memcpy(&selectionVector + 16 * hit, &val, 2);
            hit++;
        }
    }

    std::cout << "Hit string " << hit << " times\t" << std::endl;

    return hit;
}

JNIEXPORT jint JNICALL Java_com_dremio_sabot_op_filter_FilterTemplateAccelerated_doNativeEval(JNIEnv *env, jobject, jbyteArray schemaAsBytes, jint numberOfRecords, jlongArray inBufAddresses, jlongArray inBufSizes) {
    std::cout << std::endl << "STARTING NATIVE FILTER CODE" << std::endl;

    // Extract input RecordBatch
    int inBufLen = env->GetArrayLength(inBufAddresses);
    ASSERT(inBufLen == env->GetArrayLength(inBufSizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    // TODO: Read schema from schemaAsBytes argument
    std::shared_ptr<arrow::Field> field_a, field_b, field_c;
    std::shared_ptr<arrow::Schema> schema;
    field_a = arrow::field("Trip_Seconds", arrow::float64(), true);
    field_b = arrow::field("Company", arrow::utf8(), true);
    field_c = arrow::field("SelectionVector", arrow::uint16(), true);
    schema = arrow::schema({field_a, field_b, field_c});

    jlong *inAddresses = env->GetLongArrayElements(inBufAddresses, 0);
    jlong *inSizes = env->GetLongArrayElements(inBufSizes, 0);

    std::shared_ptr<arrow::RecordBatch> inBatch;
    ASSERT_OK(make_record_batch_with_buf_addrs(schema, numberOfRecords, inAddresses, inSizes, numberOfRecords, &inBatch));

    int records = trivialCpuVersion(inBatch);
    return records;
}
