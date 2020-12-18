#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include <arrow/api.h>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "com_dremio_sabot_op_project_FletcherFilterProjectOperator.h"  // Generated

using namespace std;

double trivialCpuVersion(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    auto strings = std::static_pointer_cast<arrow::StringArray>(record_batch->column(1));
    auto numbers = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(0));

    const double* raw_numbers = numbers->raw_values();

    double sum = 0.0;
    for (int i = 0; i < record_batch->num_rows(); i++) {

        if (strings->GetString(i) == "Blue Ribbon Taxi Association Inc.") {
            sum += raw_numbers[i];
        }
    }

    std::cout << "CPU Sum: \t" << sum << std::endl;

    return sum;
}

JNIEXPORT jdouble JNICALL Java_com_dremio_sabot_op_project_FletcherFilterProjectOperator_doNativeFletcher(JNIEnv *env, jobject, jbyteArray schemaAsBytes, jint numberOfRecords, jlongArray inBufAddresses, jlongArray inBufSizes) {

  std::cout << "STARTING NATIVE FLETCHER CODE" << std::endl;

  // Extract input RecordBatch
  int inBufLen = env->GetArrayLength(inBufAddresses);
  ASSERT(inBufLen == env->GetArrayLength(inBufSizes), "mismatch in arraylen of buf_addrs and buf_sizes");

  // TODO: Read schema from schemaAsBytes argument
  std::shared_ptr<arrow::Field> field_a, field_b;
  std::shared_ptr<arrow::Schema> schema;
  field_a = arrow::field("Trip_Seconds", arrow::float64(), true);
  field_b = arrow::field("Company", arrow::utf8(), true);
  schema = arrow::schema({field_a, field_b});

  jlong *inAddresses = env->GetLongArrayElements(inBufAddresses, 0);
  jlong *inSizes = env->GetLongArrayElements(inBufSizes, 0);

  std::shared_ptr<arrow::RecordBatch> inBatch;
  ASSERT_OK(make_record_batch_with_buf_addrs(schema, numberOfRecords, inAddresses, inSizes, numberOfRecords, &inBatch));

  return trivialCpuVersion(inBatch);
}