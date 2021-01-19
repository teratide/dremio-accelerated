#include <jni.h>    // JNI header provided by JDK
#include <iostream> // C++ standard IO header
#include <arrow/api.h>
#include <fletcher/api.h>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "com_dremio_sabot_op_filter_FilterTemplateAccelerated.h" // Generated

using namespace std;

JNIEXPORT jint JNICALL Java_com_dremio_sabot_op_filter_FilterTemplateAccelerated_doNativeEval(JNIEnv *env, jobject, jint recordCount, jlongArray inAddresses, jlongArray inSizes, jlong outAddress, jlong outSize)
{

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
  // auto strings = std::static_pointer_cast<arrow::StringArray>(inBatch->column(0));

  // The output SV is an array of int32's so we can access it using a simple pointer
  auto out_values = reinterpret_cast<int32_t *>(outAddress);
  // todo: wrap this buffer in a recordbatch
  // auto outBatch = ...

  fletcher::Status status;
  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;

  status = fletcher::Platform::Make(&platform);
  if (!status.ok())
  {
    exitWithError("Could not create Fletcher platform");
  }

  status = platform->Init();
  if (!status.ok())
  {
    exitWithError("Could not init Fletcher platform");
  }

  status = fletcher::Context::Make(&context, platform);
  if (!status.ok())
  {
    exitWithError("Could not create Fletcher context");
  }

  status = context->QueueRecordBatch(inBatch);
  if (!status.ok())
  {
    exitWithError("Could not queue the input RecordBatch to the context");
  }

  // status = context->QueueRecordBatch(outBatch);
  // if (!status.ok())
  // {
  //   exitWithError("Could not queue the input RecordBatch to the context");
  // }

  status = context->Enable();
  if (!status.ok())
  {
    exitWithError("Could not enable Fletcher context");
  }

  fletcher::Kernel kernel(context);

  status = kernel.Start();
  if (!status.ok())
  {
    exitWithError("Could not start Fletcher kernel");
  }

  status = kernel.PollUntilDone();
  if (!status.ok())
  {
    exitWithError("Something went wrong waiting for the kernel to finish");
  }

  uint32_t return_value_0;
  uint32_t return_value_1;

  status = kernel.GetReturn(&return_value_0, &return_value_1);
  if (!status.ok())
  {
    exitWithError("Could not obtain the return value");
  }

  // Return the number of matches
  return return_value_0;
}
