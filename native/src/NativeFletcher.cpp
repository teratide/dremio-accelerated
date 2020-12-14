#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include "com_dremio_sabot_op_project_FletcherFilterProjectOperator.h"  // Generated
using namespace std;

JNIEXPORT jboolean JNICALL Java_com_dremio_sabot_op_project_FletcherFilterProjectOperator_doNativeFletcher(JNIEnv *env, jobject filterProjectOp, jint records, jlong f_validity, jlong f_value, jlong t_validity, jlong t_value) {
  std::cout << "FLETCHERING " << records << " records";
  return true;
}
