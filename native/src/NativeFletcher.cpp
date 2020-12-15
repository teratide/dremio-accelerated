#include <jni.h>       // JNI header provided by JDK
#include <iostream>    // C++ standard IO header
#include "com_dremio_sabot_op_project_FletcherFilterProjectOperator.h"  // Generated
using namespace std;

JNIEXPORT jboolean JNICALL Java_com_dremio_sabot_op_project_FletcherFilterProjectOperator_doNativeFletcher(JNIEnv *env, jobject filterProjectOp, jint records, jlong f_validity, jlong f_value, jlong t_validity, jlong t_value) {
  std::cout << "OUTPUT BUFFER " << std::endl << "Validity: \t" << t_validity << std::endl << "Value: \t" << t_value << std::endl;
  std::cout << "INPUT BUFFER " << std::endl << "Validity: \t" << f_validity << std::endl << "Value: \t" << f_value << std::endl;

  long int *outValidity = reinterpret_cast<long int*>(t_validity);
  long int *outValue = reinterpret_cast<long int*>(t_value);

  *outValidity = 1;
  *outValue = 1000000;

  return true;
}
