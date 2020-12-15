//
// Called when JNI is loaded (or unloaded)
// > Triggers other initializations of other classes (e.g. setting static variables)
//

#include <jni.h>
#include "JavaResizableBuffer.h"
#include "JavaMemoryPool.h"

static jint JNI_VERSION = JNI_VERSION_1_6;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    // Call internal JNI loaders
    JNI_OnLoad_JavaResizableBuffer(env, reserved);
    JNI_OnLoad_JavaMemoryPool(env, reserved);

    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    // Call internal JNI unloaders
    JNI_OnUnload_JavaResizableBuffer(env, reserved);
    JNI_OnUnload_JavaMemoryPool(env, reserved);
}