//
// Created by Fabian Nonnenmacher on 11.05.20.
//

#ifndef SPARK_EXAMPLE_PROTOBUFSCHEMADESERIALIZER_H
#define SPARK_EXAMPLE_PROTOBUFSCHEMADESERIALIZER_H

#include <arrow/api.h>
#include <jni.h>
#include "protobuf/Types_.pb.h"

std::shared_ptr<arrow::Schema> ReadSchemaFromProtobufBytes(jbyte *schema_bytes, jsize schema_len);

#endif //SPARK_EXAMPLE_PROTOBUFSCHEMADESERIALIZER_H
