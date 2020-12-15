#include "ProtobufSchemaDeserializer.h"
#include "gandiva/projector.h"
#include "Types_.pb.h"

using gandiva::ConditionPtr;
using gandiva::DataTypePtr;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::SchemaPtr;
using gandiva::Status;

bool ParseProtobuf(uint8_t *buf, int bufLen, google::protobuf::Message *msg) {
    google::protobuf::io::CodedInputStream cis(buf, bufLen);
    cis.SetRecursionLimit(1000);
    return msg->ParseFromCodedStream(&cis);
}

DataTypePtr ProtoTypeToTime32(const types_::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types_::SEC:
            return arrow::time32(arrow::TimeUnit::SECOND);
        case types_::MILLISEC:
            return arrow::time32(arrow::TimeUnit::MILLI);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time32\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToTime64(const types_::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types_::MICROSEC:
            return arrow::time64(arrow::TimeUnit::MICRO);
        case types_::NANOSEC:
            return arrow::time64(arrow::TimeUnit::NANO);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time64\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToTimestamp(const types_::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types_::SEC:
            return arrow::timestamp(arrow::TimeUnit::SECOND);
        case types_::MILLISEC:
            return arrow::timestamp(arrow::TimeUnit::MILLI);
        case types_::MICROSEC:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case types_::NANOSEC:
            return arrow::timestamp(arrow::TimeUnit::NANO);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for timestamp\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToInterval(const types_::ExtGandivaType &ext_type) {
    switch (ext_type.intervaltype()) {
        case types_::YEAR_MONTH:
            return arrow::month_interval();
        case types_::DAY_TIME:
            return arrow::day_time_interval();
        default:
            std::cerr << "Unknown interval type: " << ext_type.intervaltype() << "\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToDataType(const types_::ExtGandivaType &ext_type) {
    switch (ext_type.type()) {
        case types_::NONE:
            return arrow::null();
        case types_::BOOL:
            return arrow::boolean();
        case types_::UINT8:
            return arrow::uint8();
        case types_::INT8:
            return arrow::int8();
        case types_::UINT16:
            return arrow::uint16();
        case types_::INT16:
            return arrow::int16();
        case types_::UINT32:
            return arrow::uint32();
        case types_::INT32:
            return arrow::int32();
        case types_::UINT64:
            return arrow::uint64();
        case types_::INT64:
            return arrow::int64();
        case types_::HALF_FLOAT:
            return arrow::float16();
        case types_::FLOAT:
            return arrow::float32();
        case types_::DOUBLE:
            return arrow::float64();
        case types_::UTF8:
            return arrow::utf8();
        case types_::BINARY:
            return arrow::binary();
        case types_::DATE32:
            return arrow::date32();
        case types_::DATE64:
            return arrow::date64();
        case types_::DECIMAL:
            // TODO: error handling
            return arrow::decimal(ext_type.precision(), ext_type.scale());
        case types_::TIME32:
            return ProtoTypeToTime32(ext_type);
        case types_::TIME64:
            return ProtoTypeToTime64(ext_type);
        case types_::TIMESTAMP:
            return ProtoTypeToTimestamp(ext_type);
        case types_::INTERVAL:
            return ProtoTypeToInterval(ext_type);
        case types_::FIXED_SIZE_BINARY:
        case types_::LIST:
        case types_::STRUCT:
        case types_::UNION:
        case types_::DICTIONARY:
        case types_::MAP:
            std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
            return nullptr;

        default:
            std::cerr << "Unknown data type: " << ext_type.type() << "\n";
            return nullptr;
    }
}

FieldPtr ProtoTypeToField(const types_::Field &f) {
    const std::string &name = f.name();
    DataTypePtr type = ProtoTypeToDataType(f.type());
    bool nullable = true;
    if (f.has_nullable()) {
        nullable = f.nullable();
    }

    return field(name, type, nullable);
}


SchemaPtr ProtoTypeToSchema(const types_::Schema &schema) {
    std::vector<FieldPtr> fields;

    for (int i = 0; i < schema.columns_size(); i++) {
        FieldPtr field = ProtoTypeToField(schema.columns(i));
        if (field == nullptr) {
            std::cerr << "Unable to extract arrow field from schema\n";
            return nullptr;
        }

        fields.push_back(field);
    }

    return arrow::schema(fields);
}

std::shared_ptr<arrow::Schema> ReadSchemaFromProtobufBytes(jbyte *schema_bytes, jsize schema_len) {
    types_::Schema proto_schema;
    if (!ParseProtobuf(reinterpret_cast<uint8_t *>(schema_bytes), schema_len, &proto_schema)) {
        std::cout << "Unable to parse schema protobuf\n";
        return nullptr;
    }

    return ProtoTypeToSchema(proto_schema);
}
