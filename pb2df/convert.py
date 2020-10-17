from google.protobuf.descriptor import FieldDescriptor, Descriptor
from google.protobuf.reflection import GeneratedProtocolMessageType
from google.protobuf.json_format import _IsMapEntry
from pyspark.sql import types
from pyspark.sql.types import StructType, StructField, DataType

# scalar value types mapping between ProtoBuf and Spark SQL
_SPARK_SQL_TYPE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: types.DoubleType(),
    FieldDescriptor.TYPE_FLOAT: types.FloatType(),
    FieldDescriptor.TYPE_INT64: types.LongType(),
    FieldDescriptor.TYPE_UINT64: types.LongType(),
    FieldDescriptor.TYPE_INT32: types.IntegerType(),
    FieldDescriptor.TYPE_FIXED64: types.LongType(),
    FieldDescriptor.TYPE_FIXED32: types.IntegerType(),
    FieldDescriptor.TYPE_BOOL: types.BooleanType(),
    FieldDescriptor.TYPE_STRING: types.StringType(),
    FieldDescriptor.TYPE_BYTES: types.BinaryType(),
    FieldDescriptor.TYPE_UINT32: types.IntegerType(),
    FieldDescriptor.TYPE_ENUM: types.IntegerType(),
    FieldDescriptor.TYPE_SFIXED32: types.IntegerType(),
    FieldDescriptor.TYPE_SFIXED64: types.LongType(),
    FieldDescriptor.TYPE_SINT32: types.IntegerType(),
    FieldDescriptor.TYPE_SINT64: types.LongType(),
}


def proto3_message_to_spark_dataframe():
    pass


def proto3_message_type_to_spark_schema(
    message_type: GeneratedProtocolMessageType,
) -> StructType:
    """Convert ProtoBuf message type to Spark schema object.

    Args:
        message_desc (GeneratedProtocolMessageType): A ProtoBuf message type.

    Returns:
        StructType: A spark schema object.
    """
    return _proto3_message_descriptor_to_spark_schema(message_type.DESCRIPTOR)


def _proto3_message_descriptor_to_spark_schema(
    message_desc: Descriptor,
) -> StructType:
    """Convert ProtoBuf message descriptor to Spark schema object.

    Args:
        message_desc (Descriptor): A ProtoBuf message descriptor.

    Returns:
        StructType: A Spark schema object.
    """
    struct_fields = []
    for field_name, field_desc in message_desc.fields_by_name.iteritems():
        struct_fields.append(
            StructField(field_name, _proto3_field_to_spark_data_type(field_desc))
        )
    return StructType(struct_fields)


def _proto3_field_to_spark_data_type(field_desc: FieldDescriptor) -> DataType:
    """Convert ProtoBuf field descriptor to Spark `DataType` or `StructField` object.

    Args:
        field_desc (FieldDescriptor): A ProtoBuf field descriptor.
    Returns:
        DataType: A Spark `DataType` or `StructField` object.
    """
    # map type field
    if _IsMapEntry(field_desc):
        key_field_desc = field_desc.message_type.fields_by_name["key"]
        value_field_desc = field_desc.message_type.fields_by_name["value"]
        key_struct_type = _proto3_field_to_spark_data_type(key_field_desc)
        value_struct_type = _proto3_field_to_spark_data_type(value_field_desc)
        return types.MapType(key_struct_type, value_struct_type)

    if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
        # nested message
        field_data_type = _proto3_message_descriptor_to_spark_schema(
            field_desc.message_type
        )
    else:
        # scalar value types
        field_data_type = _SPARK_SQL_TYPE_MAP[field_desc.type]

    # list type field
    if field_desc.label == FieldDescriptor.LABEL_REPEATED:
        return types.ArrayType(field_data_type)

    return field_data_type
