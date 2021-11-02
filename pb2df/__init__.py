__author__ = """Xianbing Zhang"""
__email__ = "zhangxianbing09@163.com"
__version__ = "0.1.0"


from google.protobuf.descriptor import Descriptor, FieldDescriptor
from pyspark.sql import types as T

# scalar value types mapping between ProtoBuf and Spark SQL
_SPARK_SQL_TYPE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: T.DoubleType(),
    FieldDescriptor.TYPE_FLOAT: T.FloatType(),
    FieldDescriptor.TYPE_INT64: T.LongType(),
    FieldDescriptor.TYPE_UINT64: T.LongType(),
    FieldDescriptor.TYPE_INT32: T.IntegerType(),
    FieldDescriptor.TYPE_FIXED64: T.LongType(),
    FieldDescriptor.TYPE_FIXED32: T.IntegerType(),
    FieldDescriptor.TYPE_BOOL: T.BooleanType(),
    FieldDescriptor.TYPE_STRING: T.StringType(),
    FieldDescriptor.TYPE_BYTES: T.BinaryType(),
    FieldDescriptor.TYPE_UINT32: T.IntegerType(),
    FieldDescriptor.TYPE_ENUM: T.IntegerType(),
    FieldDescriptor.TYPE_SFIXED32: T.IntegerType(),
    FieldDescriptor.TYPE_SFIXED64: T.LongType(),
    FieldDescriptor.TYPE_SINT32: T.IntegerType(),
    FieldDescriptor.TYPE_SINT64: T.LongType(),
}


def schema_for(descriptor: Descriptor) -> T.StructType:
    if descriptor is None:
        return None

    struct_type = T.StructType()

    for field_desc in descriptor.fields:
        struct_type.add(field_desc.name, __type_for(field_desc))

    return struct_type


def __type_for(field_desc: FieldDescriptor) -> T.DataType:
    if _is_map_entry(field_desc):
        key_field_desc = field_desc.message_type.fields_by_name["key"]
        value_field_desc = field_desc.message_type.fields_by_name["value"]
        return T.MapType(__type_for(key_field_desc), __type_for(value_field_desc))

    if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
        field_data_type = schema_for(field_desc.message_type)
    else:
        field_data_type = _SPARK_SQL_TYPE_MAP.get(field_desc.type, T.NullType())

    if field_desc.label == field_desc.LABEL_REPEATED:
        return T.ArrayType(field_data_type)

    return field_data_type


def _is_map_entry(field_desc: FieldDescriptor):
    return (
        field_desc.type == FieldDescriptor.TYPE_MESSAGE
        and field_desc.message_type.has_options
        and field_desc.message_type.GetOptions().map_entry
    )


def message_to_row(descriptor: Descriptor, message) -> T.Row:
    field_map = {}
    for field_tuple in message.ListFields():
        field_map[field_tuple[0].name] = field_tuple[1]

    values = []

    for field_desc in descriptor.fields:
        if field_desc.name in field_map:
            if _is_map_entry(field_desc):
                key_desc = field_desc.message_type.fields_by_name["key"]
                value_desc = field_desc.message_type.fields_by_name["value"]
                values.append(
                    {
                        _to_row_data(key_desc, key): _to_row_data(value_desc, data)
                        for key, data in field_map[field_desc.name].items()
                    }
                )
            elif field_desc.label == field_desc.LABEL_REPEATED:
                values.append(
                    [
                        _to_row_data(field_desc, data)
                        for data in field_map[field_desc.name]
                    ]
                )
            else:
                values.append(_to_row_data(field_desc, field_map[field_desc.name]))
        else:
            values.append(None)

    return values


def _to_row_data(field_descriptor, data):
    if field_descriptor.message_type is not None:
        return message_to_row(field_descriptor.message_type, data)
    # if field_descriptor.type == field_descriptor.TYPE_ENUM:
    #     return field_descriptor.enum_type.values[data].name
    return data
