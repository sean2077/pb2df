"""pytest file for testing pb2df"""
import example_pb2
from pb2df import proto3_message_type_to_spark_schema


def _test_schema(pb_msg_type, expected_schema):
    schema = proto3_message_type_to_spark_schema(pb_msg_type)
    assert schema == expected_schema


def test_schema(param):
    _test_schema(param.pb_msg_type, param.expected_schema)


if __name__ == "__main__":
    # pb_msg = example_pb2.MapMessage
    # log = {}
    # print(f"{'field':>30} | {'type':<30}")
    # for field_name, field_desc in pb_msg.DESCRIPTOR.fields_by_name.iteritems():
    #     print(f"{field_name:>30} | {field_desc.message_type.name:<30}")
    #     log[field_name] = field_desc

    pb_msg = example_pb2.TimeMessage
    schema = proto3_message_type_to_spark_schema(pb_msg)
    print(schema)