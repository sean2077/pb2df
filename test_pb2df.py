"""pytest file for testing pb2df"""
import pb2df


def test_schema(param):
    print(param)
    pb_msg_type, expected_schema = param.pb_msg_type, param.expected_schema
    assert pb_msg_type == expected_schema