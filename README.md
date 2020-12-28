# Pb2DF

A python module for converting Protobuf(3) Message type/object to Spark DafaFrame object.

## Features

- [x] proto3 type to spark schema
- [ ] proto3 object to spark dataframe

## Install

```bash
pip3 install .
# or
pip3 install git+https://github.com/zhangxianbing/pb2df
```

## Test

```bash
pytest tests
```

Result:

```bash
============================================================================================ test session starts ============================================================================================
platform linux -- Python 3.8.5, pytest-6.1.1, py-1.9.0, pluggy-0.13.1 -- /usr/bin/python3
collected 4 items

test_pb2df.py::test_schema[param0] PASSED                                                                                                                                                             [ 25%]
test_pb2df.py::test_schema[param1] PASSED                                                                                                                                                             [ 50%]
test_pb2df.py::test_schema[param2] PASSED                                                                                                                                                             [ 75%]
test_pb2df.py::test_schema[param3] PASSED                                                                                                                                                             [100%]

============================================================================================= 4 passed in 0.03s =============================================================================================

```
