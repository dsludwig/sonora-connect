# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connectrpc/conformance/v1/suite.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from connectrpc.conformance.v1 import client_compat_pb2 as connectrpc_dot_conformance_dot_v1_dot_client__compat__pb2
from connectrpc.conformance.v1 import config_pb2 as connectrpc_dot_conformance_dot_v1_dot_config__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%connectrpc/conformance/v1/suite.proto\x12\x19\x63onnectrpc.conformance.v1\x1a-connectrpc/conformance/v1/client_compat.proto\x1a&connectrpc/conformance/v1/config.proto\"\xc6\x06\n\tTestSuite\x12\x0c\n\x04name\x18\x01 \x01(\t\x12;\n\x04mode\x18\x02 \x01(\x0e\x32-.connectrpc.conformance.v1.TestSuite.TestMode\x12\x37\n\ntest_cases\x18\x03 \x03(\x0b\x32#.connectrpc.conformance.v1.TestCase\x12?\n\x12relevant_protocols\x18\x04 \x03(\x0e\x32#.connectrpc.conformance.v1.Protocol\x12\x46\n\x16relevant_http_versions\x18\x05 \x03(\x0e\x32&.connectrpc.conformance.v1.HTTPVersion\x12\x39\n\x0frelevant_codecs\x18\x06 \x03(\x0e\x32 .connectrpc.conformance.v1.Codec\x12\x45\n\x15relevant_compressions\x18\x07 \x03(\x0e\x32&.connectrpc.conformance.v1.Compression\x12U\n\x14\x63onnect_version_mode\x18\x08 \x01(\x0e\x32\x37.connectrpc.conformance.v1.TestSuite.ConnectVersionMode\x12\x15\n\rrelies_on_tls\x18\t \x01(\x08\x12\"\n\x1arelies_on_tls_client_certs\x18\n \x01(\x08\x12\x1d\n\x15relies_on_connect_get\x18\x0b \x01(\x08\x12\'\n\x1frelies_on_message_receive_limit\x18\x0c \x01(\x08\"Q\n\x08TestMode\x12\x19\n\x15TEST_MODE_UNSPECIFIED\x10\x00\x12\x14\n\x10TEST_MODE_CLIENT\x10\x01\x12\x14\n\x10TEST_MODE_SERVER\x10\x02\"}\n\x12\x43onnectVersionMode\x12$\n CONNECT_VERSION_MODE_UNSPECIFIED\x10\x00\x12 \n\x1c\x43ONNECT_VERSION_MODE_REQUIRE\x10\x01\x12\x1f\n\x1b\x43ONNECT_VERSION_MODE_IGNORE\x10\x02\"\xf6\x02\n\x08TestCase\x12?\n\x07request\x18\x01 \x01(\x0b\x32..connectrpc.conformance.v1.ClientCompatRequest\x12I\n\x0f\x65xpand_requests\x18\x02 \x03(\x0b\x32\x30.connectrpc.conformance.v1.TestCase.ExpandedSize\x12J\n\x11\x65xpected_response\x18\x03 \x01(\x0b\x32/.connectrpc.conformance.v1.ClientResponseResult\x12\x42\n\x19other_allowed_error_codes\x18\x04 \x03(\x0e\x32\x1f.connectrpc.conformance.v1.Code\x1aN\n\x0c\x45xpandedSize\x12#\n\x16size_relative_to_limit\x18\x01 \x01(\x05H\x00\x88\x01\x01\x42\x19\n\x17_size_relative_to_limitb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'connectrpc.conformance.v1.suite_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_TESTSUITE']._serialized_start=156
  _globals['_TESTSUITE']._serialized_end=994
  _globals['_TESTSUITE_TESTMODE']._serialized_start=786
  _globals['_TESTSUITE_TESTMODE']._serialized_end=867
  _globals['_TESTSUITE_CONNECTVERSIONMODE']._serialized_start=869
  _globals['_TESTSUITE_CONNECTVERSIONMODE']._serialized_end=994
  _globals['_TESTCASE']._serialized_start=997
  _globals['_TESTCASE']._serialized_end=1371
  _globals['_TESTCASE_EXPANDEDSIZE']._serialized_start=1293
  _globals['_TESTCASE_EXPANDEDSIZE']._serialized_end=1371
# @@protoc_insertion_point(module_scope)
