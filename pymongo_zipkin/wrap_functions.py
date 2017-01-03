#
"""
https://gist.github.com/kesor/1589672
"""
import struct

import bson
from bson.errors import InvalidBSON

from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import zipkin_client_span, create_attrs_for_span, ZipkinAttrs

try:
    from pymongo.mongo_client import MongoClient as Connection

except ImportError:
    from pymongo.connection import Connection


class PyMongoZipkinInstrumentation(object):

    def __init__(self, transport_handler, sample_rate=100.0):
        # type: (typing.Callable[[bytes], None], float) -> None
        self.sample_rate = sample_rate
        self.transport_handler = transport_handler
        self.started = False

    def start(self):
        if self.started:
            return

        self.wrap_connection()
        self.started = True

    def stop(self):
        if not self.started:
            return

        self.unwrap_connection()
        self.started = False

    def wrap_connection(self):
        # save original methods
        self.orig_send_message = Connection._send_message
        self.orig_send_message_with_response = Connection._send_message_with_response

        # instrument methods to record messages
        Connection._send_message = self._instrument(Connection._send_message)
        Connection._send_message_with_response = self._instrument(Connection._send_message_with_response)

    def unwrap_connection(self):
        # remove instrumentation from pymongo
        Connection._send_message = self.orig_send_message
        Connection._send_message_with_response = self.orig_send_message_with_response

    def zipkin_client_span(self, message):
        # type: (dict) -> py_mongo.zipkin.zipkin_client_span

        zipkin_attrs = get_zipkin_attrs()  # type: py_zipkin.zipkin.ZipkinAttrs
        """:type: py_zipkin.zipkin.ZipkinAttrs"""

        if zipkin_attrs:
            zipkin_attrs = ZipkinAttrs(
                trace_id=zipkin_attrs.trace_id,
                span_id=generate_random_64bit_string(),
                parent_span_id=zipkin_attrs.span_id,
                flags=zipkin_attrs.flags,
                is_sampled=zipkin_attrs.is_sampled,
            )

        else:
            zipkin_attrs = create_attrs_for_span(self.sample_rate)

        database, collection = message['collection'].split('.', 1)
        operation = message['op']

        span = zipkin_client_span(
            service_name='pymongo.{}'.format(database),
            span_name=operation,
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        return span

    def annotate_span(self, span, message):
        # type: (py_mongo.zipkin.zipkin_client_span, dict) -> None

        database, collection = message['collection'].split('.', 1)

        # Can only update annotations after span starts
        span.update_binary_annotations_for_root_span({
            'mongo.database': database,
            'mongo.collection': collection,
            'mongo.message.id': message['msg_id'],
            # 'mongo.connection_id': event.connection_id,
            # 'mongo.operation.id': message['msg_id'],
            # 'mongo.request_id': event.request_id,
            'mongo.statement': message['query'],
        })

    def _instrument(self, original_method):
        # type: (callable) -> callable
        def instrumented_method(connection_self, mdb_message, *args, **kwargs):

            message = _mongodb_decode_wire_protocol_message(mdb_message[1])

            with self.zipkin_client_span(message) as span:
                self.annotate_span(span, message)
                result = original_method(connection_self, mdb_message, *args, **kwargs)

                # response = _mongodb_decode_wire_protocol_response(result[1][0])
                # # Add PyMongo attributes to span before stopping it. Noop is sampling is not set or 0
                # span.update_binary_annotations_for_root_span({
                #     'mongo.response': response['flags'],
                #     'mongo.num_documents': response['num_documents'],
                # })

            return result

        return instrumented_method


MONGO_OPS = {
    0001: 'reply',
    1000: 'msg',
    2001: 'update',
    2002: 'insert',
    2003: 'reserved',
    2004: 'query',
    2005: 'get_more',
    2006: 'delete',
    2007: 'kill_cursors',
    2010: 'command',
    2011: 'command_reply',
}

MONGO_RESPONSES = {
    0x01: 'getMore',
    0x01 << 1: 'failure',
    0x01 << 2: 'shardConfigStale',
    0x01 << 3: 'awaitCapable',
}


def _mongodb_decode_wire_protocol_message(enc_message):
    # type: (bytes) -> dict
    """ http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol """

    # Standard message header (message length, request ID, response To, Op Code)
    _, msg_id, _, opcode, _ = struct.unpack('<iiiii', enc_message[:20])
    op = MONGO_OPS.get(opcode, 'unknown')

    # read cstring containing database & collection.
    zidx = 20
    collection_name_size = enc_message[zidx:].find('\0')
    collection_name = enc_message[zidx:zidx + collection_name_size]
    zidx += collection_name_size + 1

    skip, limit = struct.unpack('<ii', enc_message[zidx:zidx + 8])
    zidx += 8

    msg = ""
    try:
        msg = bson.decode_all(enc_message[zidx:])

    except InvalidBSON:
        msg = 'invalid bson'

    return dict(
        op=op,
        collection=collection_name,
        msg_id=msg_id,
        skip=skip,
        limit=limit,
        query=msg,
    )


def _mongodb_decode_wire_protocol_response(enc_response):
    # type: (bytes) -> dict
    """ http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol """

    # Standard message header (message length, request ID, response To, Op Code)
    _, msg_id, _, opcode, flags = struct.unpack('<iiiii', enc_response[:20])
    op = MONGO_OPS.get(opcode, 'unknown')

    # Standard message header (message length, request ID, response To, Op Code)
    num_documents = struct.unpack('<i', enc_response[32:36])

    return dict(
        op=op,
        msg_id=msg_id,
        flags=tuple(MONGO_RESPONSES[0x1 << i] for i in range(4)),
        num_documents=num_documents,
    )
