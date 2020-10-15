#
"""

"""
import threading

from pymongo import monitoring

from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import zipkin_client_span, create_attrs_for_span, ZipkinAttrs


"""
def http_transport(encoded_span):
    # type: (bytes) -> None
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body = b'\x0c\x00\x00\x00\x01' + encoded_span
    requests.post(
        'http://localhost:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )
"""


class _PyMongoInsrumentationLocal(threading.local):

    def set_span(self, key, span):
        # type: (tuple, py_zipkin.zipkin.zipkin_span) -> None
        context = self.__dict__.setdefault('zipkin_span', {})
        context[key] = span

    def get_span(self, key):
        # type: (tuple) -> py_zipkin.zipkin.zipkin_span
        context = self.__dict__.get('zipkin_span')
        if context is None:
            return None

        return context.get(key)

    def del_span(self, key):
        # type: (tuple) -> py_zipkin.zipkin.zipkin_span
        context = self.__dict__.get('zipkin_span')
        if context is None:
            return None
        if key in context:
            del context[key]

    def safe_pop_span(self, key):
        context = self.__dict__.get('zipkin_span')
        if context is None:
            return None

        val = context.get(key)
        if val is not None:
            del context[key]

        return val


_local = _PyMongoInsrumentationLocal()


class PyMongoZipkinInstrumentation(monitoring.CommandListener):

    def __init__(self, transport_handler, sample_rate=100.0):
        # type: (typing.Callable[[bytes], None], float) -> None
        self.sample_rate = sample_rate
        self.transport_handler = transport_handler
        self.started = False

    def start(self):
        if self.started:
            return

        monitoring.register(self)

    def stop(self):
        pass

    def _determine_key(self, event):
        # type: (pymongo.monitoring._CommandEvent) -> tuple
        return (event.connection_id, event.operation_id, event.request_id)

    def started(self, event):
        # type: (pymongo.monitoring.CommandStartedEvent) -> None
        key = self._determine_key(event)

        zipkin_attrs = get_zipkin_attrs()  # type: py_zipkin.zipkin.ZipkinAttrs
        """:type: py_zipkin.zipkin.ZipkinAttrs"""

        if zipkin_attrs:
            zipkin_attrs = ZipkinAttrs(
                trace_id=zipkin_attrs.trace_id,
                span_id=generate_random_64bit_string(),
                parent_span_id=zipkin_attrs.parent_span_id,
                flags=zipkin_attrs.flags,
                is_sampled=zipkin_attrs.is_sampled,
            )

        else:
            zipkin_attrs = create_attrs_for_span(self.sample_rate)

        span = zipkin_client_span(
            service_name='pymongo',
            span_name=event.database_name,
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        _local.set_span(key, span)
        span.start()
        self.annotate_started_event(span, event)

    def annotate_started_event(self, span, event):
        # type: (py_zipkin.zipkin.zipkin_client_span, pymongo.monitoring.CommandStartedEvent) -> None
        operation = ''
        statement = event.command
        # if operation in ('insert', 'update'):
        #     statement = '<redacted>'

        # Can only update annotations after span starts
        span.update_binary_annotations({
            'mongo.connection_id': event.connection_id,
            'mongo.operation_id': event.operation_id,
            'mongo.request_id': event.request_id,
            'mongo.operation': operation,
            'mongo.statement': statement,
        })

    def succeeded(self, event):
        # type: (pymongo.monitoring.CommandSucceededEvent) -> None

        key = self._determine_key(event)
        span = _local.safe_pop_span(key)
        if span is None:
            return

        self.annotate_succeeded_event(span, event)
        span.stop()

    def annotate_succeeded_event(self, span, event):
        # type: (py_zipkin.zipkin.zipkin_client_span, pymongo.monitoring.CommandSucceededEvent) -> None

        # Add PyMongo attributes to span before stopping it. Noop is sampling is not set or 0
        span.update_binary_annotations({
            'mongo.reply': event.reply,
        })

    def failed(self, event):
        # type: (pymongo.monitoring.CommandFailedEvent) -> None

        key = self._determine_key(event)
        span = _local.safe_pop_span(key)
        if span is None:
            return

        self.annotate_failed_event(span, event)
        span.stop()

    def annotate_failed_event(self, span, event):
        # type: (py_zipkin.zipkin.zipkin_client_span, pymongo.monitoring.CommandFailedEvent) -> None

        # Add PyMongo attributes to span before stopping it. Noop is sampling is not set or 0
        span.update_binary_annotations({
            'mongo.failure': event.failure,
        })
