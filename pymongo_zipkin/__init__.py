#
from pymongo import version_tuple as pymongo_version_tuple

if pymongo_version_tuple < (3, 1, 0):
    # Older versions of PyMongo do not support a monitoring interface
    from .wrap_functions import PyMongoZipkinInstrumentation

else:
    from .monitor import PyMongoZipkinInstrumentation


ZIPKIN_THRIFT_PREAMBLE = '\x0c\x00\x00\x00\x01'.encode()


__version__ = '0.1+dev0'
