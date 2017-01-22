##############
PyMongo-Zipkin
##############

An zipkin extension for PyMongo library based on py\_zipkin. This extension utilizes the monitoring functionality
introduced to PyMongo in 3.1.


************
Installation
************

.. code-block:: bash

  pip install 'git+https://github.com/mklein0/pymongo-zipkin.git#egg=PyMongo-Zipkin'


*****
Usage
*****

.. code-block:: python

  import requests
  import pymongo_zipkin


  PREAMBLE = pymongo_zipkin.ZIPKIN_THRIFT_PREAMBLE


  def http_transport(encoded_span):
      # type: (bytes) -> None

      # The collector expects a thrift-encoded list of spans. Instead of
      # decoding and re-encoding the already thrift-encoded message, we can just
      # add header bytes that specify that what follows is a list of length 1.
      url = 'http://zipkin:9411/api/v1/spans'

      body = PREAMBLE + encoded_span
      requests.post(
          url,
          data=body,
          headers={'Content-Type': 'application/x-thrift'},
      )


   pymongo_instance = pymongo_zipkin.PyMongoZipkinInstrumentation(
       http_transport, sample_rate=50.0)
   pymongo_instance.start()


*********
Reference
*********

  * http://documentation-style-guide-sphinx.readthedocs.io/en/latest/style-guide.html
