import logging

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common, InvalidResponse


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class GCSObjectStreamUpload(object):
    def __init__(
            self,
            blob_name,
            bucket,
            chunk_size=5 * 1024 * 1024,
    ):
        if isinstance(bucket, str):
            LOGGER.info('Received bucket as string. Converting to client')
            from .core import get_bucket
            bucket = get_bucket(bucket)

        self._client = bucket.client
        self._bucket = bucket
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b''
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(
            credentials=self._client._credentials
        )
        self._request = None  # type: requests.ResumableUpload

        LOGGER.info('Initiated GCS Stream with {}'.format(self.__dict__))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        url = (
            'https://www.googleapis.com/upload/storage/v1/b/'
            '{}/o?uploadType=resumable'.format(self._bucket.name)
        )
        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size
        )
        self._request.initiate(
            transport=self._transport,
            content_type='application/octet-stream',
            stream=self,
            stream_final=False,
            metadata={'name': self._blob.name},
        )

    def stop(self):
        try:
            self._request.transmit_next_chunk(self._transport)
        except InvalidResponse as e:
            print('Testing fail output in Spark')
            LOGGER.error('Error transmitting - {} - {}'.format(e, self.__dict__))

    def write(self, data):
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                self._request.transmit_next_chunk(self._transport)
            except common.InvalidResponse:
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size):
        to_read = min(chunk_size, self._buffer_size)
        LOGGER.info('Reading stream chunk type - {}'.format(type(self._buffer)))

        if isinstance(self._buffer, unicode):
            try:
                LOGGER.info('Trying to encode with ascii')
                self._buffer = self._buffer.encode('ascii')
            except UnicodeEncodeError:
                LOGGER.info('Trying to encode with utf-8')
                self._buffer = self._buffer.encode('utf-8')

        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self):
        return self._read
