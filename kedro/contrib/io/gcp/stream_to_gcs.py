# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited (“QuantumBlack”) name and logo
# (either separately or in combination, “QuantumBlack Trademarks”) are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.


"""This module provides functionality for streaming data to and from GCS.
"""

from typing import Any, Optional

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import InvalidResponse, common, requests
from loguru import logger


class GCSObjectStreamUpload:
    """
    Stream objects to GCS in a specified chunksize.
    """
    def __init__(
            self,
            path_to_blob: str,
            bucket: Any,
            chunk_size: Optional[int] = 5 * 1024 * 1024,
    ) -> None:
        """
        Args:
            path_to_blob: path and filename of object to be saved
            bucket: bucket object being targeted.
            chunk_size: size in chunks in bytes to transmit.
        """

        self._blob = bucket.blob(path_to_blob)

        self._buffer = b''
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(
            # pylint: disable=protected-access
            credentials=self._blob.bucket.client._credentials
        )
        self._request = None  # type: requests.ResumableUpload

        logger.info('Initiated GCS Stream with {}'.format(self.__dict__))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type: str, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        """
        Make start request to GCS server.
        """
        url = (
            'https://www.googleapis.com/upload/storage/v1/b/'
            '{}/o?uploadType=resumable'.format(self._blob.bucket.name)
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
        """
        Stop streaming to gcs.
        """
        try:
            self._request.transmit_next_chunk(self._transport)
        except InvalidResponse as exception:
            logger.error(f'Error transmitting - {exception} - {self.__dict__}')

    def write(self, data: Any) -> int:
        """
        Stream data to GCS.
        Args:
            data: bytes to be uploaded.
        Returns:
            data_len: Size of the file that was uploaded in bytes.
        """
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

    def read(self, chunk_size: Optional[int] = None) -> Any:
        """
        Read data from GCS.
        Args:
            chunk_size: size in bytes to read.
        Returns:
             Data: data as a bytrestream,
        """
        chunk_size = self._chunk_size or chunk_size
        to_read = min(chunk_size, self._buffer_size)
        logger.info('Reading stream chunk type - {}'.format(type(self._buffer)))

        if isinstance(self._buffer, str):
            try:
                logger.info('Trying to encode with ascii')
                self._buffer = self._buffer.encode('ascii')
            except UnicodeEncodeError:
                logger.info('Trying to encode with utf-8')
                self._buffer = self._buffer.encode('utf-8')

        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self):
        return self._read
