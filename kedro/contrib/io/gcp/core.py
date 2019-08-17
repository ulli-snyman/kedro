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


"""This module provides functionality for working with google.cloud.storage.
https://google-cloud-python.readthedocs.io/en/latest/storage-client.html
https://pypi.python.org/pypi/google-cloud-storage
"""
from pathlib import PurePosixPath
from typing import Any, Optional
from warnings import warn

import ujson as json
from google.cloud import storage
from google.oauth2 import service_account
from loguru import logger

from kedro.io.core import (
    _PATH_CONSISTENCY_WARNING,
    DataSetError,
    Version,
    generate_current_version,
)


# pylint: disable=too-few-public-methods
class GCSMixin:
    """Mixin Class for common GCS Methods"""
    @staticmethod
    def _get_client(
            project_id: str,
            credential_path: Optional[str] = None
    ) -> Any:
        """
        Connect to the the GCP Client with credentials.
         or fallback to default creds from ENVIRONMENT.
        Args:
            project_id: ID of the GCP project for
            credential_path: Path the the json keyfile holding GCS access, ie
                ``./service_account.json``.
        Returns:
            storage.Client: GCP storage client object
        """
        logger.debug('Getting client - {}'.format(project_id))

        if credential_path:
            serv_acc = service_account.Credentials.from_service_account_file(
                credential_path
            )
            return storage.Client(project_id, credentials=serv_acc)
        return storage.Client(project_id)

    @staticmethod
    def _get_bucket(
            bucket_name: str,
            client: Any
    ) -> Any:
        """
        Load the bucket object from bucket name and option credential file.
        Args:
            bucket_name: name of the storage bucket
            client: storage object client
        Returns:
             GCS bucket object
        """
        logger.debug('Getting bucket - {}'.format(bucket_name))
        if not isinstance(bucket_name, str):
            bucket_name = bucket_name.name
        return client.get_bucket(bucket_name)

    @staticmethod
    def _get_versioned_path(filepath: str, version: str) -> str:
        """
        Append version filename to path before file name.
        Args:
            filepath: current path of the file
            version: version number to be appended to filepath
        Returns:
            filepath: updated filepath string.
        """
        filepath = PurePosixPath(filepath)
        return str(filepath / version / filepath.name)

    def _get_load_path(
        self, bucket: str, filepath: str, version: Version = None
    ) -> str:
        """
        Create a load path from filepath and optional version number.
        Args:
            bucket: bucket object to be targeted
            filepath: filepath to save the object
            version: optional version number to order data
        Returns:
             path: path to load data with confirmed location.
        """
        if not version:
            return filepath
        if version.load:
            return self._get_versioned_path(filepath, version.load)

        prefix = filepath if filepath.endswith("/") else filepath + "/"
        keys = list(bucket.list_blobs(prefix=prefix))
        if not keys:
            message = f"Did not find any versions for {str(self)}"
            raise DataSetError(message)
        return sorted(keys, reverse=True)[0]

    def _get_save_path(
            self, bucket: Any, filepath: str, version: Version = None
    ) -> str:
        """
        Get the save path string with optional versioning.
        Args:
            bucket: bucket object to be targeted.
            filepath: filepath for the object to be saved.
            version: optional version number to be append to sve path.
        :return:
        """
        if not version:
            return filepath
        save_version = version.save or generate_current_version()
        versioned_path = self._get_versioned_path(filepath, save_version)

        if versioned_path in bucket.list_blobs(prefix=versioned_path):
            message = (
                "Save path `{}` for {} must not exist if versioning "
                "is enabled.".format(versioned_path, str(self))
            )
            raise DataSetError(message)
        return versioned_path

    def _check_paths_consistency(self, load_path: str, save_path: str) -> None:
        """
        Confirm paths are consistent when saving to ensure reliability.
        Args:
            load_path: path in which the data is laoded from
            save_path: path in with the data was saved
        """
        if load_path != save_path:
            warn(_PATH_CONSISTENCY_WARNING.format(
                save_path, load_path, str(self))
            )
