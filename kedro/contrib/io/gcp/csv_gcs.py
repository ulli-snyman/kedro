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

"""``CSVgcsDataSet`` loads and saves data to a file in gcs.
It uses google python client librarys to read and write CSV's from GCS.
"""
from typing import Any, Dict, Optional
from io import BytesIO

import pandas as pd

from kedro.io.core import AbstractDataSet, DataSetError, Version
from .core import GCSMixin
from .stream_to_gcs import GCSObjectStreamUpload


# pylint: disable=too-few-public-methods
class CSVGCSDataSet(AbstractDataSet, GCSMixin):
    """``CSVS3DataSet`` loads and saves data to a file in GCS.
    It uses the google python client library to load csv into pd dataframes.
    Saves dfs to a tempfile then uploads to gcs bucket.

    Example:
    ::

        >>> from kedro.io import CSVGCSDataSet
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({'col1': [1, 2], 'col2': [4, 5],
        >>>                      'col3': [5, 6]})
        >>>
        >>> data_set = CSVGCSDataSet(filepath="test.csv",
        >>>                         bucket_name="test_bucket",
        >>>                         load_args=None,
        >>>                         credential_path="./key.json"
        >>>                         save_args={"index": False})
        >>> data_set.save(data)
        >>> reloaded = data_set.load()
        >>>
        >>> assert data.equals(reloaded)
    """

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            client=self._client,
            bucket=self._bucket,
            load_args=self._load_args,
            save_args=self._save_args,
            version=self._version,
        )

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        bucket_name: str,
        project_id: Optional[str] = None,
        credential_path: Optional[str] = None,
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        version: Version = None,
    ) -> None:
        """Creates a new instance of ``CSVS3DataSet`` pointing to a concrete
        csv file on GCS.

        Args:
            filepath: Path to a csv file in gcs.
            bucket_name: GCS bucket name.
            project_id: the id of the GCP project your are working in.
            credential_path: Path to service account if available else fallback to deafult auth
                ``./service-account.json``
            load_args: Pandas options for loading csv files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
                All defaults are preserved.
            save_args: Pandas options for saving csv files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
                All defaults are preserved, but "index", which is set to False.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.

        """
        default_save_args = {"index": False}
        self._save_args = (
            {**default_save_args, **save_args} if save_args else default_save_args
        )
        self._load_args = load_args if load_args else {}
        self._filepath = filepath
        self._version = version
        self._client = self._get_client(project_id, credential_path)
        self._bucket = self._get_bucket(bucket_name, self._client)

    def _load(self) -> pd.DataFrame:
        load_key = self._get_load_path(
            self._bucket, self._filepath, self._version
        )
        blob = self._bucket.get_blob(load_key) or self._bucket.blob(load_key)
        byte_stream = BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)
        return pd.read_csv(byte_stream, **self._load_args)

    def _save(self, data: pd.DataFrame) -> None:
        save_key = self._get_save_path(
            self._bucket, self._filepath, self._version
        )

        csv_str = data.to_csv(**self._save_args).encode()
        with GCSObjectStreamUpload(save_key, self._bucket) as file:
                file.write(csv_str)

        load_key = self._get_load_path(
             self._bucket, self._filepath, self._version
        )
        self._check_paths_consistency(load_key, save_key)

    def _exists(self) -> bool:
        try:
            load_key = self._get_load_path(
                self._bucket, self._filepath, self._version
            )
        except DataSetError:
            return False

        blob = self._bucket.get_blob(load_key)
        return bool(blob)
