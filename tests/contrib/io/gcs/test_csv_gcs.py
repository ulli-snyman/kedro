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

# pylint: disable=protected-access,no-member

from multiprocessing.reduction import ForkingPickler

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from google.appengine.ext.cloudstorage import cloudstorage_stub
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.ext import testbed


import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed

from kedro.contrib.io.gcp.csv_gcs import CSVGCSDataSet
from kedro.io import DataSetError
from kedro.io.core import Version

from google.cloud import storage

PROJECT = getenv('GCP_PROJECT')
BUCKET_NAME = getenv('GCS_BUCKET')
CREDENTIAL_PATH = getenv('GCP_CREDENTIAL_PATH')
FILENAME = "test.csv"

assert PROJECT is not None, \
    "Please Provide a GCP Project as ENV VAR: GCP_PROJECT=<gcp-project-id>"
assert BUCKET_NAME is not None, \
    "Please Provide a GCS Bucket ID as ENV VAR: GCS_BUCKET=<gcp-bucket-id>"


class testGCSCSV(unittest.TestCase):
    blob_stub = blobstore_stub.BlobstoreServiceStub(blob_storage)
    storage_stub = cloudstorage_stub.CloudStorageStub(blob_storage)

    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which will allow you to use
        # service stubs.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        self.testbed._register_stub('blobstore', self.blob_stub)
        self.testbed._register_stub("cloudstorage", self.storage_stub)

    def tearDown(self):
        # Don't forget to deactivate the testbed after the tests are
        # completed. If the testbed is not deactivated, the original
        # stubs will not be restored.
        self.testbed.deactivate()




@pytest.fixture(params=[None])
def load_args(request):
    return request.param


@pytest.fixture(params=[None])
def save_args(request):
    return request.param


@pytest.fixture(scope='module')
def storage_client():
    yield storage.Client()


@pytest.fixture(scope='module')
def bucket_object(storage_client):
    bucket_object = storage_client.get_bucket(BUCKET_NAME)
    yield bucket_object


@pytest.fixture
def gcs_data_set(load_args, save_args):
    return CSVGCSDataSet(
        filepath=FILENAME,
        bucket_name=BUCKET_NAME,
        credential_path=CREDENTIAL_PATH,
        load_args=load_args,
        save_args=save_args,
    )

@pytest.fixture
def unauthed_gcs_data_set(load_args, save_args):
    return CSVGCSDataSet(
        filepath=FILENAME,
        bucket_name=BUCKET_NAME,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def versioned_s3_data_set(load_args, save_args, load_version, save_version):
    return CSVGCSDataSet(
        filepath=FILENAME,
        bucket_name=BUCKET_NAME,
        credential_path=CREDENTIAL_PATH,
        load_args=load_args,
        save_args=save_args,
        version=Version(load_version, save_version),
    )



@pytest.fixture
def mocked_s3_object_versioned(mocked_s3_bucket, dummy_dataframe, save_version):
    """Create versioned test data and add it to mocked S3 bucket."""
    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME,
        Key="{0}/{1}/{0}".format(FILENAME, save_version),
        Body=dummy_dataframe.to_csv(index=False),
    )
    return mocked_s3_bucket


class TestCSVS3DataSet:
    @pytest.mark.parametrize(
        "bad_credentials",
        [{"aws_secret_access_key": "SECRET"}, {"aws_access_key_id": "KEY"}],
    )
    def test_incomplete_credentials_load(self, bad_credentials):
        """Test that incomplete credentials passed in credentials.yml raises exception."""
        with pytest.raises(PartialCredentialsError):
            CSVS3DataSet(
                filepath=FILENAME, bucket_name=BUCKET_NAME, credentials=bad_credentials
            )

    def test_incorrect_credentials_load(self):
        """Test that incorrect credential keys won't instantiate dataset."""
        pattern = "unexpected keyword argument"
        with pytest.raises(TypeError, match=pattern):
            CSVS3DataSet(
                filepath=FILENAME,
                bucket_name=BUCKET_NAME,
                credentials={"access_token": "TOKEN", "access_key": "KEY"},
            )

    @pytest.mark.parametrize(
        "bad_credentials",
        [{"aws_access_key_id": None, "aws_secret_access_key": None}, {}, None],
    )
    def test_empty_credentials_load(self, bad_credentials):
        s3_data_set = CSVS3DataSet(
            filepath=FILENAME, bucket_name=BUCKET_NAME, credentials=bad_credentials
        )
        pattern = r"Failed while loading data from data set CSVS3DataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            s3_data_set.load()

    def test_pass_credentials(self, mocker):
        """Test that AWS credentials are passed successfully into boto3
        client instantiation on creating S3 connection."""
        mocker.patch("s3fs.core.boto3.Session.client")
        s3_data_set = CSVS3DataSet(
            filepath=FILENAME, bucket_name=BUCKET_NAME, credentials=AWS_CREDENTIALS
        )
        pattern = r"Failed while loading data from data set CSVS3DataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            s3_data_set.load()

        assert s3fs.core.boto3.Session.client.call_count == 1
        args, kwargs = s3fs.core.boto3.Session.client.call_args_list[0]
        assert args == ("s3",)
        for k, v in AWS_CREDENTIALS.items():
            assert kwargs[k] == v

    @pytest.mark.usefixtures("mocked_s3_object")
    def test_load_data(self, s3_data_set, dummy_dataframe):
        """Test loading the data from S3."""
        loaded_data = s3_data_set.load()
        assert_frame_equal(loaded_data, dummy_dataframe)

    @pytest.mark.usefixtures("mocked_s3_object")
    def test_save_data(self, s3_data_set):
        """Test saving the data to S3."""
        new_data = pd.DataFrame(
            {"col1": ["a", "b"], "col2": ["c", "d"], "col3": ["e", "f"]}
        )
        s3_data_set.save(new_data)
        loaded_data = s3_data_set.load()
        assert_frame_equal(loaded_data, new_data)

    @pytest.mark.usefixtures("mocked_s3_bucket")
    def test_exists(self, s3_data_set, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not s3_data_set.exists()
        s3_data_set.save(dummy_dataframe)
        assert s3_data_set.exists()

    @mock_s3
    def test_exists_raises_error(self, s3_data_set):
        """Check the error if the given S3 bucket doesn't exist."""
        with pytest.raises(DataSetError, match="NoSuchBucket"):
            s3_data_set.exists()

    def test_load_save_args(self, s3_data_set):
        """Test default load and save arguments of the data set."""
        assert not s3_data_set._load_args
        assert "index" in s3_data_set._save_args

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, s3_data_set, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert s3_data_set._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, s3_data_set, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert s3_data_set._save_args[key] == value

    @pytest.mark.parametrize("save_args", [{"option": "value"}], indirect=True)
    def test_str_representation(self, s3_data_set, save_args):
        """Test string representation of the data set instance."""
        str_repr = str(s3_data_set)
        assert "CSVS3DataSet" in str_repr
        for k in save_args.keys():
            assert k in str_repr
        for secret in AWS_CREDENTIALS.values():
            assert secret not in str_repr

    def test_serializable(self, s3_data_set):
        ForkingPickler.dumps(s3_data_set)

    # pylint: disable=unused-argument
    def test_load_args_propagated(self, mocker, mocked_s3_object):
        mock = mocker.patch("kedro.io.csv_s3.pd.read_csv")
        CSVS3DataSet(
            FILENAME, BUCKET_NAME, AWS_CREDENTIALS, load_args=dict(custom=42)
        ).load()
        assert mock.call_args_list[0][1] == {"custom": 42}


@pytest.mark.usefixtures("mocked_s3_bucket")
class TestCSVS3DataSetVersioned:
    def test_save_and_load(self, versioned_s3_data_set, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_s3_data_set.save(dummy_dataframe)
        reloaded_df = versioned_s3_data_set.load()
        assert_frame_equal(reloaded_df, dummy_dataframe)

    def test_no_versions(self, versioned_s3_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for CSVS3DataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_s3_data_set.load()

    def test_exists(self, versioned_s3_data_set, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_s3_data_set.exists()

        versioned_s3_data_set.save(dummy_dataframe)
        assert versioned_s3_data_set.exists()

    @pytest.mark.usefixtures("mocked_s3_object_versioned")
    def test_prevent_override(self, versioned_s3_data_set, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding csv file for a given save version already exists in S3.
        """
        pattern = (
            r"Save path \`.+\` for CSVS3DataSet\(.+\) must not exist "
            r"if versioning is enabled"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_s3_data_set.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_s3_data_set, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            r"Save path `{f}/{sv}/{f}` did not match load path "
            r"`{f}/{lv}/{f}` for CSVS3DataSet\(.+\)".format(
                f=FILENAME, sv=save_version, lv=load_version
            )
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_s3_data_set.save(dummy_dataframe)

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        ds = CSVS3DataSet(filepath=FILENAME, bucket_name=BUCKET_NAME)
        ds_versioned = CSVS3DataSet(
            filepath=FILENAME,
            bucket_name=BUCKET_NAME,
            version=Version(load_version, save_version),
        )
        assert FILENAME in str(ds)
        assert "version" not in str(ds)

        assert FILENAME in str(ds_versioned)
        ver_str = "version=Version(load={}, save='{}')".format(
            load_version, save_version
        )
        assert ver_str in str(ds_versioned)
