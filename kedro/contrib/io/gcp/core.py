"""For working with google.cloud.storage.
https://google-cloud-python.readthedocs.io/en/latest/storage-client.html
https://pypi.python.org/pypi/google-cloud-storage
"""
import gzip
from io import BytesIO
import logging
import os
import shutil
import ujson as json

from stream_to_gcs import GCSObjectStreamUpload

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import argparse

try:
    import cPickle as pickle
except ImportError:
    import pickle

from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_PROJECT_ID = 'skim-audience'


def get_client(project_id=DEFAULT_PROJECT_ID):
    """
    Connect to the the GCP Client.
    :param project_id: Name of the GCP project
    :return: Stroage client object
    """
    logger.info('Getting client - {}'.format(project_id))
    return storage.Client(project_id)


def get_bucket(bucket_name):
    """
    Load a bucket object from GCS.
    :param bucket_name: name of the storage bucket
    :return: GCS bucket object
    """
    logger.info('Getting bucket - {}'.format(bucket_name))
    client = get_client()
    if not isinstance(bucket_name, str):
        bucket_name = bucket_name.name
    return client.get_bucket(bucket_name)


def create_bucket(bucket_name):
    """
    Create a bucket in the default credentials project.
    :param bucket_name: The bucket name we want to create in our project.
    :type bucket_name: str
    :return: None
    :rtype: None
    """
    logger.info('Creating bucket - {}'.format(bucket_name))
    client = get_client()
    if not isinstance(bucket_name, str):
        bucket_name = bucket_name.name
    client.create_bucket(bucket_name)
    logger.info("created bucket '{}'".format(bucket_name))


def bucket_exists(bucket_name):
    """
    Check if a bucket exists.
    :param bucket_name: The bucket name to check for existance.
    :type bucket_name: str
    :return: True if bucket exists in project.
    :rtype: bool
    """
    logger.info('Checking bucket exists - {}'.format(bucket_name))
    client = get_client()
    if not isinstance(bucket_name, str):
        bucket_name = bucket_name.name
    if client.lookup_bucket(bucket_name):
        return True
    return False


def blob_exists(static_data_bucket, path):
    """
    Check if a file exists in a bucket.
    :param static_data_bucket:
    :param path:
    :return:
    """
    logger.info('Checkig blob exists - {} - {}'.format(static_data_bucket, path))
    bucket = get_bucket(static_data_bucket)
    blob = bucket.get_blob(path)
    return True if blob else False


def file_exists(bucket_name, blob_name):
    """Check if blob exists in bucket.

    :param bucket_name: the bucket to check in
    :param blob_name: the blob to look for
    :return:
    """
    logger.info('Checking file exists - {} - {}'.format(bucket_name, blob_name))
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()


def file_is_empty(static_data_bucket, path):
    """
    See if a file on gcs contains any data
    :param static_data_bucket:
    :param path:
    :return:
    """
    logger.info('Checking file exists - {} - {}'.format(
        static_data_bucket, path))
    if not isinstance(static_data_bucket, str):
        static_data_bucket = static_data_bucket.name

    if not file_exists(static_data_bucket, path):
        return True

    blob_str = get_file_as_string(static_data_bucket, path)
    return not len(blob_str) > 0


def get_contents_of_directory(directory, bucket=None):
    logger.info('Getting contents of directory - {} - {}'.format(
        directory, bucket))
    if not bucket:
        bucket, directory = _get_bucket_and_path_from_uri(directory)

    return list_blobs_with_prefix(bucket, directory)


def list_blobs_with_prefix(bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".
         /a/1.txt
        /a/b/2.txt

    If you just specify prefix = '/a', you'll get back:

        /a/1.txt
        /a/b/2.txt

    """
    logger.info('Listing blobs with prefix - {} - {}'.format(
        bucket_name, prefix))
    bucket = get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return list(blobs)


def delete_bucket(bucket_name):
    """Delete the given bucket.
    :param bucket_name: The bucket name
    :type bucket_name: str
    :return: None
    :rtype: None
    """
    logger.info('Deleting bucket - {}'.format(bucket_name))
    bucket = get_bucket(bucket_name)
    bucket.delete()


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    logger.info('Deleting blob - {} - {}'.format(bucket_name, blob_name))
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.delete()


def clear_bucket_uri(uri):
    logger.info('Clearing bucket from URI - {}'.format(uri))
    bucket, path = _get_bucket_and_path_from_uri(uri)
    clear_bucket(bucket, path)


def clear_bucket(bucket_name, blob_prefix=''):
    """Clear the bucket of all content within it.
    :param bucket_name: The bucket to clear.
    :type bucket_name: str
    :param blob_prefix: The prefix of all objects within the objects.
    :type blob_prefix: str
    :return: None
    :rtype: None
    """
    logger.info('Clearing bucket - {} - {}'.format(bucket_name, blob_prefix))
    bucket = get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=blob_prefix)
    for blob in blobs:
        logger.info("Deleting '{}'".format(blob.name))
        bucket.delete_blob(blob.name)


def list_bucket(bucket_name, prefix=''):
    """List all of the items within a bucket.
    :param bucket_name: Name of the bucket
    :type bucket_name: str
    :param prefix: Prefix of the files for the bucket.
    :type prefix: str or None
    :return: List of items in bucket.
    :rtype: list of dict
    """
    logger.info('Listing bucket - {} - {}'.format(bucket_name, prefix))
    blobs = list_bucket_blobs(bucket_name, prefix)
    return [o.__dict__ for o in blobs]


def list_bucket_blobs(bucket_name, prefix=''):
    logger.info('Listing bucket blobs - {} - {}'.format(bucket_name, prefix))
    return get_bucket(bucket_name).list_blobs(prefix=prefix)


def get_file_as_string(bucket_name, file_path):
    """
    Get the file in a bucket and return as a string.
    :param bucket_name:
    :param file_path:
    :return blob: file as a string
    """
    logger.info('Getting file as string - {} - {}'.format(
        bucket_name, file_path))
    bucket = get_bucket(bucket_name)
    blob = bucket.get_blob(file_path) or bucket.blob(file_path)
    return blob.download_as_string()


def load_file(bucket_name, file_path, path_to_write):
    """
    Load file from gcs bucket into a file object
    :param bucket_name:
    :param config_file_path:
    :return:
    """
    logger.info('Loading file - {} - {} - {}'.format(
        bucket_name, file_path, path_to_write))
    bucket = get_bucket(bucket_name)
    blob = bucket.get_blob(file_path) or bucket.blob(file_path)
    return blob.download_to_file(path_to_write)


def load_pickle_uri(uri):
    logger.info('Loading pickle URI - {}'.format(uri))
    bucket, path = _get_bucket_and_path_from_uri(uri)
    return load_pickle(bucket, path)


def load_pickle(bucket, path):
    """Loads pickle files from GCS, retries on the provided exception.

    We retry on EOF and Value errors as these occasionally occur in production
    and disappear upon re-run, leading us to believe they are S3 related
    """
    logger.info('Loading pickle - {} - {}'.format(bucket, path))
    if not isinstance(bucket, str):
        bucket = bucket.name

    pkl = get_file_as_string(bucket, path)
    try:
        return pickle.loads(pkl, encenrich_commissionsoding='utf-8')  # python3
    except TypeError:
        return pickle.loads(pkl)  # python2


def load_jsonfile(bucket, path, **kwargs):
    file_contents = get_file_as_string(bucket, path)
    return [json.loads(item) for item in file_contents.splitlines()]


def save_pickle(bucket, path, content):
    logger.info('Saving pickle - {} - {}'.format(bucket, path))
    upload_string_to_file(bucket, path, pickle.dumps(content))


def copy_storage_dir_to_local(bucket_name,
                              prefix,
                              local_path,
                              clear_local_first=True):
    """Copy data from cloud over to local.
    :param bucket_name: Bucket path where files are stored.
    :type bucket_name: str
    :param prefix: Storage prefix path where files are stored.
    :type prefix: str
    :param local_path: Local path where data is stored.
    :type local_path: str
    :param clear_local_first: True: we want to clear local directory first
    :type clear_local_first: bool
    :return: None
    :rtype: None
    """
    logger.info('Copying storage dir to local - {} - {} - {}'.format(
        bucket_name, prefix, local_path))
    if isinstance(bucket_name, str):
        bucket = get_bucket(bucket_name)
    else:
        bucket = bucket_name

    blobs = [blob for blob in bucket.list_blobs(prefix=prefix)]

    # clear the local directory first
    if clear_local_first and os.path.isdir(local_path):
        logger.info("clearing '{}'".format(local_path))
        shutil.rmtree(local_path)

    if not os.path.isdir(local_path):
        logger.info("creating '{}'".format(local_path))
        os.makedirs(local_path)

    for blob in blobs:
        if blob.name.endswith("/"):
            # First blob is the folder
            continue

        filename = "{}".format(blob.name.split('/')[-1])
        source_name = "gs://{}/{}".format(bucket.name, blob.name)
        local_filename = os.path.join(local_path, filename)
        logger.info("Downloading '{}' to '{}'...".format(
            source_name, local_filename))

        with open(local_filename, 'wb') as file_obj:
            blob.download_to_file(file_obj)

    logger.info("Finished copying storage files to local")


def upload_object(local_path, bucket_name, prefix=''):
    """Upload the local model up to cloud storage.
    :param local_path: Local location of the file to upload.
    :type local_path: str
    :param bucket_name: Target google storage bucket for file.
    :type bucket_name: str
    :param prefix: Target gs 'prefix': The rest of the path after bucket.
    :type prefix: str
    :return: None
    :rtype: None
    """
    logger.info('Uploading object - {} - {} - {}'.format(
        bucket_name, prefix, local_path))
    bucket = get_bucket(bucket_name)

    # If we have a directory, go through and get each file
    files_to_upload = []
    for root, subfolders, files in os.walk(local_path):
        for file in files:
            filepath = os.path.join(root, file)
            files_to_upload.append(filepath)

    # if we have a single file, then the list will be empty
    if not files_to_upload:
        files_to_upload.append(local_path)

    for upload_file in files_to_upload:
        filename = os.path.basename(upload_file)
        blob_name = os.path.join(prefix, filename)
        blob = bucket.blob(blob_name)
        try:
            blob.upload_from_filename(filename=upload_file)
            logger.info(
                "uploaded '{}' to '{}'".format(upload_file, blob_name)
            )
        except Exception:
            # in case there are temporary cached files
            logger.warning("'{}' not found".format(upload_file))
    logger.info("uploading '{}' done.".format(local_path))


def upload_string_to_file(bucket, path, content):
    """
    Upload a string to a specified path and file on gcs.
    :param bucket: Bucket to upload to.
    :param path: path in the bucket for the file to be uploaded to.
    :param content: the string that needs to be written to the file.
    :return:
    """
    logger.info('Uploading string to file - {} - {}'.format(bucket, path))
    bucket = get_bucket(bucket)
    blob = bucket.blob(path)
    blob.upload_from_string(content)


def save_to_gcs(bucket, path, data):
    logger.info('Saving to GCS - {} - {}'.format(bucket, path))
    bucket = get_bucket(bucket)
    upload_string_to_file(bucket, path, data)


def save_to_storage_stream(filepath, data):
    """Writes an iterable of strings to a text file
        Args:
            filepath: (str) full file path (file:// or gs://)
            data: list(str) iterable
        """
    logger.info('Saving to Storage Stream - {} - {}'.format(filepath, data))
    if filepath.endswith('/'):
        raise ValueError('The storage path must '
                         'be a filename, not a directory')
    try:
        bucket, path = _get_bucket_and_path_from_uri(filepath)
        upload_string_to_file(bucket, path, '\n'.join(data))
        logger.info('File written in : %s' % filepath)
    except IOError as e:
        logger.error('The path does not exist - {}'.format(e))
        raise


def _get_bucket_and_path_from_uri(uri):
    """Returns a list containing GS bucket and path from a URI.

    :param path: (str) a S3 uri -> 's3n://bucket/path-to/something'
    :return: A tuple containing the S3 bucket and path
     -> (bucket, path-to/something)
    """
    logger.info('Converting URI to bucket and path - {}'.format(uri))
    return extract_bucket_name_from_uri(uri), get_file_relative_to_bucket(uri)


def smart_open(uri):
    logger.info('Smart Opening - {}'.format(uri))
    bucket_name = extract_bucket_name_from_uri(uri)
    destination_blob_name = get_file_relative_to_bucket(uri)

    bucket = get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    return blob.download_as_string()


def extract_bucket_name_from_uri(uri):
    """Extract the bucket name from a GS URI.

    :param uri: the URI to process
    :return: the bucket name
    """
    logger.info('Extracting bucket name from URI - {}'.format(uri))
    return uri.lstrip('gs://').split('/')[0]


def get_file_relative_to_bucket(uri):
    """

    :param uri:
    :return:
    """
    logger.info('Getting file relative to bucket - {}'.format(uri))
    return '/'.join(uri.lstrip('gs://').split('/')[1:])


def get_gcs_filename(gcs_path):
    """Fetches the filename of a key from GCS
    Args:
        gcs_path (str): 'production/output/file.txt'
    Returns (str): 'file.txt'
    """
    logger.info('Getting GCS filename - {}'.format(gcs_path))
    if gcs_path.split('/')[-1] == '':
        raise ValueError('Supplied gcs path: {} '
                         'is a directory not a file path'.format(gcs_path))
    return gcs_path.split('/')[-1]


def storage_uri(uri):
    """Check whether the provided URI is a valid GCS path.

    :param uri: the URI to check
    :return: True if it is a valid GCS path; False otherwise
    """
    logger.info('Storage URI - {}'.format(uri))
    if not uri.startswith('gs://'):
        raise argparse.ArgumentTypeError('{} is not a valid URI'.format(uri))
    return uri


def bucket_obj(name):
    logger.info('Getting bucket object - {}'.format(name))
    return get_bucket(name)


def save_partitions_as_csv(output_rdd, output_path_uri,
                           header_text, output_filename_func,
                           encoding='utf-8'):
    """Save an rdd as a group of csv files, one per partition.

    Args:
        output_rdd (pyspark.RDD): Data we want to save
        output_path_uri (str): Folder we want the files saved into
        header_text (str|list of str): The header text for each csv,
             can also be a list of str,
             in which case it is changed to a comma seperated string
        output_filename_func (int -> str): Function that takes an int
        encoding (str):
    """
    logger.info('Saving partition as csv - {} - {}'.format(
        output_path_uri, output_filename_func))
    if isinstance(header_text, list):
        header_text_encodeed = ','.join(header_text).encode(encoding)
    else:
        header_text_encodeed = header_text.encode(encoding)

    def map_write(i, part):
        output_file = '{}{}'.format(output_path_uri, output_filename_func(i))
        with open('/tmp/output', "wb") as f:
            f.write(header_text_encodeed)
            f.write(b'\n')
            for line in part:
                f.write(line)
                f.write(b'\n')

            bucket_name, path = _get_bucket_and_path_from_uri(output_file)
            bucket = get_bucket(bucket_name)
            blob = bucket.get_blob(path) or bucket.blob(path)
            blob.upload_from_file(f, True)

        # we return to so that this method result
        # can be collected in the last line of this method
        return [None]

    return output_rdd.mapPartitionsWithIndex(map_write).collect()


def fetch_gcs_keys_by_regex_pattern(gcs_bucket, gcs_directory, pattern):
    """Fetches the gcs keys of all files within the supplied directory.
     using a regex pattern match.

    Args:
        bucket (boto.s3.bucket.Bucket):
        directory (str): 'production/output/'
        pattern (re.RegexObject): compiled regex pattern,
         e.g re.compile('.*\d+$')
    Returns: ([boto.s3.key.Key, boto.s3.key.Key...]):
    """
    logger.info('Fetching GCS keys by regex pattern - {} - {} - {}'.format(
        gcs_bucket, gcs_directory, pattern))
    bucket_contents = list_blobs_with_prefix(gcs_bucket, gcs_directory)
    return [key for key in bucket_contents if pattern.search(key.name)]


def get_file_as_string_from_gcs(bucket, path, compressed=False):
    """Load data from storage into a string object
        with option for compressed data.
    :param bucket:
    :param path:
    :param compressed:
    :return: File contents as string
    """
    logger.info('Getting file as string from GCS - {} - {}'.format(bucket, path))
    data = get_file_as_string(bucket, path)

    if compressed:
        with gzip.GzipFile(fileobj=BytesIO(data), mode="r") as f:
            data = f.read()

    return data


def file_size(bucket, file_path):
    """Get the size of a file. Raises an exception if the file is not found

    Args:
        bucket (boto.s3.bucket.Bucket):
        file_path (str):

    Returns:
        int: file size in bytes
    """
    blob = bucket.get_blob(file_path)
    if not blob:
        raise IOError(
            'file %s does not exist in bucket %s' % (file_path, bucket)
        )

    return blob.size


def get_md5(bucket, path):
    """ Returns the md5 of a key using bucket and path """
    return bucket.get_blob(path).etag[1:-1]


def write_to_storage_stream(uri, data_list):
    bucket, path = _get_bucket_and_path_from_uri(uri)

    if isinstance(bucket, str):
        bucket = get_bucket(bucket)

    with GCSObjectStreamUpload(path, bucket) as f:
        for item in data_list:
            f.write(item)
