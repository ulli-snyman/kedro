# GCP IO Connectors

## CSV_GCS
CSV GCS implemtents the connector from Google cloud storage to the pandas dataframe.

Load modulde uses the google storage python client library to initate a bucket object and load it into a string object that is then consumed by pandas.

Saving a dataset makes use of a custom streaming class that will stream the datframe in bytes to GCS in a specified chunk size, allowing datasets of any size to be uploaded.
<br>
I did find that pandas now nativley supports loading data in nativley using the `gs://` prefix for the path to the data, it might be a good idea to consider this implementation instead.

