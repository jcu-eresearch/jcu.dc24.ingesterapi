from jcudc24ingesterapi.models.sampling import _Sampling
from jcudc24ingesterapi import typed

"""
    Defines all possible data sources or in other words data input methods that can be provisioned.
"""

__author__ = 'Casey Bajema'

class _DataSource(object):
    """
    Base data source class that does nothing beyond defining a known type.

    Data sources are known types that provide a known set of information but are unrelated to the data type.
    The ingester platform will need to implement data type specific ingesters for each data source.
    """
    processing_script = typed("_processing_script", str, "Script to run after download")

class DatasetDataSource(_DataSource):
    """
    Uses the resulting data_entry from another dataset and processes it further.
    """
    def __init__(self, sampling, dataset_id):
        self.dataset_id = dataset_id
        self.sampling = sampling

class PullDataSource(_DataSource):
    """
    A data source that polls a URI for data of the dataset's data type.
    """
    __xmlrpc_class__ = "pull_data_source"
    uri = typed("_uri", str, "URI of the directory to scan")
    pattern = typed("_pattern", str, "Pattern for identifying files, and extracting metadata")
    mime_type = typed("_mime_type", str, "Mime type of the file")
    field = typed("_field", str, "Field name to ingest into")
    sampling_script = typed("_sampling_script", str, "Script to run to determine when to sample")
    def __init__(self, uri=None, pattern=None, mime_type=None, field=None, processing_script=None, sampling_script=None):
        """Initialise the PullDataSource with a URI for the source file, and the field that 
        the uri will be saved to.
        """
        self.uri = uri
        self.field = field
        self.pattern = pattern
        self.mime_type = mime_type
        self.processing_script = processing_script
        self.sampling_script = sampling_script


class PushDataSource(_DataSource):
    """
    A data source where the external application will use the ingester platform API to pass data into.
    """
    pass



class SOSDataSource(_DataSource):
    """
    A data source that provides a Sensor Observation Service accessible over the web.

    SOS standards will be followed such as:
    * No authentication required
    * Invalid data is dropped
    """ # TODO: Work out the exact implementation details

    sensor_id = None   # Need to check the sensor_id type
    sensorml = None 
    pass

class UploadDataSource(_DataSource):
    """
    A data source where the user manually uploads a file using the provisioning system.

    This data source will be very similar to PushDataSource but:
    * Won't require authentication as it is using the standard provisioning system API by passing a data_entry object
    * The provisioning system will setup an upload form.
    """
    pass

class FormDataSource(_DataSource):
    """
    A data source where the user manually enters data into a form within the provisioning interface

    The data entry's will be passed to the ingester platform through the API as data_entry objects.
    """
    pass



