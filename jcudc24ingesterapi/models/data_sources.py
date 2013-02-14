from jcudc24ingesterapi.models.sampling import _Sampling
from jcudc24ingesterapi import typed, APIDomainObject
from simplesos.client import SOSVersions
from simplesos.varients import _52North, SOSVariants, getSOSVariant


"""
    Defines all possible data sources or in other words data input methods that can be provisioned.
"""

__author__ = 'Casey Bajema'

class _DataSource(APIDomainObject):
    """
    Base data source class that does nothing beyond defining a known type.

    Data sources are known types that provide a known set of information but are unrelated to the data type.
    The ingester platform will need to implement data type specific ingesters for each data source.
    """
    processing_script = typed("_processing_script", str, "Script to run after download")

    def __init__(self, processing_script=None):
        self.processing_script = processing_script

class DatasetDataSource(_DataSource):
    """
    Uses the resulting data_entry from another dataset and processes it further.
    """
    __xmlrpc_class__ = "dataset_data_source"
    dataset_id = typed("_dataset_id", int, "")
    
    def __init__(self, dataset_id=None, processing_script=None):
        self.dataset_id = dataset_id
        self.processing_script = processing_script

class PullDataSource(_DataSource):
    """
    A data source that polls a URI for data of the dataset's data type.
    """
    __xmlrpc_class__ = "pull_data_source"
    url = typed("_url", (str,unicode), "URL of the directory to scan")
    pattern = typed("_pattern", (str,unicode), "Pattern for identifying files, regex")
    recursive = typed("_recursive", bool, "Should the URL be treated as an index page")
    mime_type = typed("_mime_type", (str,unicode), "Mime type of the file")
    field = typed("_field", (str,unicode), "Field name to ingest into")
    sampling = typed("_sampling", _Sampling, "Script to run to determine when to sample")
    def __init__(self, url=None, pattern=None, recursive=False, mime_type=None, field=None, processing_script=None, sampling=None):
        """Initialise the PullDataSource with a URI for the source file, and the field that 
        the uri will be saved to.
        """
        self.url = url
        self.field = field
        self.pattern = pattern
        self.mime_type = mime_type
        self.processing_script = processing_script
        self.sampling = sampling
        self.recursive = recursive

class PushDataSource(_DataSource):
    """
    A data source where the external application will use the ingester platform API to pass data into.
    """
    __xmlrpc_class__ = "push_data_source"
    path = typed("_path", (str,unicode), "Path to monitor for new files")
    def __init__(self, path=None):
        self.path = path


class SOSScraperDataSource(_DataSource):
    __xmlrpc_class__ = "sos_scraper_data_source"
    url = typed("_url", (str,unicode), "URL of the directory to scan")
    field = typed("_field", (str,unicode), "Field name to ingest into")
    sampling = typed("_sampling", _Sampling, "Script to run to determine when to sample")
    variant = typed("_variant", (str,unicode), "The SOS varient.")
    version = typed("_version", (str,unicode), "The SOS API version to use.")
    def __init__(self, url=None, field=None, sampling=None, processing_script=None, version=SOSVersions.v_1_0_0, variant="52North"):
        self.url = url
        self.field = field
        self.sampling = sampling
        self.variant = variant
        self.version = version
        self.processing_script = processing_script

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



