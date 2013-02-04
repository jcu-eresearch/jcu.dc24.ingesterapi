__author__ = 'Casey Bajema'
from jcudc24ingesterapi import typed, APIDomainObject
from jcudc24ingesterapi.models.data_sources import _DataSource
from jcudc24ingesterapi.models.locations import LocationOffset

class Dataset(APIDomainObject):
    """
    Represents a single dataset and contains the information required to ingest the data as well as location
    metadata.
    """
    __xmlrpc_class__ = "dataset"
    id = typed("_id", int)
    location = typed("_location", int, "ID of location for dataset")
    schema = typed("_schema", int, "ID of schema for dataset")
    data_source = typed("_data_source", _DataSource, "Data source used for ingesting")
    location_offset = typed("_location_offset", LocationOffset, "Offset from the locations frame of reference")
    redbox_uri = typed("_redbox_uri", (str,unicode), "Redbox URI")
    enabled = typed("_enabled", bool, "Dataset enabled")
    description = typed("_description", (str,unicode), "Description of dataset")

    def __init__(self, dataset_id = None, location = None, schema = None, data_source = None, sampling=None, redbox_uri = None, location_offset = None):
        self.id = dataset_id
        self.location = location
        self.schema = schema                      # subclass of DataType
        self.data_source = data_source
        self.redbox_uri = redbox_uri                  # URL to the ReDBox collection.
        self.sampling = sampling
        self.enabled = False
        self.description = None
        self.location_offset = location_offset
        
