__author__ = 'Casey Bajema'
from jcudc24ingesterapi import typed, APIDomainObject, ValidationError
from jcudc24ingesterapi.models.data_sources import _DataSource
from jcudc24ingesterapi.models.locations import LocationOffset

class Dataset(APIDomainObject):
    """
    Represents a single dataset and contains the information required to ingest the data as well as location
    metadata.
    """
    __xmlrpc_class__ = "dataset"
    id = typed("_id", int)
    version = typed("_version", int)
    location = typed("_location", int, "ID of location for dataset")
    schema = typed("_schema", int, "ID of schema for dataset")
    data_source = typed("_data_source", _DataSource, "Data source used for ingesting")
    location_offset = typed("_location_offset", LocationOffset, "Offset from the locations frame of reference")
    redbox_uri = typed("_redbox_uri", (str,unicode), "Redbox URI")
    enabled = typed("_enabled", bool, "Dataset enabled")
    running = typed("_running", bool, "Dataset running")
    description = typed("_description", (str,unicode), "Description of dataset")
    repository_id = typed("_repository_id", (str))

    def __init__(self, dataset_id=None, location=None, schema=None, data_source=None, redbox_uri=None, location_offset=None, enabled=False):
        self.id = dataset_id
        self.location = location
        self.schema = schema                      # subclass of DataType
        self.data_source = data_source
        self.redbox_uri = redbox_uri                  # URL to the ReDBox collection.
        self.enabled = enabled
        self.description = None
        self.location_offset = location_offset
        
    def validate(self):
        valid = []
        if self.location == None:
            valid.append(ValidationError("location", "Location must be set"))
        if self.schema == None:
            valid.append(ValidationError("schema", "Schema must be set"))
        return valid
