__author__ = 'Casey Bajema'
from jcudc24ingesterapi import typed
from jcudc24ingesterapi.models.data_sources import _DataSource
from jcudc24ingesterapi.models.locations import LocationOffset

class Dataset(object):
    """
    Represents a single dataset and contains the information required to ingest the data as well as location
    metadata.
    """
    __xmlrpc_class__ = "dataset"
    data_source = typed("_data_source", _DataSource, "Data source used for ingesting")
    location_offset = typed("_location_offset", LocationOffset, "Offset from the locations frame of reference")

    def __init__(self, location = None, schema = None, data_source = None, sampling=None, redbox_uri = None):
        self.id = None
        self.location = location
        self.schema = schema                      # subclass of DataType
        self.data_source = data_source
        self.redbox_uri = redbox_uri                  # URL to the ReDBox collection.
        self.sampling = sampling
        self.enabled = False
        self.description = None
        
        
