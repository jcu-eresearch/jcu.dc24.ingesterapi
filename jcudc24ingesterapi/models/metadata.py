from jcudc24ingesterapi.ingester_exceptions import UnknownParameterError
from jcudc24ingesterapi import typed, APIDomainObject

__author__ = 'Casey Bajema'

class MetadataEntry(APIDomainObject):
    """
    Metadata class that provides further information about a single object, this could be a data_entry,
    dataset or any other stored object.

    This class represents any metadata where the schema passed in defines the metadata and the actual data
    is passed in through the kwargs argument.

    The kwargs parameters must conform to the calibration schema or an exception will be thrown on initialisation.
    """
    id = typed("_id", int, "An identifier for the data entry")
    object_id = typed("_object_id", int, "An identifier for the object data entry")
    metadata_schema = typed("_schema", int, "The schema ID")
    data = typed("_data", dict, "Data storage")

    def __init__(self, object_id = None, metadata_schema_id=None, id = None, **kwargs):
        self.id = id
        self.object_id = object_id
        self.metadata_schema = metadata_schema_id

        self.data = {}

        for key, value in kwargs:
            raise UnknownParameterError(key, value)

    def __getitem__(self, item):
        return self.data[item]
    def __setitem__(self, item, value):
        self.data[item] = value
    def __delitem__(self, item):
        del self.data[item]
        
class DatasetMetadataEntry(MetadataEntry):
    __xmlrpc_class__ = "dataset_metadata_entry"

class DataEntryMetadataEntry(MetadataEntry):
    dataset = typed("_dataset", int, "The dataset ID")

    __xmlrpc_class__ = "data_entry_metadata_entry"

    def __init__(self, object_id = None, dataset_id=None, metadata_schema_id=None, id = None, **kwargs):
        self.dataset_id = dataset_id
        super(DataEntryMetadataEntry, self).__init__(object_id, metadata_schema_id, id, **kwargs)
    
