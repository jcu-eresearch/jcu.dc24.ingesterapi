from jcudc24ingesterapi.models.sampling import _Sampling
from jcudc24ingesterapi.schemas.data_types import String, Double, DateTime, DataType
from jcudc24ingesterapi.schemas import Schema

__author__ = 'Casey Bajema'

class DataEntryMetadataSchema(Schema):
    """
    Base class for all calibration schemas that provide a known type.

    All calibration schemas that will be used need to be setup when creating the dataset.

    Each calibration schema has a 1:1 relationship with each data_entry.  This means that there can only
    be 1 QualitySchema calibration for a specific data_entry but there may be a different calibration
    (sub-classed from _CalibrationSchema) added to the same data_entry.  Sending a duplicate calibration
    will overwrite previous values.
    """
    __xmlrpc_class__ = "data_entry_metadata_schema"

class DatasetMetadataSchema(Schema):
    __xmlrpc_class__ = "dataset_metadata_schema"