from jcudc24ingesterapi.ingester_exceptions import UnknownParameterError
from jcudc24ingesterapi import typed
from jcudc24ingesterapi.models.locations import LocationOffset

__author__ = 'Casey Bajema'

class DataEntry(object):
    """
    Base class for individual data points of a dataset.

    DataEntry objects will be used for each data point where the actual data is passed in through the
    kwargs argument.

    The kwargs parameters must conform to the data_type schema in the dataset or an exception will be thrown on initialisation.
    """
    location_offset = typed("_location_offset", LocationOffset, "Offset from the locations frame of reference")

    def __init__(self, dataset=None, timestamp=None, data_entry_id = None, **kwargs):
        self.data_entry_id = data_entry_id
        self.dataset = dataset
        self.timestamp = timestamp
        self.data = {}

        # Push the kwargs to fields
#        for key in data_type_schema.keys():
#            self[key] =  kwargs.pop(key, None)

        for key, value in kwargs:
            raise UnknownParameterError(key, value)

    def __getitem__(self, item):
        return self.data[item]
    def __setitem__(self, item, value):
        self.data[item] = value
    def __delitem__(self, item):
        del self.data[item]


