__author__ = 'Casey Bajema'


class Dataset(object):
    """
    Represents a single dataset and contains the information required to ingest the data as well as location
    metadata.
    """

    def __init__(self, location = None, schema = None, data_source = None, sampling=None, processing_script = None, redbox_link = None):
        self.id = None
        self.location = location
        self.schema = schema                      # subclass of DataType
        self.data_source = data_source                  # subclass of _DataSource
        self.processing_script = processing_script      # handle to a file containing a python script, the script can access the data_entry through self.data_entry
        self.redbox_link = redbox_link                  # URL to the ReDBox collection.
        self.sampling = sampling
        self.enabled = False
        
