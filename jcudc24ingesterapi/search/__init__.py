from jcudc24ingesterapi import APIDomainObject, typed
import datetime


class DataEntrySearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "data_entry_search"
    
    dataset = typed("_dataset", int, "The dataset ID")
    start_time = typed("_start_time", datetime.datetime, "The first valid time")
    end_time = typed("_end_time", datetime.datetime, "The last valid time") 

    def __init__(self, dataset=None, start_time=None, end_time=None):
        self.dataset = dataset
        self.start_time = start_time
        self.end_time = end_time
        
class DatasetMetadataSearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "dataset_metadata_search"
    
    dataset = typed("_dataset", int, "The dataset ID")

    def __init__(self, dataset=None):
        self.dataset = dataset
                
class DataEntryMetadataSearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "data_entry_metadata_search"
    
    dataset = typed("_dataset", int, "The dataset ID")
    data_entry = typed("_data_entry", int, "The data_entry ID")

    def __init__(self, dataset=None, data_entry=None):
        self.dataset = dataset
        self.data_entry = data_entry
        
class DatasetSearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "dataset_search"
    
    location = typed("_location", int, "The location ID")
    
    def __init__(self, location=None):
        self.location = location
        
class LocationSearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "dataset_search"
    
    location = typed("_location", int, "The location ID")
    
    def __init__(self, location=None):
        self.location = location
         
class DataEntrySchemaSearchCriteria(APIDomainObject):
    """
    """
    __xmlrpc_class__ = "data_entry_schema_search"
    
    
    def __init__(self):
        pass

         
class DataEntryMetadataSchemaSearchCriteria(APIDomainObject):
    """
    """
    __xmlrpc_class__ = "data_entry_metadata_schema_search"
    
    
    def __init__(self):
        pass

         
class DatasetMetadataSchemaSearchCriteria(APIDomainObject):
    """
    """
    __xmlrpc_class__ = "dataset_metadata_schema_search"
    
    
    def __init__(self):
        pass
