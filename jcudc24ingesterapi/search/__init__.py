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
    schema = typed("_schema", int, "The schema ID")
    
    def __init__(self, location=None, schema=None):
        self.location = location
        self.schema = schema
        
class LocationSearchCriteria(APIDomainObject):
    """
    Data Entry search criteria object
    """
    __xmlrpc_class__ = "location_search"
    
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


class SearchResults(APIDomainObject):
    """The results of a search method call"""
    __xmlrpc_class__ = "search_results"
    
    count = typed("_count", int, "Total number of results")
    offset = typed("_dataset", int, "Offset from the start of the results")
    limit = typed("_limit", int, "Maximum number of results to return")
    results = typed("_results", list, "Actual result objects")
    
    def __init__(self, results=None, offset=None, limit=None, count=None):
        self.results = results
        self.offset = offset
        self.limit = limit
        self.count = count
        
