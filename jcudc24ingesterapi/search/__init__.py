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
        