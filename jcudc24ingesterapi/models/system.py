"""This modules contains system level objects, such as logging and monitoring"""
import datetime

from jcudc24ingesterapi import typed, APIDomainObject, ValidationError

class IngesterLog(APIDomainObject):
    """
    Represents a single dataset and contains the information required to ingest the data as well as location
    metadata.
    """
    __xmlrpc_class__ = "ingester_log"
    id = typed("_id", int)
    dataset_id = typed("_dataset", int, "The dataset ID")
    timestamp = typed("_timestamp", datetime.datetime, "The timestamp for the entry") 
    level = typed("_level", str, "Log level")
    message = typed("_message", str, "Log message")

