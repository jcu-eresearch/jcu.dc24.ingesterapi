from jcudc24ingesterapi.ingester_exceptions import InvalidObjectError,\
    InternalSystemError
__author__ = 'Casey Bajema'
import xmlrpclib
import inspect
import datetime
import httplib
import urllib2
import urlparse
import logging
import base64

from jcudc24ingesterapi import parse_timestamp, format_timestamp, typed,\
    ValidationError, ingester_exceptions
import jcudc24ingesterapi.models.system
import jcudc24ingesterapi.models.dataset
import jcudc24ingesterapi.models.locations
import jcudc24ingesterapi.models.sampling
import jcudc24ingesterapi.models.data_sources
import jcudc24ingesterapi.models.metadata
import jcudc24ingesterapi.schemas.metadata_schemas
import jcudc24ingesterapi.schemas.data_entry_schemas
import jcudc24ingesterapi.schemas.data_types
import jcudc24ingesterapi.search
from jcudc24ingesterapi.models.data_entry import FileObject, DataEntry

logger = logging.getLogger(__name__)

def get_properties(obj):
    """Returns a list of valid property names for this object"""
    return [k for k,v in inspect.getmembers(type(obj)) if isinstance(v, property)]

class Marshaller(object):
    """A Marshaller object is responsible for converting between real objects
    and dicts. This is used as a helper for the XMLRPC service.
    """
    def __init__(self):
        self._classes = {}
        self._class_factories = {}

        self.scanPackage(jcudc24ingesterapi.models.locations)
        self.scanPackage(jcudc24ingesterapi.models.dataset)
        self.scanPackage(jcudc24ingesterapi.models.sampling)
        self.scanPackage(jcudc24ingesterapi.models.data_sources)
        self.scanPackage(jcudc24ingesterapi.models.data_entry)
        self.scanPackage(jcudc24ingesterapi.models.metadata)
        self.scanPackage(jcudc24ingesterapi.models.system)
        self.scanPackage(jcudc24ingesterapi.search)
        self.scanPackage(jcudc24ingesterapi.schemas.metadata_schemas)
        self.scanPackage(jcudc24ingesterapi.schemas.data_entry_schemas)
        self.scanPackage(jcudc24ingesterapi.schemas.data_types)
        self.scanPackage(jcudc24ingesterapi.ingester_platform_api)
        
    def scanPackage(self, pkg):
        """Scan through the given package and find classes that are eligable for 
        marshalling."""
        for cls in dir(pkg):
            cls = getattr(pkg, cls)
            if isinstance(cls, type) and hasattr(cls, "__xmlrpc_class__"):
                self._classes[cls] = cls.__xmlrpc_class__
                self._class_factories[cls.__xmlrpc_class__] = cls

    def class_for(self, klass):
        return self._class_factories[klass]

    def obj_to_dict(self, obj, special_attrs=[]):
        """Maps an object of base class BaseManagementObject to a dict.
        """
        if type(obj) in (str, int, float, unicode, bool, type(None), tuple):
            return obj
        elif type(obj) == list:
            return [self.obj_to_dict(o, special_attrs=special_attrs) for o in obj] 
        elif not self._classes.has_key(type(obj)):
            raise ValueError("This object class is not supported: " + str(obj.__class__))
        ret = {}

        data_keys = get_properties(obj)

        for k in data_keys:
            v = getattr(obj, k)
            if k in ("attrs", "extends") and isinstance(obj, jcudc24ingesterapi.schemas.Schema): continue
            if type(v) == datetime.datetime:
                ret[k] = format_timestamp(v)
            elif isinstance(v, dict):
                ret[k] = {}
                for k1 in v:
                    ret[k][k1] = self.obj_to_dict(v[k1])
            else:
                ret[k] = self.obj_to_dict(v)

        for k in special_attrs:
            ret[k] = getattr(obj, k)

        if isinstance(obj, jcudc24ingesterapi.schemas.Schema):
            ret["attributes"] = []
            for k in obj.attrs:
                attr = obj.attrs[k]
                ret["attributes"].append({"class":attr.__xmlrpc_class__, "name":attr.name, 
                                          "description":attr.description, "units":attr.units})
            ret["extends"] = [] + obj.extends
            
        ret["class"] = self._classes[type(obj)]
        return ret

    def dict_to_obj(self, x, obj=None):
        """Maps a dict back to an object, created based on the 'class' element.
        
        :param x: 
        :param obj: an optional object to use as the destination
        """
        if isinstance(x, list):
            return [self.dict_to_obj(obj) for obj in x]
        elif type(x) in (str, int, float, unicode, bool, type(None)):
            return x
        elif not x.has_key("class"):
            raise ValueError("There is no class element")
        
        if obj == None:
            try:
                obj = self._class_factories[x["class"]]()
            except TypeError, e:
                raise TypeError(e.message + " for " + x["class"], *e.args[1:])

        # Create a dict of the properties, and the valid types allowed
        data_keys = dict([ (k,v.fset.valid_types if v.fset != None and hasattr(v.fset, "valid_types") else []) for k,v in inspect.getmembers(type(obj)) if isinstance(v, property)])

        for k in x:
            if k == "class": 
                continue
            elif k == "attributes" and x["class"].endswith("_schema"):
                for attr in x["attributes"]:
                    obj.addAttr(self.class_for(attr["class"])(attr["name"], 
                                description=attr["description"], units=attr["units"]))
#                    setattr(obj, k2, self._class_factories[x["attributes"][k2]]())
            elif k not in data_keys:
                print "Ignoring ", k
                continue
            else:
                if isinstance(x[k], dict) and dict not in data_keys[k]:
                    setattr(obj, k, self.dict_to_obj(x[k]))
                elif isinstance(obj, DataEntry) and k == "data":
                    for data_key in x[k]:
                        obj[data_key] = self.dict_to_obj(x[k][data_key])
                elif datetime.datetime in data_keys[k]:
                    setattr(obj, k, parse_timestamp(x[k]))
                elif isinstance(x[k], list):
                    setattr(obj, k, [self.dict_to_obj(val_) for val_ in x[k]])
                else:
                    setattr(obj, k, x[k])
        return obj

def translate_exception(e):
    """Translate an exception from an XMLRPC fault into an actual exception"""
    if not isinstance(e, xmlrpclib.Fault):
        return e
    
    if e.faultCode == 99:
        return ValueError(e.faultString)
    
    exception_factory = {}
    
    for cls in dir(ingester_exceptions):
        cls = getattr(ingester_exceptions, cls)
        if isinstance(cls, type) and hasattr(cls, "__xmlrpc_error__"):
            exception_factory[cls.__xmlrpc_error__] = cls

    if e.faultCode in exception_factory:
        return exception_factory[e.faultCode](e.faultString)
    else:
        logger.error("Unsupported exception returned by webservice: "+str(e))
        return Exception(e.faultString)

class IngesterPlatformAPI(object):
    """
    The ingester platform API's are intended to provide a simple way of provisioning ingesters for sensors
    or other research data sources.

    Any call to an API method that doesn't meet expectations will throw an exception, common exceptions include:
        * Missing parameters
        * Parameters of an unknown type
        * Parameter values that don't make sense (eg. inserting an object that has an ID set)
    """
    
    def __init__(self, service_url, auth=None):
        """Initialise the client connection using the given URL
        @param service_url: The server URL. HTTP and HTTPS only.
        
        >>> s = IngesterPlatformAPI("")
        Traceback (most recent call last):
        ...
        ValueError: Invalid server URL specified
        >>> s = IngesterPlatformAPI("ssh://")
        Traceback (most recent call last):
        ...
        ValueError: Invalid server URL specified
        >>> c = IngesterPlatformAPI("http://localhost:8080")
        """
        if not service_url.startswith("http://") and not service_url.startswith("https://"):
            raise ValueError("Invalid server URL specified")
        url_obj = urlparse.urlparse(service_url)
        self.service_url = service_url
        if auth == None:
            connection_url = "%s://%s%s"%(url_obj[0], url_obj[1], url_obj[2])
        else:
            connection_url = "%s://%s:%s@%s%s"%(url_obj[0], auth.username, auth.password, url_obj[1], url_obj[2])
        self.server = xmlrpclib.ServerProxy(connection_url, allow_none=True)
        self.auth = auth
        self._marshaller = Marshaller()

    def ping(self):
        """A simple diagnotic method which should return "PONG"
        """
        return self.server.ping()

    def post(self, ingester_object):
        """
        Create a new entry or update an existing entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: Insert a new record if the ID isn't set, if the ID is set update the existing record.
        :return: The object passed in with the ID field set.
        """
        try:
            if ingester_object.id is None:
                return self.insert(ingester_object)
            else:
                return self.update(ingester_object)
        except Exception, e:
            raise translate_exception(e)
        
    def insert(self, ingester_object):
        """
        Create a new entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: If the objects ID is set an exception will be thrown.
        :return: The object passed in with the ID field set.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.insert(self._marshaller.obj_to_dict(ingester_object)))
        except Exception, e:
            raise translate_exception(e)

    def update(self, ingester_object):
        """
        Update an entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: If the passed in object doesn't have it's ID set an exception will be thrown.
        :return: The updated object (eg. :return == ingester_object should always be true on success).
        """
        try:
            return self._marshaller.dict_to_obj(self.server.update(self._marshaller.obj_to_dict(ingester_object)))
        except Exception, e:
            raise translate_exception(e)

    def delete(self, ingester_object):
        """
        Delete an entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object:  All fields except the objects ID will be ignored.
        :return: The object that has been deleted, this should have all fields set.
        """
        pass
    
    def search(self, criteria, offset, limit):
        try:
            return self._marshaller.dict_to_obj(self.server.search(self._marshaller.obj_to_dict(criteria), offset, limit))
        except Exception, e:
            logger.exception(e)
            raise translate_exception(e)

    def commit(self, unit):
        """Commit a unit of work
        :param unit: Unit of work which is going to be committed
        :return: No return
        """
        transaction_id = None
        try:
            to_upload = []
            unit_dto = self._marshaller.obj_to_dict(unit)
            
            for obj in unit._to_update:
                if not hasattr(obj, "data"): continue
                for k in obj.data:
                    val = obj.data[k]
                    if not isinstance(val, FileObject): continue
                    to_upload.append( ( "%s:%d"%(obj.__xmlrpc_class__,obj.id), k, val) )
    
            for obj in unit._to_insert:
                if not hasattr(obj, "data"): continue
                for k in obj.data:
                    val = obj.data[k]
                    if not isinstance(val, FileObject): continue
                    to_upload.append( ( "%s:%d"%(obj.__xmlrpc_class__,obj.id), k, val) )
            
            transaction_id = self.server.precommit(unit_dto)
            # do uploads
            
            (proto, host, path, params, query, frag) = urlparse.urlparse(self.service_url)
            c = httplib.HTTPConnection(host)
            headers = {"Content-Type":"application/octet-stream"}
            if self.auth != None:
                headers["Authorization"] = "Basic %s"%(base64.b64encode("%s:%s"%(self.auth.username, self.auth.password)))
            for oid, attr, file_obj in to_upload:
                if file_obj.f_handle != None:
                    f_handle = file_obj.f_handle
                else:
                    f_handle = open(file_obj.f_path, "rb")
                c.request('POST', "%s/%s/%s/%s"%(path, transaction_id, oid, attr), f_handle, 
                          headers)
                r = c.getresponse()
                r.close()
                if r.status != 200:
                    raise Exception("Error uploading data files")
                f_handle.close()
            c.close()
            
            results = self.server.commit(transaction_id)
            
            lookup = {}
            for result in results: lookup[result["correlationid"]] = result
            for obj in unit._to_update:
                if obj.id not in lookup: continue
                self._marshaller.dict_to_obj(lookup[obj.id], obj)
            for obj in unit._to_insert:
                if obj.id not in lookup: continue
                self._marshaller.dict_to_obj(lookup[obj.id], obj)
        except Exception, e:
            logger.exception("Exception while committing " + `transaction_id`)
            raise translate_exception(e)

    def enableDataset(self, dataset_id):
        """Enable data ingestion for this dataset.
        """
        try:
            return self.server.enableDataset(dataset_id)
        except Exception, e:
            raise translate_exception(e)
    
    def disableDataset(self, dataset_id):
        """Disable data ingestion for this dataset.
        """
        try:
            return self.server.disableDataset(dataset_id)
        except Exception, e:
            raise translate_exception(e)

    def getIngesterLogs(self, dataset_id):
        """
        Get all ingester logs for a single dataset.

        :param dataset_id: ID of the dataset to get ingester logs for
        :return: an array of file handles for all log files for that dataset.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getIngesterLogs(dataset_id))
        except Exception, e:
            raise translate_exception(e)


    def getRegion(self, reg_id):
        """Get the region object specified by the given id.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getRegion(reg_id))
        except Exception, e:
            raise translate_exception(e)
    
    def getLocation(self, loc_id):
        """Get the location object specified by the given id.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getLocation(loc_id))
        except Exception, e:
            raise translate_exception(e)
    
    def getSchema(self, s_id):
        """Get the schema object specified by the given id.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getSchema(s_id))
        except Exception, e:
            raise translate_exception(e)
    
    def getDataset(self, ds_id):
        """Get the dataset object specified by the given id.
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getDataset(ds_id))
        except Exception, e:
            raise translate_exception(e)

    def getDataEntry(self, ds_id, de_id):
        """A data entry is uniquely identified by dataset id + data entry id.
        
        """
        try:
            return self._marshaller.dict_to_obj(self.server.getDataEntry(ds_id, de_id))
        except Exception, e:
            raise translate_exception(e)
        
    def getDataEntryStream(self, ds_id, de_id, attr):
        """A data entry stream is uniquely identified by dataset id + data entry id + attribute name.
        
        """
        try:
            req = urllib2.Request("%s/data_entry/%s/%s/%s"%(self.service_url, ds_id, de_id, attr))
            if self.auth != None:
                req.add_header("Authorization" ,"Basic %s"%base64.b64encode("%s:%s"%(self.auth.username, self.auth.password)))
            resp = urllib2.urlopen(req)
            if resp.getcode == 404:
                resp.close()
                return None
            elif resp.code == 200:
                return resp
            else:
                resp.close()
                raise InternalSystemError("Error getting stream, got code " + resp.getcode)

        except Exception, e:
            raise translate_exception(e)
    
    def reset(self):
        """Resets the service
        """
        self.server.reset()

    def findDatasets(self, **kwargs):
        """Search for datasets
        """
        try:
            return self._marshaller.dict_to_obj(self.server.findDatasets(kwargs))
        except Exception, e:
            raise translate_exception(e)
        
    def createUnitOfWork(self):
        """Creates a unit of work object that can be used to create transactional consistent set of operations
        """
        return UnitOfWork(self)

    def close(self):
        """
        Close should always be called when finished with the IngesterPlatformAPI to allow it to clean up any related data.
        :return:
        """
        pass

class UnitOfWork(object):
    """The unit of work encapsulates all the operations in a transaction.
    
    There is no rollback, simply discard the unit of work in this case.
    """
    __xmlrpc_class__ = "unit_of_work"
    to_insert = typed("_to_insert", list)
    to_update = typed("_to_update", list)
    to_delete = typed("_to_delete", list)
    to_enable = typed("_to_enable", list)
    to_disable = typed("_to_disable", list)
    
    def __init__(self, service=None):
        self.service = service
        self._to_insert = []
        self._to_update = []
        self._to_delete = []
        self._to_enable = []
        self._to_disable = []
        self._next = -1
        
    def post(self, ingester_object):
        """ If the object has an ID this object is updated, else it is inserted.
        
        @return: the ID for this object to be used on other objects
        """
        if ingester_object.id == None:
            return self.insert(ingester_object)
        else:
            self.update(ingester_object)
            return ingester_object.id

    def insert(self, ingester_object):
        """Records the object for ingestion.
        
        @return: the ID to use on other objects.
        """
        if ingester_object.id != None:
            raise InvalidObjectError([ValidationError("id","Expected no ID set")])
        validation = ingester_object.validate()
        if len(validation) > 0:
            raise InvalidObjectError(validation)
        
        ingester_object.id = self._next
        self._to_insert.append(ingester_object)
        self._next = self._next - 1
        return ingester_object.id

    def update(self, ingester_object):
        validation = ingester_object.validate()
        if len(validation) > 0:
            raise InvalidObjectError(validation)
        
        self._to_update.append(ingester_object)

    def delete(self, ingester_object):
        self._to_delete.append(ingester_object)

    def enable(self, ingester_object_id):
        self._to_enable.append(ingester_object_id)
        
    def disable(self, ingester_object_id):
        self._to_disable.append(ingester_object_id)

    def commit(self):
        """Commit this unit of work using the original service instance.
        """
        self.service.commit(self)
        
    def findId(self, collection, obj_id):
        """Looks for an id in a collection of objects.
        
        This is an internal method used for checking if other operations are already registerd for this object.
        >>> from jcudc24ingesterapi.models.dataset import Dataset
        >>> col = []
        >>> ds = Dataset()
        >>> ds.id = 1
        >>> col.append(ds)
        >>> UnitOfWork(None).findId(col, 1)
        True
        >>> UnitOfWork(None).findId(col, 2)
        False
        """
        for obj in collection:
            if obj.id == obj_id: return True
        return False

def push_data(self, authentication, data_entry, dataset_id):
    """
        For datasets that use a PushDataSource, data can be entered using this method.

        If the data_entry_id is set the upd

        :param data_entry: The actual data to save, an InvalidObjectError will be raised if the data_entry_id is set.
        :param key:
        :param username:
        :param password:
        :return: The data_entry object with the data_entry_id set on success, otherwise raise an AuthenticationError
    """
    pass
