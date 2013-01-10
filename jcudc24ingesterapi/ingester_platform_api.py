__author__ = 'Casey Bajema'
import xmlrpclib
import inspect
import datetime

from jcudc24ingesterapi import parse_timestamp, format_timestamp
import jcudc24ingesterapi.models.dataset
import jcudc24ingesterapi.models.locations
import jcudc24ingesterapi.models.sampling
import jcudc24ingesterapi.models.data_sources
import jcudc24ingesterapi.schemas.metadata_schemas
import jcudc24ingesterapi.schemas.data_entry_schemas
import jcudc24ingesterapi.schemas.data_types

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
        self.scanPackage(jcudc24ingesterapi.schemas.metadata_schemas)
        self.scanPackage(jcudc24ingesterapi.schemas.data_entry_schemas)
        self.scanPackage(jcudc24ingesterapi.schemas.data_types)
        
    def scanPackage(self, pkg):
        """Scan through the given package and find classes that are eligable for 
        marshalling."""
        for cls in dir(pkg):
            cls = getattr(pkg, cls)
            if isinstance(cls, type) and hasattr(cls, "__xmlrpc_class__"):
                self._classes[cls] = cls.__xmlrpc_class__
                self._class_factories[cls.__xmlrpc_class__] = cls

    def obj_to_dict(self, obj):
        """Maps an object of base class BaseManagementObject to a dict.
        """
        if not self._classes.has_key(type(obj)):
            raise ValueError("This object class is not supported: " + str(obj.__class__))
        ret = {}
        if isinstance(obj, jcudc24ingesterapi.schemas.Schema):
            ret["attributes"] = []
            for k in obj.attrs:
                attr = obj.attrs[k]
                ret["attributes"].append({"class":attr.__xmlrpc_class__, "name":attr.name, 
                                          "description":attr.description, "units":attr.units})
            if hasattr(obj, "id"):
                ret["id"] = obj.id
        else:
            data_keys = [k for k,v in inspect.getmembers(type(obj)) if isinstance(v, property)]

            for k in data_keys:
                v = getattr(obj, k)
                if type(v) == datetime.datetime:
                    ret[k] = format_timestamp(v)
                elif type(v) not in (str, int, float, unicode, dict, bool, type(None), tuple):
                    ret[k] = self.obj_to_dict(v)
                else:
                    ret[k] = v
        ret["class"] = self._classes[type(obj)]
        return ret

    def dict_to_obj(self, x):
        """Maps a dict back to an object, created based on the 'class' element.
        """
        if isinstance(x, list):
            return [self.dict_to_obj(obj) for obj in x]
        if not x.has_key("class"):
            raise ValueError("There is no class element")
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
                    obj.addAttr(self._class_factories[attr["class"]](attr["name"], 
                                description=attr["description"], units=attr["units"]))
#                    setattr(obj, k2, self._class_factories[x["attributes"][k2]]())
            elif k not in data_keys:
                print "Ignoring ", k
                continue
            else:
                if isinstance(x[k], dict) and dict not in data_keys[k]:
                    setattr(obj, k, self.dict_to_obj(x[k]))
                elif datetime.datetime in data_keys[k]:
                    setattr(obj, k, parse_timestamp(x[k]))
                else:
                    setattr(obj, k, x[k])
        return obj

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
        self.server = xmlrpclib.ServerProxy(service_url, allow_none=True)
        self.auth = auth
        self._marshaller = Marshaller()

    def ping(self):
        """A simple diagnotic method which should return "PONG"
        """
        return self.server.ping()

    def post(self, ingester_object):
        """
        Create a new entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: Insert a new record if the ID isn't set, if the ID is set update the existing record.
        :return: The object passed in with the ID field set.
        """
        if ingester_object.id is None:
            return self.insert(ingester_object)
        else:
            return self.update(ingester_object)

    def insert(self, ingester_object):
        """
        Create a new entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: If the objects ID is set an exception will be thrown.
        :return: The object passed in with the ID field set.
        """
        return self._marshaller.dict_to_obj(self.server.insert(self._marshaller.obj_to_dict(ingester_object)))

    def update(self, ingester_object):
        """
        Update an entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object: If the passed in object doesn't have it's ID set an exception will be thrown.
        :return: The updated object (eg. :return == ingester_object should always be true on success).
        """
        pass

    def delete(self, ingester_object):
        """
        Delete an entry using the passed in object, the entry type will be based on the objects type.

        :param ingester_object:  All fields except the objects ID will be ignored.
        :return: The object that has been deleted, this should have all fields set.
        """
        pass

    def commit(self, unit):
        """Commit a unit of work
        :param unit: Unit of work which is going to be committed
        :return: No return
        """
        
        unit_dto = {"delete":[],
                    "update":[self._marshaller.obj_to_dict(obj) for obj in unit._to_update],
                    "insert":[self._marshaller.obj_to_dict(obj) for obj in unit._to_insert]}
        
        transaction_id = self.server.precommit(unit_dto)
        # do uploads
        results = self.server.commit(transaction_id)
        
        lookup = {}
        for result in results: lookup[result["correlationid"]] = self._marshaller.dict_to_obj(result)
        for obj in unit._to_update:
            if obj.id not in lookup: continue
            obj.__dict__ = lookup[obj.id].__dict__.copy()
        for obj in unit._to_insert:
            if obj.id not in lookup: continue
            obj.__dict__ = lookup[obj.id].__dict__.copy()

    def enableDataset(self, dataset_id):
        """
        """
        return self.server.enableDataset(dataset_id)
    
    def disableDataset(self, dataset_id):
        """
        """
        return self.server.disableDataset(dataset_id)

    def getIngesterLogs(self, dataset_id):
        """
        Get all ingester logs for a single dataset.

        :param dataset_id: ID of the dataset to get ingester logs for
        :return: an array of file handles for all log files for that dataset.
        """
        pass
    
    def getLocation(self, loc_id):
        """
        """
        return self._marshaller.dict_to_obj(self.server.getLocation(loc_id))
    

    def getDataset(self, ds_id):
        """
        """
        return self._marshaller.dict_to_obj(self.server.getDataset(ds_id))
    
    def reset(self):
        """Resets the service
        """
        self.server.reset()

    def findDatasets(self, **kwargs):
        """Search for datasets
        """
        return self._marshaller.dict_to_obj(self.server.findDatasets(kwargs))
        
    def createUnitOfWork(self):
        """Creates a unit of work object that can be used to create transactional consistent set of operations
        """
        return UnitOfWork(self)

    def close(self):
        pass

class UnitOfWork(object):
    """The unit of work encapsulates all the operations in a transaction.
    
    There is no rollback, simply discard the unit of work in this case.
    """
    def __init__(self, service):
        self.service = service
        self._to_insert = []
        self._to_update = []
        self._to_delete = []
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
            raise ValueError("Expected no ID set")
        ingester_object.id = self._next
        self._to_insert.append(ingester_object)
        self._next = self._next - 1
        return ingester_object.id

    def update(self, ingester_object):
        self._to_update.append(ingester_object)

    def delete(self, ingester_object):
        self._to_delete.append(ingester_object)
    
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
