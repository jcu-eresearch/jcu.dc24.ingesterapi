__author__ = 'Casey Bajema'

from jcudc24ingesterapi import typed, APIDomainObject, ValidationError
from jcudc24ingesterapi.schemas.data_types import DataType

class TypedList(list):
    def __init__(self, valid_type):
        self.valid_type = valid_type

    def append(self, item):
        if not isinstance(item, self.valid_type):
            raise TypeError, 'item is not of type %s' % self.valid_type
        super(TypedList, self).append(item)  #append the item to itself (the list)

class SchemaAttrDict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        if key != value.name:
            raise ValueError("The provided key and the fields name do not match")
        # optional processing here
        super(SchemaAttrDict, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("update expected at most 1 arguments, got %d" % len(args))
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        for key in kwargs:
            self[key] = kwargs[key]
        
class Schema(APIDomainObject):
    """
    Base class for all calibration schemas that provide a known type.

    All calibration schemas that will be used need to be setup when creating the dataset.

    Each calibration schema has a 1:1 relationship with each data_entry.  This means that there can only
    be 1 QualitySchema calibration for a specific data_entry but there may be a different calibration
    (sub-classed from _CalibrationSchema) added to the same data_entry.  Sending a duplicate calibration
    will overwrite previous values.
    """
    id = typed("_id", int)
    name = typed("_name", (str, unicode) )

    def __init__(self, name=None):
        self.name = name
        self.__attrs = SchemaAttrDict() 
        self.__extends = TypedList(int)

    def addAttr(self, data_type):
        if not isinstance(data_type, DataType):
            raise ValueError("Not a subclass of DataType")
        self.attrs[data_type.name] = data_type

    @property
    def attrs(self):
        return self.__attrs

    @property
    def extends(self):
        return self.__extends
    
    @extends.setter
    def extends(self, values):
        """Check that the list is valid before replacing it"""
        tmp = TypedList(int)
        for v in values:
            tmp.append(v)
        self.__extends = tmp
        
    def validate(self):
        valid = []
        if self.name == None:
            valid.append(ValidationError("name", "Name must be set"))
        return valid
    
    
    
