__author__ = 'Casey Bajema'


def deleter(attr):
    """Deleter closure, used to remove the inner variable"""
    def deleter_real(self):
        return delattr(self, attr)
    return deleter_real

def getter(attr):
    """Getter closure, used to simply return the inner variable"""
    def getter_real(self):
        return getattr(self, attr)
    return getter_real

def setter(attr, valid_types):
    """Setter closure, used to do type checking before storing var"""
    def setter_real(self, var):
        if var != None and \
                not isinstance(var, valid_types): raise TypeError("Not of required type: "+str(valid_types))
        setattr(self,attr,var)
    return setter_real

def typed(attr, valid_types, docs=""):
    """Wrapper around property() so that we can easily apply type checking
    to properties"""
    return property(getter(attr), setter(attr, valid_types), deleter(attr), docs)

