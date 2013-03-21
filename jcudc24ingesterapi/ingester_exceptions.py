from jcudc24ingesterapi import ValidationError
__author__ = 'Casey Bajema'

class IngestPlatformError(Exception):
    __xmlrpc_error__ = 1
    pass

class UnknownParameterError(IngestPlatformError):
    """
    Ingester objects that are created with parameters that don't exist in their schema will raise this exception.
    """
    __xmlrpc_error__ = 2

    def __init__(self, parameter_name, parameter_value):
        """
        :param parameter_name: Name of the unknown parameter
        :param parameter_value: Value of the unknown parameter
        """
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value

    def __str__(self):
        return repr(self.parameter_name + " : " + self.parameter_value)


class UnsupportedSchemaError(IngestPlatformError):
    """
    Schema definitions that have invalid fields or cannot be implemented for any other reason will raise this
    exception.
    """
    __xmlrpc_error__ = 3
    def __init__(self, schema):
        self.schema = schema

    def __str__(self):
        return repr(self.schema + " : " + self.schema.items())

class InvalidObjectError(IngestPlatformError):
    """
    Exception that is thrown by the ingester_platform if the passed in object is in an invalid state.
    """
    __xmlrpc_error__ = 4
    def __init__(self, validation_errors):
        if not isinstance(validation_errors, list):
            raise ValueError("Must pass a list of ValidationError objects")
        for e in validation_errors:
            if not isinstance(e, ValidationError):
                raise ValueError("Must pass a list of ValidationError objects")
        self.errors = validation_errors

    def __str__(self):
        return "\n".join([str(e) for e in self.errors])

class UnknownObjectError(IngestPlatformError):
    """
    Exception that is thrown by the ingester_platform if the passed in object of an unknown or incorrect type
    """
    __xmlrpc_error__ = 5
    def __init__(self, ingester_object):
        self.ingester_object = ingester_object

    def __str__(self):
        return repr(self.ingester_object)

class AuthenticationError(IngestPlatformError):
    """
    Exception that is thrown by the ingester_platform user fails to authenticate.
    """
    __xmlrpc_error__ = 6
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

class PersistenceError(IngestPlatformError):
    """
    Thrown when an API method call cannot be completed because it would corrupt the data state (eg. deleting a location
    that  quality data associated with it).
    """
    __xmlrpc_error__ = 7
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

class StaleObjectError(PersistenceError):
    """Thrown when an object the object being persisted has already been updated.
    """
    __xmlrpc_error__ = 8
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class OperationFailedException(IngestPlatformError):
    """
    Thrown when an operation fails to execute as expected.
    """
    __xmlrpc_error__ = 9
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


