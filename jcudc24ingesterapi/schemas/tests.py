import unittest
from jcudc24ingesterapi.schemas.metadata_schemas import DataEntryMetadataSchema
from jcudc24ingesterapi.schemas.data_types import *

class TestSchemas(unittest.TestCase):
    def test_valid_schemas(self):
        """Test that the schema creation APIs properly validate the schemas
        as they are constructed.
        """
        good_schema = DataEntryMetadataSchema()
        good_schema.addAttr(Double("attr1"))
        good_schema.addAttr(String("attr2"))
        
        bad_schema = DataEntryMetadataSchema()
        self.assertRaises(ValueError, bad_schema.addAttr, str)        

    def test_data_types(self):
        pass

class TestDataTypes(unittest.TestCase):
    def test_names(self):
        self.assertRaises(ValueError, Double, "")
        self.assertRaises(ValueError, Double, "dfkj dfjk")
        self.assertRaises(ValueError, Double, "1dfkjdfjk")
        Double("validname")

if __name__ == '__main__':
    unittest.main()