import unittest
from jcudc24ingesterapi.schemas.metadata_schemas import MetadataSchema
from jcudc24ingesterapi.schemas.data_types import *

class TestSchemas(unittest.TestCase):
    def test_valid_schemas(self):
        """Test that the schema creation APIs properly validate the schemas
        as they are constructed.
        """
        good_schema = MetadataSchema()
        good_schema.addAttr("attr1", Double())
        good_schema.addAttr("attr2", String())
        
        bad_schema = MetadataSchema()
        self.assertRaises(ValueError, bad_schema.addAttr, "attr2", str)        

    def test_data_types(self):
        pass

if __name__ == '__main__':
    unittest.main()