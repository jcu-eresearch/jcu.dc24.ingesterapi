import datetime
import jcudc24ingesterapi
import os
import os.path
import unittest
import sys
import tempfile

from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.models.locations import Location, Region, LocationOffset
from jcudc24ingesterapi.schemas.data_types import FileDataType, Double, String
from jcudc24ingesterapi.models.data_sources import PullDataSource, PushDataSource
from jcudc24ingesterapi.models.data_entry import DataEntry, FileObject
from jcudc24ingesterapi.ingester_platform_api import IngesterPlatformAPI, Marshaller
from jcudc24ingesterapi.authentication import CredentialsAuthentication
from jcudc24ingesterapi.models.metadata import DatasetMetadataEntry
from jcudc24ingesterapi.schemas.metadata_schemas import DataEntryMetadataSchema, DatasetMetadataSchema
from jcudc24ingesterapi.models.sampling import RepeatSampling, PeriodicSampling, CustomSampling
from jcudc24ingesterapi.ingester_exceptions import UnsupportedSchemaError, InvalidObjectError, UnknownObjectError, AuthenticationError
from jcudc24ingesterapi.schemas.data_entry_schemas import DataEntrySchema

class ProvisioningInterfaceTest(unittest.TestCase):
    """
    This test defines and checks that the Ingester API works the way the provisioning interface expects.
    """
    def setUp(self):
        self.auth = CredentialsAuthentication("casey", "password")
        self.ingester_platform = IngesterPlatformAPI("http://localhost:8080/api", self.auth)
        self.cleanup_files = []

    def test_api_usage(self):
#       User data that is created by filling out the provisioning interface workflow steps.
        #   General
        title = "Test project"
        data_manager = "A Person"
        project_lead = "Another Person"

        #   Metadata
        project_region = Region("Test Region", ((1, 1), (2, 2),(2,1), (1,1)))

        #   Methods & Datasets
        loc1 = Location(11.0, 11.0, "Test Site", 100)
        loc2 = Location(11.0, 11.0, "Test Site", 100)
        loc3 = Location(12.0, 11.0, "Test Site", 100)

        temperature_schema = DataEntrySchema()
        temperature_schema.addAttr(Double("temperature"))   
        temperature_schema = self.ingester_platform.post(temperature_schema)
        
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema = self.ingester_platform.post(file_schema)

        dataset1 = Dataset(None, temperature_schema.id)
        dataset2 = Dataset(None, file_schema.id, PullDataSource("http://test.com", "file_handle", processing_script="file://d:/processing_scripts/awsome_processing.py"), None)

#        dataset3 = Dataset(None, file_schema, PullDataSource("http://test.com", "file_handle"), CustomSampling("file://d:/sampling_scripts/awsome_sampling.py"), "file://d:/processing_scripts/awsome_processing.py")

        self.cleanup_files.append(dataset2.data_source.processing_script)
#        self.cleanup_files.push(dataset3.sampling.script)
#        self.cleanup_files.push(dataset3.processing_script)

#       Provisioning admin accepts the submitted project
        work = self.ingester_platform.createUnitOfWork()

        work.post(project_region)    # Save the region

        loc1.region = project_region.id                  # Set the datasets location to use the projects region
        work.post(loc1)                        # Save the location
        dataset1.location = loc1.id                            # Set the datasets location
        work.post(dataset1)                # Save the dataset

        loc2.region = project_region.id
        work.post(loc2)
        dataset2.location = loc2.id
        work.post(dataset2)

        work.commit()

        # Region, location and dataset id's will be saved to the project within the provisioning system in some way


#       User searches for datasets

        # TODO: Nigel? - Define searching api
        found_dataset_id = dataset1.id                  # The dataset that has an extended file schema

#       User manually enters data
        data_entry_1 = DataEntry(found_dataset_id, datetime.datetime.now())
        data_entry_1['temperature'] = 27.8                # Add the extended schema items
        data_entry_1 = self.ingester_platform.post(data_entry_1)
        self.assertIsNotNone(data_entry_1.id)

        work = self.ingester_platform.createUnitOfWork()
        data_entry_2 = DataEntry(dataset2.id, datetime.datetime.now())
        data_entry_2['file'] = FileObject(open(os.path.join(
                    os.path.dirname(jcudc24ingesterapi.__file__), "tests/test_ingest.xml")), "text/xml")
        work.post(data_entry_2)
        work.commit()
        self.assertIsNotNone(data_entry_2.id)

#       User enters quality assurance metadata
        quality_metadata_schema = DatasetMetadataSchema()
        quality_metadata_schema.addAttr(String("unit"))
        quality_metadata_schema.addAttr(String("description"))
        quality_metadata_schema.addAttr(Double("value"))
        quality_metadata_schema = self.ingester_platform.post(quality_metadata_schema)
        
        entered_metadata = DatasetMetadataEntry(data_entry_1.dataset, quality_metadata_schema.id)
        entered_metadata['unit'] = "%"
        entered_metadata['description'] = "Percent error"
        entered_metadata['value'] = 0.98

        entered_metadata = self.ingester_platform.post(entered_metadata)

#       User changes sampling rate
# FIXME: This test is going to be changed to be done by editing the dataset
#        sampling_rate_changed = Metadata(dataset1.id, type(dataset1), SampleRateMetadataSchema())
#        sampling_rate_changed.change_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
#        sampling_rate_changed.sampling = CustomSampling("file://d:/sampling_scripts/awsome_sampling.py")
#
#        try:
#            sampling_rate_changed = self.ingester_platform.post(sampling_rate_changed)
#            assert(sampling_rate_changed.metadata_id is None, "Sampling rate change failed")
#        except:
#            assert(True, "Sampling rate change failed")

#       User wants some random metadata specific to their project
# FIXME: Not sure what use case this is trying to demonstrate
#        random_metadata_schema =  DataEntryMetadataSchema()
#        random_metadata_schema.addAttr('random_field', Double())

#        random_metadata = Metadata(data_entry.data_entry_id, type(data_entry), random_metadata_schema)
#        random_metadata.random_field = 1.5

#        try:
#            random_metadata = self.ingester_platform.post(random_metadata)
#            assert(random_metadata.metadata_id is None, "random_metadata failed")
#        except:
#            assert(True, "random_metadata failed")

#       User changes the data source of the dataset
        new_data_source = PullDataSource("http://test.com/new_data", "file_handle")
        dataset1.data_source = new_data_source
        dataset1 = self.ingester_platform.post(dataset1)
        self.assertNotEqual(None, dataset1)

#       External, 3rd party searches for data
        # TODO: external 3rd parties should be able to use the api to get data without authentication
        # TODO: I'm not sure exactly how this should work, but the search api could be open access (need spam limitations or something?)

#       Project is disabled/finished
        work = self.ingester_platform.createUnitOfWork()
        work.disable(dataset1.id)
        work.disable(dataset2.id)
        work.commit()

#       Project is obsolete and data should be deleted
        work = self.ingester_platform.createUnitOfWork()
        work.delete(dataset1.id)
        work.delete(dataset2.id)
        work.commit()

    def testMultiDatasetExtraction(self):
        """This test demonstrates use case #402.
        There are 2 datasets created, the first holds a datafile, and has a pull ingest occurring, along with 
        a configured custom script. The second dataset holds observation data, that will be extracted from the
        datafile in the first dataset.
        """
        temperature_schema = DataEntrySchema()
        temperature_schema.addAttr(Double("Temperature"))   
        temperature_schema = self.ingester_platform.post(temperature_schema)
        
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema = self.ingester_platform.post(file_schema)

        location = self.ingester_platform.post(Location(10.0, 11.0, "Test Site", 100))
        temp_dataset = Dataset(None, temperature_schema.id)

        file_dataset = Dataset(None, file_schema.id, PullDataSource("http://test.com", "file_handle"), None, "file://d:/processing_scripts/awsome_processing.py")




    def tearDown(self):
        self.ingester_platform.reset()

        for f in self.cleanup_files:
            try:
                os.remove(f)
            except:
                print "failed to remove file: " + f




class TestIngesterModels(unittest.TestCase):
    def test_authentication(self):
        pass

    def test_ingester_exceptions(self):
        pass

    def test_listeners(self):
        # Use a list ot beat the closure
        called = [False] 
        
        def loc_listener(obj, var, value):
            called.remove(False)
            called.append(True)
            
            self.assertEquals("_id", var)
            self.assertEquals(1, value)
        
        loc = Location()
        loc.set_listener(loc_listener)
        loc.id = 1
        self.assertTrue(called[0])
        

#    def test_ingester_platform(self):
#        self.ingester_platform = IngesterPlatformAPI()
#        dataset = self.ingester_platform.post(self.auth, self.dataset)
#        self.assertTrue(dataset is set, "Failed to add dataset to ingester API")
#        self.assertTrue(dataset.dataset_id is set and dataset.dataset_id >= 0,
#            "Ingester API returned dataset with invalid dataset_id")
#        self.dataset = dataset
#
#        data_entry = self.ingester_platform.post(self.auth, self.data_entry)
#        self.assertTrue(data_entry is set, "Failed to add data_entry to ingester API")
#        self.assertTrue(data_entry.data_entry_id is set and data_entry.data_entry_id >= 0,
#            "Ingester API returned data_entry with invalid data_entry_id")
#        self.data_entry = data_entry
#
#        datasets = self.ingester_platform.get(self.auth, Dataset())    # Get all datasets
#        self.assertTrue(len(datasets) > 0, "Ingester API failed to insert dataset")
#        self.assertIn(self.dataset, datasets, "Ingester API didn't return the inserted dataset")
#
#        data_quality = Metadata(self.data_entry, QualityMetadataSchema(), {"description": "Sensor was moved", "value": 0})
#        stored_data_quality = self.ingester_platform.post(self.auth, data_quality)
#        self.assertEquals(data_quality, stored_data_quality)
#
#        sampling_rate = Metadata(self.dataset, SampleRateMetadataSchema(), {"sampling": PeriodicSampling()})
#
#        location_elevation_type = Metadata(self.location, NoteMetadataSchema(),
#                {"name": "Elevation type", "text": "Height above ground"})


    def test_ingester_scripts(self):
        pass


class TestIngesterPersistence(unittest.TestCase):
    """This set of tests checks that the CRUD functionality works as expected
    """
    def setUp(self):
        self.auth = CredentialsAuthentication("casey", "password")
        self.ingester_platform = IngesterPlatformAPI("http://localhost:8080/api", self.auth)
        self.cleanup_files = []
        
    def test_region_persistence(self):
        project_region = Region("Test Region", ((1, 1), (2, 2),(2,1), (1,1)))
        project_region1 = self.ingester_platform.post(project_region)
        self.assertNotEqual(project_region1.id, None, "ID should have been set")

    def test_location_persistence(self):
        loc = Location(10.0, 11.0, "Test Site", 100, None)
        loc1 = self.ingester_platform.post(loc)
        self.assertNotEqual(loc1.id, None, "ID should have been set")
        self.assertEqual(loc.latitude, loc1.latitude, "latitude does not match")
        self.assertEqual(loc.longitude, loc1.longitude, "longitude does not match")
        self.assertEqual(loc.elevation, loc1.elevation, "elevation does not match")
        self.assertEqual(loc.name, loc1.name, "name does not match")
        
        locs = self.ingester_platform.search("location")
        self.assertEquals(1, len(locs))
        
        # Now update the location
        loc1.name = "The Test Site"
        loc1.latitude = -19.0
        loc2 = self.ingester_platform.post(loc1)
        self.assertEqual(loc1.id, loc2.id, "")
        self.assertEqual(loc1.latitude, loc2.latitude, "latitude does not match")
        self.assertEqual(loc1.longitude, loc2.longitude, "longitude does not match")
        self.assertEqual(loc1.elevation, loc2.elevation, "elevation does not match")
        self.assertEqual(loc1.name, loc2.name, "name does not match")        

    def test_dataset_persistence(self):
        loc = Location(10.0, 11.0, "Test Site", 100, None)
        loc = self.ingester_platform.post(loc)
        self.assertIsNotNone(loc, "Location should not be none")
        self.assertIsNotNone(loc.id, "Location should not be none")

        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema = self.ingester_platform.post(file_schema)

        script_contents = """Some Script
More"""
        
        dataset = Dataset(loc.id, file_schema.id, PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file", processing_script=script_contents), location_offset=LocationOffset(0, 1, 2))
        dataset1 = self.ingester_platform.post(dataset)
        self.assertIsNotNone(dataset1, "Dataset should not be none")
        self.assertEquals(dataset1.location, dataset.location, "Location ID does not match")
        self.assertEquals(dataset1.schema, dataset.schema, "schema does not match %d!=%d"%(dataset1.schema, dataset.schema))
        self.assertEquals(dataset1.location_offset.x, 0)
        self.assertEquals(dataset1.location_offset.y, 1)
        self.assertEquals(dataset1.location_offset.z, 2)

        self.assertEquals(dataset1.data_source.processing_script, script_contents)

        datasets = self.ingester_platform.findDatasets()
        self.assertEquals(1, len(datasets))

        datasets = self.ingester_platform.findDatasets(location=loc.id)
        self.assertEquals(1, len(datasets))
        
        data_entry_schemas = self.ingester_platform.search("data_entry_schema")
        self.assertEquals(1, len(data_entry_schemas))

        datasets = self.ingester_platform.search("dataset")
        self.assertEquals(1, len(datasets))
        
    def test_unit_of_work_persistence(self):
        unit = self.ingester_platform.createUnitOfWork()
        
        loc = Location(10.0, 11.0, "Test Site", 100, None)
        unit.insert(loc)
 
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema_id = unit.insert(file_schema)

        self.assertIsNotNone(file_schema_id, "Schema ID should not be null")

        dataset = Dataset(loc.id, file_schema.id, PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file"))
        unit.insert(dataset)
        
        # Persist all the objects
        unit.commit()

        self.assertIsNotNone(loc, "Location should not be none")
        self.assertIsNotNone(loc.id, "Location should not be none")
        self.assertGreater(loc.id, 0, "Location ID not real")
        self.assertEqual(loc.name, "Test Site", "Location name doesn't match")
        
        self.assertIsNotNone(dataset, "dataset should not be none")
        self.assertIsNotNone(dataset.id, "dataset should not be none")
        self.assertGreater(dataset.id, 0, "dataset ID not real")

    def tearDown(self):
        self.ingester_platform.reset()
        
class TestIngesterFunctionality(unittest.TestCase):
    """This set of tests test the actual functioning of the service.
    """
    def setUp(self):
        self.auth = CredentialsAuthentication("casey", "password")
        self.ingester_platform = IngesterPlatformAPI("http://localhost:8080/api", self.auth)
        self.cleanup_files = []
        
    def test_pull_ingest_functionality(self):
        loc = Location(10.0, 11.0, "Test Site", 100, None)
        loc = self.ingester_platform.post(loc)
        
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema = self.ingester_platform.post(file_schema)
        
        dataset = Dataset(loc.id, file_schema.id, PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file"),
                PeriodicSampling(10000))
        dataset1 = self.ingester_platform.post(dataset)
        self.assertEquals(dataset1.location, dataset.location, "Location ID does not match")
        self.assertEquals(dataset1.schema, dataset.schema, "schema does not match")
        
        self.ingester_platform.disableDataset(dataset1.id)

        dataset1a = self.ingester_platform.getDataset(dataset1.id)
        self.assertEquals(dataset1a.enabled, False)

    def tearDown(self):
        self.ingester_platform.reset()

#class TestProjectFunctionality(unittest.TestCase):
#    #---------------Create example data---------------
#    def setUp(self):
#        self.auth = CredentialsAuthentication("casey", "password")
#        self.ingester_platform = IngesterPlatformAPI("http://localhost:8080", self.auth)
#        self.cleanup_files = []
#
#    def test_connection(self):
#        result = self.ingester_platform.ping()
#        self.assertEquals(result, "PONG", "Could not ping service")
#
#    def test_ingest_functionality(self):
#        self.region = Region(
#            "Queensland",
#                [(2314, 1234), (1234, 1234), (1234, 1234), (1234, 2134)]
#        )
#
#        self.location = Location(1234, 1234, "Example Point", 1.5, self.region)
#
#        self.data_type = FileDataType()
#        self.data_type.extra_data_example = BOOLEAN()
#
#        self.script_handle = os.path.join(tempfile.gettempdir(), 'test_script_file.py')
#        self.cleanup_files.append(self.script_handle)
#        
#        script = "class TestProcessingScript(_ProcessingScript):"\
#                 "  def process_it(self, data_entry):"\
#                 "      assertIsInstance(data_entry, FileDataType)"\
#                 "      return {data_entry, DataEntry(data_entry.dataset, data_entry.data_type_schema, data_entry.datetime, {'processed':True})}"\
#                 ""
#        script_file = open(self.script_handle, 'w')
#        script_file.write(script)
#
#        self.dataset = Dataset(self.location, self.data_type, PushDataSource(sampling_script), self.script_handle)
#
#        self.data_entry = DataEntry(self.dataset, FileDataType(), 123456789,
#                {"mime_type": "text/xml", "file_handle": "c:/test.xml", "extra_data_example": False})
#
#        self.data_quality = Metadata(self.data_entry, QualityMetadataSchema(),
#                {"description": "The entered data was invalid."})
#        self.dataset_sampling_changed = Metadata(self.dataset, SampleRateMetadataSchema(),
#                {"change_time": datetime.today(), "sampling": PeriodicSampling(1000)})
#    # Datasets
#        try:
#            # Add a dataset
#            self.dataset = self.ingester_platform.post(self.dataset)
#
#            # Update the dataset with a ReDBox link after the metadata is entered.
#            self.dataset.redbox_link = "https://eresearch.jcu.edu.au/researchdata/123456789" # After entry into ReDBox
#            self.dataset = self.ingester_platform.post(self.dataset)
#
#            # Change the sampling rate of the dataset
#            self.dataset = self.ingester_platform.post(self.dataset_sampling_changed)
#
#            # Add a manual data entry
#            self.data_entry = self.ingester_platform.post(self.data_entry)
#
#            # Provide quality information on the data_entry
#            self.data_entry = self.ingester_platform.post(self.data_quality)
#
#            # Example of searching a range
#            start_search_id_range = DataEntry(data_entry_id=0)
#            start_search_id_range = DataEntry(data_entry_id=self.data_entry.data_entry_id)
#            found_data_entries = self.ingester_platform.get(start_search_id_range, start_search_id_range)
#            found = False
#            for data_entry in found_data_entries:
#                if data_entry == self.data_entry:
#                    # Delete the data_entry
#                    found = True
#            self.assertTrue(found, "id range search failed")
#
#            a_data_entry = DataEntry(kwargs={"extra_data_example": False})
#            data_entry = self.ingester_platform.get(DataEntry)
#            self.ingester_platform.delete(self.auth, self.data_entry)
#
#            # Get all datasets at the location
#            location_search = Dataset(self.location, None, None)
#            found_datasets = self.ingester_platform.get(location_search)
#            found = False
#            for dataset in found_datasets:
#                if dataset == self.dataset:
#                    # Delete the dataset
#                    self.ingester_platform.delete(self.auth, self.dataset)
#                    found = True
#            self.assertTrue(found, "location search failed")
#
#        except UnsupportedSchemaError:
#            assert True, "The data_type schema was an unknown type (this should never happen except under development"
#
#        except InvalidObjectError:
#            assert True, "Will occur if the model is invalid due to a not set required field or fields that are set "\
#                         "incorrectly (eg. data_schema is missing, location is an integer instead of a location object)"
#        except UnknownObjectError:
#            assert True, "Will occur if a non-ingester model object is posted. (eg. data_entry, dataset, location or metadata "\
#                         "are valid - sampling, data_sources or any other object are not)."
#        except AuthenticationError:
#            assert True, "The ingester API couldn't authenticate."
#        except ValueError:
#            assert True, "Any parameter that has an invalid value such as location.location_id isn't set"
#        except:
#            assert True, "Any other run time error that the ingester platform throws."
#
#    def tearDown(self):
#        self.ingester_platform.reset()
#        for f_name in self.cleanup_files:
#            if os.path.exists(f_name):
#                try:
#                    os.remove(f_name)
#                except:
#                    print "Exception: ", str(sys.exc_info())

class TestMarshaller(unittest.TestCase):
    """Test marshalling and object round tripping"""
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.marshaller = Marshaller()
    
    def test_schema_attributes(self):
        schema = DataEntryMetadataSchema()
        schema.addAttr(Double("one"))
        schema.addAttr(String("two"))
        self.assertEquals("one", schema.attrs["one"].name)
        self.assertEquals("two", schema.attrs["two"].name)
        self.assertTrue(isinstance(schema.attrs["one"], Double))
        self.assertTrue(isinstance(schema.attrs["two"], String))
        
        schema_dict = self.marshaller.obj_to_dict(schema)
        
        schema_obj = self.marshaller.dict_to_obj(schema_dict)
        
        self.assertEquals("one", schema_obj.attrs["one"].name)
        self.assertEquals("two", schema_obj.attrs["two"].name)
        self.assertTrue(isinstance(schema_obj.attrs["one"], Double))
        self.assertTrue(isinstance(schema_obj.attrs["two"], String))

    def test_dataset_roundtrip(self):
        """Attempt to round trip a dataset object"""
        script_contents = """Some Script
More"""
        
        dataset = Dataset(1, 2, PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file", processing_script=script_contents), location_offset=LocationOffset(0, 1, 2))

        dataset_dict = self.marshaller.obj_to_dict(dataset)
        dataset1 = self.marshaller.dict_to_obj(dataset_dict)

        self.assertIsNotNone(dataset1, "Dataset should not be none")
        self.assertEquals(dataset1.location, dataset.location, "Location ID does not match")
        self.assertEquals(dataset1.schema, dataset.schema, "schema does not match %d!=%d"%(dataset1.schema, dataset.schema))
        self.assertEquals(dataset1.location_offset.x, 0)
        self.assertEquals(dataset1.location_offset.y, 1)
        self.assertEquals(dataset1.location_offset.z, 2)

    def test_data_entry(self):
        dt = datetime.datetime.utcfromtimestamp(1357788112)
        dt = dt.replace(tzinfo = jcudc24ingesterapi.UTC)
        
        data_entry = DataEntry(1, dt)
        data_entry["temp"] = 1.2
        
        data_entry_dto = self.marshaller.obj_to_dict(data_entry)
        self.assertEquals("2013-01-10T03:21:52.000Z", data_entry_dto["timestamp"])
        self.assertEquals(1, data_entry_dto["dataset"])
        self.assertEquals(1.2, data_entry_dto["data"]["temp"])

        data_entry_return = self.marshaller.dict_to_obj(data_entry_dto)
        self.assertEquals(data_entry.timestamp, data_entry_return.timestamp)
        self.assertEquals(data_entry.dataset, data_entry_return.dataset)
        self.assertEquals(data_entry.data["temp"], data_entry_return.data["temp"])

    def test_file_object_roundtrip(self):
        """The file object should marshall everything but the file stream"""
        data_entry = DataEntry(1)
        data_entry["temp"] = FileObject(os.path.join(
                    os.path.dirname(jcudc24ingesterapi.__file__), "tests/test_ingest.xml"), "text/xml")
        
        data_entry_dto = self.marshaller.obj_to_dict(data_entry)
        self.assertEqual("text/xml", data_entry_dto["data"]["temp"]["mime_type"])

if __name__ == '__main__':
    unittest.main()
