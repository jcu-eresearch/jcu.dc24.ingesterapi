
import datetime
import jcudc24ingesterapi
import os.path
import unittest

from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.models.locations import Location, Region, LocationOffset
from jcudc24ingesterapi.schemas.data_types import FileDataType, Double, String, DateTime
from jcudc24ingesterapi.models.data_sources import PullDataSource, PushDataSource
from jcudc24ingesterapi.models.data_entry import DataEntry, FileObject
from jcudc24ingesterapi.ingester_platform_api import IngesterPlatformAPI, Marshaller,\
    UnitOfWork
from jcudc24ingesterapi.authentication import CredentialsAuthentication
from jcudc24ingesterapi.models.metadata import DatasetMetadataEntry, DataEntryMetadataEntry
from jcudc24ingesterapi.schemas.metadata_schemas import DataEntryMetadataSchema, DatasetMetadataSchema
from jcudc24ingesterapi.models.sampling import PeriodicSampling #, CustomSampling, RepeatSampling
from jcudc24ingesterapi.ingester_exceptions import UnsupportedSchemaError, InvalidObjectError, UnknownObjectError, AuthenticationError,\
    StaleObjectError
from jcudc24ingesterapi.schemas.data_entry_schemas import DataEntrySchema
from jcudc24ingesterapi.search import DataEntrySearchCriteria,\
    DatasetMetadataSearchCriteria, LocationSearchCriteria, DatasetSearchCriteria,\
    DataEntrySchemaSearchCriteria, DataEntryMetadataSearchCriteria


class SchemaTest(unittest.TestCase):
    """
    This test defines and checks that the Ingester API works the way the provisioning interface expects.
    """
    def setUp(self):
        self.auth = CredentialsAuthentication("casey", "password")
        self.ingester_platform = IngesterPlatformAPI("http://localhost:8080/api", self.auth)
        self.schemas = []

    def compare_schema_attrs(self, attrs_src, attrs_dst):
        # make a copy
        attrs_dst = attrs_dst.copy()
        
        for attr in attrs_src:
            found = False
            for attr_dst in attrs_dst:
                if attr in attrs_dst:
                    del attrs_dst[attr]
                    found = True
                    break
            self.assertTrue(found, "Attribute not found "+attr)
        self.assertEquals(0, len(attrs_dst), "Extra attributes in destination")
                    

    def test_data_metadata(self):
        work = self.ingester_platform.createUnitOfWork()
        schema = DataEntryMetadataSchema("Quality Assurance")
        schema.addAttr(Double("value"))
        schema.addAttr(String("description"))
        work.post(schema)
        work.commit()
        self.schemas.append(schema)

        ingested_schema = self.ingester_platform.getSchema(schema.id)
        self.compare_schema_attrs(ingested_schema.attrs, schema.attrs)
        self.assertEquals(ingested_schema.name, schema.name)

    def test_dataset_metadata(self):
        work = self.ingester_platform.createUnitOfWork()
        schema = DatasetMetadataSchema("Dataset Calibration")
        schema.addAttr(DateTime("date"))
        schema.addAttr(String("description"))
        work.post(schema)
        work.commit()
        self.schemas.append(schema)

        ingested_schema = self.ingester_platform.getSchema(schema.id)
        self.compare_schema_attrs(ingested_schema.attrs, schema.attrs)
        self.assertEquals(ingested_schema.name, schema.name)

    def test_data(self):
        work = self.ingester_platform.createUnitOfWork()
        schema = DataEntrySchema("Test123")
        schema.addAttr(Double("value"))
        schema.addAttr(String("description"))
        work.post(schema)
        work.commit()
        self.schemas.append(schema)

        ingested_schema = self.ingester_platform.getSchema(schema.id)
        self.compare_schema_attrs(ingested_schema.attrs, schema.attrs)
        self.assertEquals(ingested_schema.name, schema.name)

    def test_dup_data(self):
        work = self.ingester_platform.createUnitOfWork()
        schema = DataEntrySchema("Test123")
        schema.addAttr(Double("value"))
        schema.addAttr(String("description"))
        work.post(schema)
        work.commit()
        self.schemas.append(schema)

    def test_delete(self):
        work = self.ingester_platform.createUnitOfWork()
        for schema in self.schemas:
            work.delete(schema)
        work.commit()

        for schema in self.schemas:
            self.assertIsNone(self.ingester_platform.getSchema(schema.id))

    def tearDown(self):
        self.ingester_platform.close()


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

        temp_work = self.ingester_platform.createUnitOfWork()
        temperature_schema = DataEntrySchema("Test Temp Schema")
        temperature_schema.addAttr(Double("temperature"))
        temp_work.post(temperature_schema)
        temp_work.commit()

        air_temperature_schema = DataEntrySchema("Air Temp Schema")
        air_temperature_schema.extends = [temperature_schema.id]
        air_temperature_schema = self.ingester_platform.post(air_temperature_schema)

        second_level_inheritence_schema = DataEntrySchema("Second Inheritence")
        second_level_inheritence_schema.extends = [air_temperature_schema.id]
        second_level_inheritence_schema = self.ingester_platform.post(second_level_inheritence_schema)

        # Check the name is set
        temperature_schema_1 = self.ingester_platform.getSchema(temperature_schema.id)
        self.assertIsNotNone(temperature_schema.name)
        self.assertEquals(temperature_schema.name, temperature_schema_1.name)
        
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        file_schema = self.ingester_platform.post(file_schema)

        dataset1 = Dataset(location=None, schema=temperature_schema.id)
        dataset2 = Dataset(location=None, schema=file_schema.id, data_source=PullDataSource("http://test.com", "file_handle", processing_script="file://d:/processing_scripts/awsome_processing.py"))

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
        timestamp = datetime.datetime.now()
        data_entry_1 = DataEntry(found_dataset_id, timestamp)
        data_entry_1['temperature'] = 27.8                # Add the extended schema items
        data_entry_1 = self.ingester_platform.post(data_entry_1)
        self.assertIsNotNone(data_entry_1.id)

        timestamp2 = timestamp + datetime.timedelta(seconds=1)
        data_entry_2 = DataEntry(found_dataset_id, timestamp2)
        data_entry_2['temperature'] = 27.8                # Add the extended schema items
        data_entry_2 = self.ingester_platform.post(data_entry_2)
        
        self.assertEquals(2, len(self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id), 0, 10).results))
        result = self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id), 0, 1)
        self.assertEquals(2, result.count)
        self.assertEquals(1, len(result.results))
        self.assertEquals(1, len(self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id), 1, 1).results))
        
        result = self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id), 2, 1)
        self.assertEquals(0, len(result.results))
                
        self.assertEquals(0, len(self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id, 
                                 end_time=timestamp-datetime.timedelta(seconds=60)), 0, 10).results))
        self.assertEquals(0, len(self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id, 
                                 start_time=timestamp+datetime.timedelta(seconds=60)), 0, 10).results))
        self.assertEquals(2, len(self.ingester_platform.search(DataEntrySearchCriteria(found_dataset_id, 
                                 start_time=timestamp-datetime.timedelta(seconds=60),
                                 end_time=timestamp+datetime.timedelta(seconds=60)), 0, 10).results))

        work = self.ingester_platform.createUnitOfWork()
        data_entry_3 = DataEntry(dataset2.id, datetime.datetime.now())
        data_entry_3['file'] = FileObject(f_handle=open(os.path.join(
                    os.path.dirname(jcudc24ingesterapi.__file__), "tests/test_ingest.xml")), 
                    mime_type="text/xml")
        work.post(data_entry_3)
        work.commit()
        self.assertIsNotNone(data_entry_3.id)
        
        f_in = self.ingester_platform.getDataEntryStream(dataset2.id, data_entry_3.id, "file")
        self.assertIsNotNone(f_in)
        data = f_in.read()
        f_in.close()
        self.assertLess(0, len(data), "Expected data in file")

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
        
        # Now find that metadata
        results = self.ingester_platform.search(DatasetMetadataSearchCriteria(data_entry_1.dataset),0 , 10).results
        self.assertEqual(1, len(results))
        
        
        data_entry_md_schema = DataEntryMetadataSchema("test")
        data_entry_md_schema.addAttr(String("description"))
        data_entry_md_schema.addAttr(Double("value"))
        data_entry_md_schema = self.ingester_platform.post(data_entry_md_schema)
        calibration = DataEntryMetadataEntry(metadata_schema_id=int(data_entry_md_schema.id), dataset_id=dataset2.id, object_id=data_entry_3.id)
        calibration["description"] = "Test"
        calibration["value"] = 1.2

        calibration2 = DataEntryMetadataEntry(metadata_schema_id=int(data_entry_md_schema.id), dataset_id=dataset2.id, object_id=data_entry_3.id)
        calibration2["description"] = "Test2"
        calibration2["value"] = 2.3
        calibration2 = self.ingester_platform.post(calibration2)

        calibrations = self.ingester_platform.search(DataEntryMetadataSearchCriteria(int(81), int(3648)), offset=0, limit=1000)
        self.assertEquals(1, len(calibrations.results))
        self.assertEquals(calibrations.results[0].schema_id, data_entry_md_schema.id)

        self.ingester_platform.delete(calibration2)
        self.ingester_platform.delete(calibration)
        self.ingester_platform.delete(data_entry_md_schema)

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
        
    def test_parent_schemas(self):
        """This test creates a nested schema with attributes provided at 2
        different levels. A data entry is saved, and then retrieved, and the
        values tested.
        """
        loc1 = self.ingester_platform.post(Location(11.0, 11.0, "Test Site", 100))

        temp_work = self.ingester_platform.createUnitOfWork()
        temperature_schema = DataEntrySchema("Test Temp Schema")
        temperature_schema.addAttr(Double("temperature"))
        temp_work.post(temperature_schema)
        temp_work.commit()

        air_temperature_schema = DataEntrySchema("Air Temp Schema")
        air_temperature_schema.extends = [temperature_schema.id]
        air_temperature_schema = self.ingester_platform.post(air_temperature_schema)

        instrument_schema = DataEntrySchema("Instrument Schema")
        instrument_schema.extends = [air_temperature_schema.id]
        instrument_schema.addAttr(Double("var2"))
        instrument_schema = self.ingester_platform.post(instrument_schema)

        dataset = Dataset(location=loc1.id, schema=instrument_schema.id)
        dataset = self.ingester_platform.post(dataset)
        
        work = self.ingester_platform.createUnitOfWork()
        data_entry = DataEntry(dataset.id, datetime.datetime.now())
        data_entry["temperature"] = 10
        data_entry["var2"] = 11
        work.post(data_entry)
        work.commit()
        
        data_entry_ret = self.ingester_platform.getDataEntry(dataset.id, data_entry.id)

        self.assertEquals(data_entry["temperature"], data_entry_ret["temperature"])
        self.assertEquals(data_entry["var2"], data_entry_ret["var2"])
        

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
        temp_dataset = Dataset(location=None, schema=temperature_schema.id)

        file_dataset = Dataset(location=None, schema=file_schema.id, data_source=PullDataSource("http://test.com", "file_handle", processing_script="file://d:/processing_scripts/awsome_processing.py"))


    def test_listeners(self):
        # Use a list to beat the closure
        called = [False] 
        
        def loc_listener(obj, var, value):
            # The listener will be called when the object is posted
            # and when it is committed, so we want to filter out the 
            # post call
            if var == "_id" and value > 0:
                called.remove(False)
                called.append(True)
        
        loc = Location()
        loc.name = "Test Loc1"
        loc.set_listener(loc_listener)

        work = self.ingester_platform.createUnitOfWork()
        work.post(loc)
        work.commit()

        self.assertTrue(called[0])

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
        # Use a list to beat the closure
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
        
        locs = self.ingester_platform.search(LocationSearchCriteria(),0 , 10).results
        self.assertEquals(1, len(locs))
        
        # Now update the location
        loc1.name = "The Test Site"
        loc1.latitude = -19.0
        
        # Test that the version check is observed
        self.assertEquals(1, loc1.version)
        loc1.version = 0
        self.assertRaises(StaleObjectError, self.ingester_platform.post, loc1)
        
        loc1.version = 1
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
        
        dataset = Dataset(location=loc.id, schema=file_schema.id, data_source=PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file", processing_script=script_contents), location_offset=LocationOffset(0, 1, 2))
        dataset1 = self.ingester_platform.post(dataset)
        self.assertIsNotNone(dataset1, "Dataset should not be none")
        self.assertEquals(dataset1.location, dataset.location, "Location ID does not match")
        self.assertEquals(dataset1.schema, dataset.schema, "schema does not match %d!=%d"%(dataset1.schema, dataset.schema))
        self.assertEquals(dataset1.location_offset.x, 0)
        self.assertEquals(dataset1.location_offset.y, 1)
        self.assertEquals(dataset1.location_offset.z, 2)

        self.assertEquals(script_contents, dataset1.data_source.processing_script)

        datasets = self.ingester_platform.findDatasets()
        self.assertEquals(1, len(datasets))

        datasets = self.ingester_platform.findDatasets(location=loc.id)
        self.assertEquals(1, len(datasets))
        
        data_entry_schemas = self.ingester_platform.search(DataEntrySchemaSearchCriteria(),0 , 10).results
        self.assertEquals(1, len(data_entry_schemas))

        datasets = self.ingester_platform.search(DatasetSearchCriteria(),0 , 10).results
        self.assertEquals(1, len(datasets))
        
    def test_schema_persistence(self):
        file_schema = DataEntrySchema()
        file_schema.addAttr(FileDataType("file"))
        self.ingester_platform.post(file_schema)
        
    def test_unit_of_work_persistence(self):
        unit = self.ingester_platform.createUnitOfWork()
        
        loc = Location(10.0, 11.0, "Test Site", 100, None)
        unit.insert(loc)
        self.assertIsNotNone(loc.id)
        
        file_schema = DataEntrySchema()
        file_schema.name = "File Schema"
        file_schema.addAttr(FileDataType("file"))
        file_schema_id = unit.insert(file_schema)

        self.assertIsNotNone(file_schema_id, "Schema ID should not be null")

        dataset = Dataset(location=loc.id, schema=file_schema.id, data_source=PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file"))
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
        
        dataset = Dataset(location=loc.id, schema=file_schema.id, data_source=PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file", sampling=PeriodicSampling(10000)))
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
        
        dataset = Dataset(location=1, schema=2, data_source=PullDataSource("http://www.bom.gov.au/radar/IDR733.gif", "file", processing_script=script_contents), location_offset=LocationOffset(0, 1, 2))

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
        data_entry["temp"] = FileObject(f_path=os.path.join(
                    os.path.dirname(jcudc24ingesterapi.__file__), "tests/test_ingest.xml"), mime_type="text/xml")
        
        data_entry_dto = self.marshaller.obj_to_dict(data_entry)
        self.assertEqual("text/xml", data_entry_dto["data"]["temp"]["mime_type"])
        
        data_entry_domain = self.marshaller.dict_to_obj(data_entry_dto)
        self.assertEqual("text/xml", data_entry_domain["temp"].mime_type)

    def test_unit_of_work_roundtrip(self):
        unit = UnitOfWork(None)
        loc = Location(10, 11)
        loc.name = "Loc 1"
        unit.insert(loc)
        unit_dict = self.marshaller.obj_to_dict(unit)
        self.assertEquals("unit_of_work", unit_dict["class"])
        
        unit2 = self.marshaller.dict_to_obj(unit_dict)
        self.assertEquals(10.0, unit2._to_insert[0].latitude)
        self.assertEquals(11.0, unit2._to_insert[0].longitude)

    def test_special_attr(self):
        loc = Location(10, 11)
        loc.correlationid = -1
        loc_dict = self.marshaller.obj_to_dict([loc], special_attrs=["correlationid"])
        
        self.assertEquals(1, len(loc_dict))
        self.assertEquals(-1, loc_dict[0]["correlationid"])
        
    def test_unit_of_work_validation(self):
        unit = UnitOfWork(None)
        loc = Location(10, 11)
        self.assertRaises(InvalidObjectError, unit.insert, loc)
        loc.name = "test"
        unit.insert(loc) # Should work now.

    def test_marshaller_data_entry_schema(self):
        schema = {'attributes': [{'units': None, 'description': None, 'name': 'file', 'class': 'file'}], 'id': None, 'class': 'data_entry_schema'}
        schema = self.marshaller.dict_to_obj(schema)

if __name__ == '__main__':
    unittest.main()
