#!/usr/bin/env python
from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.models.locations import Location, Region
from jcudc24ingesterapi.schemas.data_types import FileDataType, Double, String
from jcudc24ingesterapi.models.data_sources import PullDataSource, DatasetDataSource
from jcudc24ingesterapi.models.data_entry import DataEntry
from jcudc24ingesterapi.ingester_platform_api import IngesterPlatformAPI
from jcudc24ingesterapi.authentication import CredentialsAuthentication
from jcudc24ingesterapi.models.metadata import MetadataEntry
from jcudc24ingesterapi.schemas.metadata_schemas import DataEntryMetadataSchema, DatasetMetadataSchema
from jcudc24ingesterapi.models.sampling import RepeatSampling, PeriodicSampling, CustomSampling
from jcudc24ingesterapi.ingester_exceptions import UnsupportedSchemaError, InvalidObjectError, UnknownObjectError, AuthenticationError
from jcudc24ingesterapi.schemas.data_entry_schemas import DataEntrySchema

auth = CredentialsAuthentication("casey", "password")
ingester_platform = IngesterPlatformAPI("http://localhost:8080/api", auth)

# Setup the file storage schema
file_schema = DataEntrySchema("arduino_file")
file_schema.addAttr(FileDataType("file"))
file_schema = ingester_platform.post(file_schema)

temp_schema = DataEntrySchema("temperature_reading")
temp_schema.addAttr(Double("temp"))
temp_schema = ingester_platform.post(temp_schema)

# Setup the location
loc = Location(-19.34427, 146.784197, "Mt Stuart", 100, None)
loc = ingester_platform.post(loc)

# Create the dataset to store the data in
file_dataset = Dataset(location=loc.id, schema=file_schema.id, data_source=PullDataSource(url="http://emu.hpc.jcu.edu.au/tree/split/", field="file", recursive=True, sampling=PeriodicSampling(10000)))
file_dataset.enabled = False
file_dataset = ingester_platform.post(file_dataset)

# This processing script is 
processing_script = """
import os
import datetime
from dc24_ingester_platform.utils import *

def process(cwd, data_entries):
    ret = []
    started = False
    for data_entry in data_entries:
        with open(os.path.join(cwd, data_entry["file"].f_path)) as f:
            for l in f.readlines():
                l = l.strip()
                if not started:
                    if l == "BEGIN TEMP": started = True
                elif started and l == "END TEMP":
                    break
                else:
                    # parse line
                    l = l.split(",")
                    if len(l) < 3: continue
                    if l[1] != "%s": continue
                    new_data_entry = DataEntry(timestamp=datetime.datetime.now())
                    new_data_entry["temp"] = float(l[2])
                    ret.append( new_data_entry )
    return ret
"""

temp_dataset1 = Dataset(location=loc.id, schema=file_schema.id, data_source=DatasetDataSource(file_dataset.id, processing_script=processing_script%"28180E08030000BE"))
temp_dataset1.enabled = True
temp_dataset1 = ingester_platform.post(temp_dataset1)

ingester_platform.enableDataset(file_dataset.id)

