import unittest

from jcudc24ingesterapi.models.data_entry import DataEntry
from jcudc24ingesterapi.models.data_sources import PullDataSource
from jcudc24ingesterapi.models.dataset import Dataset

class TestIngesterModels(unittest.TestCase):
    def test_metadata(self):
        pass

    def test_data_entry(self):
        pass

    def test_data_sources(self):
        pass

    def test_dataset(self):
        # Basic instanciation
        ds = Dataset()
        ds = Dataset(data_source=PullDataSource())
        self.assertRaises(TypeError, Dataset, data_source=1)


    def test_sampling(self):
        pass

    def test_locations(self):
        pass

if __name__ == '__main__':
    unittest.main()
