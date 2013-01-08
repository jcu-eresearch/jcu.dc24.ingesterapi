__author__ = 'Casey Bajema'

class Region(object):
    """
        Represents a 2D area on earth, possible a sub-region of another regions.

        An example would be that Queensland is a sub-region of Australia
    """
    __xmlrpc_class__ = "region"

    def __init__(self, region_name = None, region_points = None, parent_regions = None, region_id = None):
        """
        :param region_name: A human recognisable string naming the region
        :param region_points: A 2D array of latitude/longitude points ((lat, long), (lat, long),...), the
                                last point joins the first point to close the region.
        :param parent_regions: A region object containing the parent region.
        :return: The initialised region.
        """
        self.id = region_id
        self.name = region_name
        self.region_points = region_points
        self.parent_region = parent_regions

class Location(object):
    """
    A 3D point on Earth.
    """
    __xmlrpc_class__ = "location"
    
    def __init__(self, latitude=None, longitude=None, location_name = None, elevation = None, region = None):
        """
        :param latitude: Double value indicating the latitude (WGS84 assumed, metadata should be attached otherwise)
        :param longitude: Double value representing the longitude (WGS84 assumed, metadata should be attached otherwise)
        :param location_name: Human identifiable string naming this location
        :param elevation: Height of the location (Height above mean sea level assumed, attach metadata otherwise)
        :param region: A region object that this location is associated with, the location should be within
                        the regions area.
        :return: Initialised Location object.
        """
        self.id = None
        self.name = location_name              # String
        self.latitude = latitude                        # double
        self.longitude = longitude                      # double
        self.elevation = elevation                      # double
        self.region = region

class LocationOffset(object):
    """An offset from a frame of reference.
    """
    __xmlrpc_class__ = "offset"
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z
