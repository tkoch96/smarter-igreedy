import geopy.distance

DATA_DIR = "data"
FIG_DIR = "figures"
CACHE_DIR = "cache"


def get_distance(loc1, loc2):
	# assume that loc1 and loc2 are lat,lon pairs
	return geopy.distance.geodesic(loc1, loc2).km

def convert_32_to_24(slash_32):
	return ".".join(slash_32.split('.')[0:3]) + ".0"