import geopy.distance

DATA_DIR = "data"
FIG_DIR = "figures"
CACHE_DIR = "cache"


# Global cache dictionary for distances
_DISTANCE_CACHE = {}

def get_distance(loc1, loc2):
	"""
	Calculates geodesic distance in km, with aggressive caching.
	loc1 and loc2 are assumed to be (lat, lon) tuples.
	"""
	# Round to 4 decimal places (~11 meters resolution) to dramatically 
	# increase cache hits, especially from the scipy optimizer.
	lat1, lon1 = round(loc1[0], 4), round(loc1[1], 4)
	lat2, lon2 = round(loc2[0], 4), round(loc2[1], 4)
	
	# Order the points so A->B and B->A generate the exact same cache key
	if (lat1, lon1) > (lat2, lon2):
		key = (lat1, lon1, lat2, lon2)
	else:
		key = (lat2, lon2, lat1, lon1)

	if key not in _DISTANCE_CACHE:
		# Only compute the heavy trig if we haven't seen this pair before
		_DISTANCE_CACHE[key] = geopy.distance.geodesic(loc1, loc2).km
		
	return _DISTANCE_CACHE[key]

def convert_32_to_24(slash_32):
	return ".".join(slash_32.split('.')[0:3]) + ".0"