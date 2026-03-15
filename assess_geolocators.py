import numpy as np, pickle, os
import geopy.distance
from scipy.optimize import minimize
from utils import *
from perfect_geolocator import Perfect_Geolocator
from pull_ripe_atlas_measurement_data import RipeAtlasPipeline
from random_geolocator import Random_Geolocator

from plot_results import *

class Geolocator_Comparator:
	def __init__(self):
		self.geolocators = [Perfect_Geolocator(), Random_Geolocator()]
		self.measurement_converter_mode = 'nearest_neighbor' # or 'nearest_neighbor'
		self.target_data = None
		self.errors = {}

	def load_target_measurement_data(self):
		## loads all measurements from ripe atlas probes, and information about those probes
		cache_fn = os.path.join(CACHE_DIR, 'cached_target_data.pkl')
		if not os.path.exists(cache_fn):
			rap = RipeAtlasPipeline(start_date="2026-01-24", end_date="2026-01-24")
			rap.execute()
			self.target_data = rap.load_parsed_target_data()
			pickle.dump(self.target_data, open(cache_fn, 'wb'))
		else:
			self.target_data = pickle.load(open(cache_fn, 'rb'))

	def convert_measurements_to_locations(self, measurements):
		estimated_locations = {}
		address_to_loc = self.target_data.get('address_to_loc', {})
		
		# Invert measurements to be dst -> src -> min_rtt
		dst_to_src_rtts = {}
		for src, dsts in measurements.items():
			for dst, rtts in dsts.items():
				if not rtts: 
					continue
				min_rtt = min(rtts)
				if dst not in dst_to_src_rtts:
					dst_to_src_rtts[dst] = {}
				dst_to_src_rtts[dst][src] = min_rtt

		for dst, src_rtts in dst_to_src_rtts.items():
			if self.measurement_converter_mode == 'nearest_neighbor':
				# Target location is the location of the VP with the lowest latency
				closest_src = min(src_rtts, key=src_rtts.get)
				if closest_src in address_to_loc:
					estimated_locations[dst] = address_to_loc[closest_src]

			elif self.measurement_converter_mode == 'great_circle_overlap_centroid':
				sources = []
				max_distances = []
				
				for src, rtt in src_rtts.items():
					if src in address_to_loc:
						sources.append(address_to_loc[src])
						# Note: 1ms RTT = ~100km one-way distance in fiber. 
						# Adjust to 50.0 if you strictly meant 50km one-way.
						max_distances.append(rtt * 100.0) 

				if not sources:
					continue
				if len(sources) == 1:
					estimated_locations[dst] = sources[0]
					continue

				# Initial guess is the simple centroid of all responding sources
				lat_guess = sum(s[0] for s in sources) / len(sources)
				lon_guess = sum(s[1] for s in sources) / len(sources)
				initial_guess = np.array([lat_guess, lon_guess])

				# Objective: minimize distance violations based on the Speed of Light
				def error_function(point):
					lat, lon = point
					penalty = 0
					for (src_lat, src_lon), max_dist in zip(sources, max_distances):
						dist = geopy.distance.geodesic((lat, lon), (src_lat, src_lon)).km
						
						# Penalize heavily if we exceed the physical distance limit dictated by RTT
						if dist > max_dist:
							penalty += (dist - max_dist) ** 2
						else:
							# Add a tiny pull towards the center to approximate a centroid within the valid intersection
							penalty += 0.001 * dist 
					return penalty

				# Use Nelder-Mead as it handles non-differentiable spatial optimizations well
				result = minimize(
					error_function, 
					initial_guess, 
					method='Nelder-Mead',
					bounds=[(-90, 90), (-180, 180)]
				)
				
				estimated_locations[dst] = (result.x[0], result.x[1])

			else:
				raise ValueError(f"measurement_converter_mode {self.measurement_converter_mode} not understood")

		return estimated_locations

	def run(self, min_budget=100, max_budget=2000, step=100):
		self.load_target_measurement_data()
		
		address_to_loc = self.target_data.get('address_to_loc', {})
		all_targets = set()
		for dsts in self.target_data.get('loc_loc_meas', {}).values():
			all_targets.update(dsts.keys())
		
		# Dictionary to hold the plotting data
		self.plot_data = {}

		for geolocator in self.geolocators:
			geolocator.set_data(self.target_data)
			geolocator.solve()
			
			print(f"\n--- Running {geolocator.name} ---")
			
			# Initialize data storage for this specific geolocator
			self.plot_data[geolocator.name] = {'budgets': [], 'errors': []}
			
			for budget in range(min_budget, max_budget + 1, step):
				budgeted_measurements = geolocator.measurements(budget)
				estimated_locations = self.convert_measurements_to_locations(budgeted_measurements)

				errors = []
				for dst in all_targets:
					if dst not in address_to_loc:
						continue 
						
					actual_location = address_to_loc[dst]
					
					if dst in estimated_locations:
						error_km = get_distance(estimated_locations[dst], actual_location)
						errors.append(error_km)
					else:
						# Penalty for missing targets
						errors.append(10000.0) 
				
				if errors:
					avg_error = np.mean(errors)
					targets_found = len(estimated_locations)
					print(f"Budget: {budget:4d} | Targets Estimated: {targets_found:4d}/{len(all_targets)} | Avg Error: {avg_error:.2f} km")
					
					# Store the results
					self.plot_data[geolocator.name]['budgets'].append(budget)
					self.plot_data[geolocator.name]['errors'].append(avg_error)

		# Call the plotting function after all geolocators have run
		plot_error_over_budget(self.plot_data, os.path.join(FIG_DIR, "geolocator_results.pdf"))


if __name__ == "__main__":
	gc = Geolocator_Comparator()
	gc.run()