import numpy as np, pickle, os
from scipy.optimize import minimize
from utils import *
from perfect_geolocator import Perfect_Geolocator
from pull_ripe_atlas_measurement_data import RipeAtlasPipeline
from random_geolocator import Random_Geolocator
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from feasible_region_maintainer import FeasibleRegion

from plot_results import *

class Geolocator_Comparator:
	def __init__(self):
		self.geolocators = [Perfect_Geolocator(), Random_Geolocator(), Iterative_Greedy_Geolocator()]
		self.measurement_converter_mode = 'great_circle_overlap_centroid' # setting this to 'great_circle_overlap_centroid' really hurts performance, why?
		self.target_data = None
		self.errors = {}

	def load_target_measurement_data(self):
		## loads all measurements from ripe atlas probes, and information about those probes
		cache_fn = os.path.join(CACHE_DIR, 'cached_target_data.pkl')
		if not os.path.exists(cache_fn):
			rap = RipeAtlasPipeline(start_date="2026-02-24", end_date="2026-02-24")
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
				closest_src = min(src_rtts, key=src_rtts.get)
				if closest_src in address_to_loc:
					estimated_locations[dst] = address_to_loc[closest_src]

			elif self.measurement_converter_mode == 'great_circle_overlap_centroid':
				# Use the new FeasibleRegion object for batch estimation
				region = FeasibleRegion(target_id=dst)
				
				batch_measurements = []
				for src, rtt in src_rtts.items():
					if src in address_to_loc:
						batch_measurements.append((address_to_loc[src], rtt))
				
				if not batch_measurements:
					continue
				elif len(batch_measurements) == 1:
					estimated_locations[dst] = batch_measurements[0][0]
				else:
					# Seed an initial guess before optimizing (optional but helps convergence)
					lat_guess = sum(m[0][0] for m in batch_measurements) / len(batch_measurements)
					lon_guess = sum(m[0][1] for m in batch_measurements) / len(batch_measurements)
					region.best_guess = np.array([lat_guess, lon_guess])
					
					region.add_measurements_batch(batch_measurements)
					estimated_locations[dst] = region.get_location()

			else:
				raise ValueError(f"measurement_converter_mode {self.measurement_converter_mode} not understood")

		return estimated_locations

	def do_cache(self, geolocator):
		return {'smart_perfect': True, 'random': True}.get(geolocator.name, False)

	def run(self, min_budget=100, max_budget=5000, step=100):
		self.load_target_measurement_data()
		
		address_to_loc = self.target_data.get('address_to_loc', {})
		all_targets = set()
		for dsts in self.target_data.get('loc_loc_meas', {}).values():
			all_targets.update(dsts.keys())
		
		# Dictionary to hold the plotting data
		self.plot_data = {}

		for geolocator in self.geolocators:
			print(f"\n--- Running {geolocator.name} ---")
			
			cache_fn = os.path.join(CACHE_DIR, f"cached_results_{geolocator.name}_{self.measurement_converter_mode}.pkl")
			
			if os.path.exists(cache_fn) and self.do_cache(geolocator):
				self.plot_data[geolocator.name] = pickle.load(open(cache_fn, 'rb'))
				continue
			
			geolocator.set_data(self.target_data)
			geolocator.solve()
			
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
			if self.do_cache(geolocator):
				pickle.dump(self.plot_data[geolocator.name], open(cache_fn, 'wb'))

		# Call the plotting function after all geolocators have run (or loaded)
		plot_error_over_budget(self.plot_data, os.path.join(FIG_DIR, "geolocator_results.pdf"))

if __name__ == "__main__":
	gc = Geolocator_Comparator()
	
	# 1. Load the data into gc.target_data
	gc.load_target_measurement_data()
	
	# 2. Call the diagnostic plot to see what your dataset actually looks like
	print("Generating Latency vs. Distance diagnostic plot...")
	plot_latency_vs_distance(gc.target_data, os.path.join(FIG_DIR, "latency_vs_distance.pdf"))
	
	# 3. Run your geolocator simulation as normal
	gc.run()

