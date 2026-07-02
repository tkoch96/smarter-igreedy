import numpy as np, pickle, os
from scipy.optimize import minimize
from utils import *
from perfect_geolocator import Perfect_Geolocator
from pull_ripe_atlas_measurement_data import RipeAtlasPipeline
from random_geolocator import Random_Geolocator
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from feasible_region_maintainer import FeasibleRegion, HARD_CIRCLE, GAUSSIAN
from probabilistic_helpers import GLOBAL_SIGMA_MS

from plot_results import *

class Geolocator_Comparator:
	def __init__(self):
		self.geolocators = [Iterative_Greedy_Geolocator(), Perfect_Geolocator(), Random_Geolocator()]
		self.measurement_converter_mode = 'nearest_neighbor'
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
			for src in self.target_data['loc_loc_meas']:
				for dst in self.target_data['loc_loc_meas'][src]:
					self.target_data['loc_loc_meas'][src][dst] = [self.target_data['loc_loc_meas'][src][dst]]

	def convert_measurements_to_locations(self, measurements):
		"""
		Phase (b) estimation: turn a strategy's chosen measurements into
		location estimates (see run() for the two-phase design).

		The converter mode is the *estimation half* of a strategy, not a
		shared harness component. 'nearest_neighbor' (report the lowest-RTT
		VP's location) is the deliberately dumb estimator paired with the
		baselines; 'hard_circle'/'gaussian' are the overlap-computation
		methodology under development. Geolocators that carry their own
		estimator (Iterative_Greedy via get_current_estimates) never enter
		this function.
		"""
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

			elif self.measurement_converter_mode in ('hard_circle', 'great_circle_overlap_centroid'):
				region = FeasibleRegion(target_id=dst, mode=HARD_CIRCLE)
				for src, rtt in src_rtts.items():
					if src in address_to_loc:
						region.add_measurement(address_to_loc[src], max(0.0, rtt))
				if region.constraints:
					estimated_locations[dst] = region.get_location()

			elif self.measurement_converter_mode == 'gaussian':
				region = FeasibleRegion(target_id=dst, mode=GAUSSIAN)
				for src, rtt in src_rtts.items():
					if src in address_to_loc:
						region.add_measurement(address_to_loc[src], rtt, sigma_ms=GLOBAL_SIGMA_MS)
				if region.constraints:
					estimated_locations[dst] = region.get_location()

			else:
				raise ValueError(f"measurement_converter_mode {self.measurement_converter_mode} not understood")

		return estimated_locations

	def get_random_subsample(self, n=100):
		## Gets a random sample of all the measurement data, for testing
		print("Grabbing random subsample of {} sources".format(n))
		all_srcs = list(self.target_data['loc_loc_meas'])
		np.random.shuffle(all_srcs)
		all_srcs_subsample = all_srcs[0:n]
		new_target_data = {s:{} for s in all_srcs_subsample}
		for src in all_srcs_subsample:
			for dst in self.target_data['loc_loc_meas'][src]:
				if src == dst: continue
				try:
					new_target_data[dst]
					new_target_data[src][dst] = self.target_data['loc_loc_meas'][src][dst]
				except KeyError:
					pass
		self.target_data['loc_loc_meas'] = new_target_data

	def do_cache(self, geolocator):
		return {'smart_perfect': True, 'random': True}.get(geolocator.name, False)

	def run(self, min_budget=100, max_budget=2500, step=100):
		## Two-phase comparison, analogous to a train/test split:
		##   (a) selection  - geolocator.measurements(budget) decides which pings
		##       to spend, under realistic information limits (no ground truth;
		##       the oracle deliberately cheats here, that is its job as an
		##       upper bound).
		##   (b) evaluation - the chosen measurements become location estimates
		##       and are scored against ground-truth probe locations (perfect
		##       information, unrealistic by design; ground truth is never fed
		##       back into selection or estimation).
		## Each strategy is a complete system of selection + estimation, so the
		## baselines are *supposed* to lack the overlap computation: random+NN
		## is the dumb whole-system baseline, greedy+overlap is ours.
		self.load_target_measurement_data()

		self.get_random_subsample()
		
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
				
				# A geolocator with get_current_estimates() brings its own
				# estimation method (the greedy's live FeasibleRegion overlap
				# estimates); everything else is paired with the converter-mode
				# estimator (nearest_neighbor for the baselines).
				if hasattr(geolocator, 'get_current_estimates'):
					estimated_locations = geolocator.get_current_estimates()
				else:
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
	np.random.seed(31415)
	gc = Geolocator_Comparator()
	
	# 1. Load the data into gc.target_data
	gc.load_target_measurement_data()
	plot_ping_count_cdf(gc.target_data)
	
	# # 2. Call the diagnostic plot to see what your dataset actually looks like
	# print("Generating Latency vs. Distance diagnostic plot...")
	# plot_latency_vs_distance(gc.target_data, os.path.join(FIG_DIR, "latency_vs_distance.pdf"))
	
	# 3. Run your geolocator simulation as normal
	gc.run()

