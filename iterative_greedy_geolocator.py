import numpy as np, multiprocessing,time, os, pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils import *
from feasible_region_maintainer import FeasibleRegion


def default_expected_rtt_model(vp_loc, target_region):
	"""Default geometric RTT estimation."""
	current_guess_loc = target_region.get_location()
	distance_to_guess = get_distance(vp_loc, current_guess_loc)
	return distance_to_guess * 1.5 / 100.0

def default_utility_evaluator(vp, target_region, vp_loc, current_size, rtt_model_func):
	"""Default utility calculation simulating region reduction."""
	expected_rtt = rtt_model_func(vp_loc, target_region)
	
	# Clone and simulate
	temp_region = target_region.clone()
	temp_region.constraints.append((vp_loc, expected_rtt * 100.0))
	temp_region._update_estimate()
	
	new_size = temp_region.get_region_size()
	return current_size - new_size

def _evaluate_vp_worker(vp, target_region, vp_loc, current_size, utility_func, rtt_func):
	"""
	The standalone worker. It dynamically calls whatever utility_func 
	and rtt_func were injected into the system.
	"""
	if not target_region.constraints:
		return vp, 1000000.0 
	
	# Delegate the actual math to the injected modular function
	utility_score = utility_func(vp, target_region, vp_loc, current_size, rtt_func)
	return vp, utility_score


class Iterative_Greedy_Geolocator:
	def __init__(self, max_workers=None, utility_func=None, rtt_func=None):
		self.name = "iterative_greedy"
		self.data = None
		self.vp_locations = {}
		
		# Use provided functions or fall back to defaults
		self.utility_func = utility_func or default_utility_evaluator
		self.rtt_func = rtt_func or default_expected_rtt_model
	
		if max_workers is None:
			max_workers = multiprocessing.cpu_count()		
		self.max_workers = max_workers
		self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
		
		# --- STATE VARIABLES ---
		self.measurement_history = []  
		self.target_regions = {}
		self.measurements_used = {}
		self.current_region_sizes = {}
		self.best_vp_cache = {}


		self.available_measurements = {}
		self.targets = []
		self.utility_tracking = []

	def set_data(self, data):
		self.data = data
		self.vp_locations = data.get('address_to_loc', {})

	def get_prior_guess(self, dst):
		return (0.0, 0.0)

	def solve(self):
		self.measurement_history = []
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		self.available_measurements = {} 
		
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in self.available_measurements:
						self.available_measurements[dst] = []
					self.available_measurements[dst].append(src)
		
		self.targets = list(self.available_measurements.keys())
		if not self.targets:
			return

		self.target_regions = {dst: FeasibleRegion(dst, self.get_prior_guess(dst)) for dst in self.targets}
		self.measurements_used = {dst: set() for dst in self.targets}
		self.current_region_sizes = {dst: 20037.0 for dst in self.targets}
		self.best_vp_cache = {}
		
		for dst in self.targets:
			self._update_best_vp_for_target(dst)

		cache_fn = os.path.join(CACHE_DIR, f"{self.name}_initial_pass_{len(self.targets)}_targets.pkl")

		if os.path.exists(cache_fn):
			with open(cache_fn, 'rb') as f:
				self.best_vp_cache = pickle.load(f)
		else:
			for dst in self.targets:
				self._update_best_vp_for_target(dst)
			with open(cache_fn, 'wb') as f:
				pickle.dump(self.best_vp_cache, f)

	def _update_best_vp_for_target(self, dst):
		best_src = None
		best_utility = -float('inf')
		
		available_srcs = [s for s in self.available_measurements[dst] if s not in self.measurements_used.get(dst,[])]
		
		if not available_srcs:
			self.best_vp_cache[dst] = (None, -float('inf'))
			return
		
		try:
			target_region = self.target_regions[dst]
		except KeyError:
			return
		current_size = target_region.get_region_size()
		
		# Pass the injected functions into the worker
		futures = [
			self.executor.submit(
				_evaluate_vp_worker, 
				src, 
				target_region, 
				self.vp_locations[src], 
				current_size,
				self.utility_func,  # <-- Injected utility module
				self.rtt_func       # <-- Injected RTT module
			)
			for src in available_srcs
		]

		for future in as_completed(futures):
			src, utility = future.result()
			if utility > best_utility:
				best_utility = utility
				best_src = src
	
		self.best_vp_cache[dst] = (best_src, best_utility)

	def measurements(self, budget):
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		
		while len(self.measurement_history) < budget:
			best_global_dst = None
			best_global_src = None
			best_global_utility = -float('inf')
			
			for dst, (src, utility) in self.best_vp_cache.items():
				if src is not None and utility > best_global_utility:
					best_global_utility = utility
					best_global_dst = dst
					best_global_src = src
					
			if best_global_dst is None:
				break 

			size_before = self.current_region_sizes[best_global_dst]
			expected_utility = best_global_utility
				
			self.measurements_used[best_global_dst].add(best_global_src)
			actual_rtts = loc_loc_meas[best_global_src][best_global_dst]
			
			min_actual_rtt = min(actual_rtts)
			self.target_regions[best_global_dst].add_measurement(self.vp_locations[best_global_src], min_actual_rtt)

			new_actual_size = self.target_regions[best_global_dst].get_region_size()
			actual_utility = size_before - new_actual_size
			
			predicted_rtt_used = self.rtt_func(
				self.vp_locations[best_global_src], 
				self.target_regions[best_global_dst]
			)

			self.current_region_sizes[best_global_dst] = new_actual_size

			self.utility_tracking.append({
				'ping_num': len(self.measurement_history) + 1,
				'target': best_global_dst,
				'src': best_global_src,
				'expected_util': expected_utility,
				'actual_util': actual_utility,
				'error': expected_utility - actual_utility,
				'predicted_rtt': predicted_rtt_used,
				'actual_rtt': min_actual_rtt
			})
			
			self._update_best_vp_for_target(best_global_dst)
			self.measurement_history.append((best_global_src, best_global_dst))

			actual_pings = len(self.measurement_history)

			if actual_pings % 500 == 0: # Print a debug snapshot every 500 pings
				print(f"      [DEBUG] --- Utility Reality Check at Ping {actual_pings} ---")
				recent_pings = self.utility_tracking[-500:]
				avg_expected = sum(p['expected_util'] for p in recent_pings) / len(recent_pings)
				avg_actual = sum(p['actual_util'] for p in recent_pings) / len(recent_pings)
				print(f"      [DEBUG] Average Expected Utility (Last 500): {avg_expected:.2f} km^2 reduction")
				print(f"      [DEBUG] Average Actual Utility (Last 500):   {avg_actual:.2f} km^2 reduction")
				worst_delusion = max(recent_pings, key=lambda x: abs(x['error']))
				print(f"      [DEBUG] Biggest Hallucination: Ping {worst_delusion['ping_num']}")
				print(f"      [DEBUG]    -> Expected drop of {worst_delusion['expected_util']:.2f}, actually dropped by {worst_delusion['actual_util']:.2f}")
				print(f"      [DEBUG]    -> Algorithm assumed RTT: {worst_delusion['predicted_rtt']:.2f}ms | Real RTT: {worst_delusion['actual_rtt']:.2f}ms")
				print("      [DEBUG] --------------------------------------------------")

		meas_dict = {}
		for src, dst in self.measurement_history[:budget]:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]
			
		return meas_dict

	def cleanup(self):
		if self.executor:
			self.executor.shutdown(wait=True)