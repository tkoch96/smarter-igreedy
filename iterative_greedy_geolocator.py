import numpy as np
from utils import get_distance
from feasible_region_maintainer import FeasibleRegion

class Iterative_Greedy_Geolocator:
	def __init__(self):
		self.name = "iterative_greedy"
		self.data = None
		self.vp_locations = {}
		
		# --- STATE VARIABLES ---
		self.measurement_history = []  # Ordered list of executed (src, dst) pairs
		self.target_regions = {}
		self.measurements_used = {}
		self.current_region_sizes = {}
		self.best_vp_cache = {}
		self.available_measurements = {}
		self.targets = []

	def set_data(self, data):
		self.data = data
		self.vp_locations = data.get('address_to_loc', {})

	def get_prior_guess(self, dst):
		return (0.0, 0.0)

	def solve(self):
		"""
		Initializes the state of the geolocator before the budget loop begins.
		This ensures we don't restart from zero every time measurements() is called.
		"""
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

		# Setup initial state for all targets
		self.target_regions = {dst: FeasibleRegion(dst, self.get_prior_guess(dst)) for dst in self.targets}
		self.measurements_used = {dst: set() for dst in self.targets}
		self.current_region_sizes = {dst: 20037.0 for dst in self.targets}
		self.best_vp_cache = {}
		
		# Populate initial cache
		for dst in self.targets:
			self._update_best_vp_for_target(dst)

	def _update_best_vp_for_target(self, dst):
		"""Helper to find and cache the highest utility VP for a specific target."""
		best_src = None
		best_utility = -float('inf')
		
		available_srcs = [s for s in self.available_measurements[dst] if s not in self.measurements_used[dst]]
		
		for src in available_srcs:
			if src in self.vp_locations:
				utility = self.evaluate_vp_utility(self.vp_locations[src], self.target_regions[dst])
				if utility > best_utility:
					best_utility = utility
					best_src = src
		
		self.best_vp_cache[dst] = (best_src, best_utility)

	def evaluate_vp_utility(self, vp_loc, target_region):
		"""
		Evaluates a candidate VP by simulating the actual geometric intersection.
		Forces at least one measurement per target using an artificial penalty.
		"""
		if not target_region.constraints:
			return 1000000.0 
			
		current_guess_loc = target_region.get_location()
		distance_to_guess = get_distance(vp_loc, current_guess_loc)
		
		expected_rtt = (distance_to_guess * 1.5) / 100.0
		max_radius_km = expected_rtt * 100.0
		
		current_size = target_region.get_region_size()
		
		old_guess = target_region.best_guess.copy()
		target_region.constraints.append((vp_loc, max_radius_km))
		target_region._update_estimate()
		
		new_size = target_region.get_region_size()
		
		target_region.constraints.pop()
		target_region.best_guess = old_guess
		
		return current_size - new_size

	def measurements(self, budget):
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		
		# 1. Generate new pings ONLY if our history is smaller than the requested budget
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
				break # We have exhausted all possible measurements in the dataset
				
			# Execute measurement
			self.measurements_used[best_global_dst].add(best_global_src)
			actual_rtts = loc_loc_meas[best_global_src][best_global_dst]
			
			# Add constraint and update the region
			min_actual_rtt = min(actual_rtts)
			self.target_regions[best_global_dst].add_measurement(self.vp_locations[best_global_src], min_actual_rtt)
			
			# Update expected error tracking
			new_actual_size = self.target_regions[best_global_dst].get_region_size()
			self.current_region_sizes[best_global_dst] = new_actual_size
			
			# Recompute cache for the target we just updated
			self._update_best_vp_for_target(best_global_dst)
			
			# Add to our stateful history
			self.measurement_history.append((best_global_src, best_global_dst))

		# 2. Print the internal model's status for the current budget
		if self.targets:
			avg_expected_error = sum(self.current_region_sizes.values()) / len(self.targets)
			actual_pings = min(budget, len(self.measurement_history))
			print(f"   -> [Internal Model] Ping {actual_pings:4d} | Expected Avg Error: {avg_expected_error:.2f} km")
			if actual_pings % 500 == 0: # Print a debug snapshot every 500 pings
				print(f"      [DEBUG] --- Top 5 Worst Estimates at Ping {actual_pings} ---")
				
				# Sort all targets by their current region size, descending
				sorted_targets = sorted(
					self.targets, 
					key=lambda t: self.target_regions[t].get_region_size(), 
					reverse=True
				)
				
				# Grab the 5 with the largest remaining uncertainty
				worst_5_targets = sorted_targets[:5]
				
				for target in worst_5_targets:
					region = self.target_regions[target]
					print(f"      [DEBUG] Target {target}:")
					print(f"      [DEBUG]   Centroid Guess: {region.get_location()}")
					print(f"      [DEBUG]   Region Size: {region.get_region_size():.2f} km")
					
					# Look at the utility of the absolute best remaining VP for this target
					best_vp, max_util = self.best_vp_cache[target]
					print(f"      [DEBUG]   Best remaining VP utility: {max_util:.2f}")
					
					# Look at the actual physical constraints we've applied
					if region.constraints:
						tightest = min(c[1] for c in region.constraints)
						loosest = max(c[1] for c in region.constraints)
						print(f"      [DEBUG]   Tightest constraint: {tightest:.2f} km | Loosest: {loosest:.2f} km")
					else:
						print("      [DEBUG]   No constraints applied yet (0 pings).")
						
				print("      [DEBUG] --------------------------------------------------")
	
		# 3. Construct the output dictionary using exactly `budget` amount of history
		meas_dict = {}
		for src, dst in self.measurement_history[:budget]:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]
			
		return meas_dict
