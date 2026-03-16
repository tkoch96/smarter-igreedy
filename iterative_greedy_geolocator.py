import numpy as np
from scipy.optimize import minimize
from utils import get_distance
from feasible_region_maintainer import FeasibleRegion

class Iterative_Greedy_Geolocator:
	def __init__(self):
		self.name = "iterative_greedy"
		self.data = None
		self.vp_locations = {}

	def set_data(self, data):
		self.data = data
		self.vp_locations = data.get('address_to_loc', {})

	def solve(self):
		# State management for online probing is handled in measurements()
		pass

	def get_prior_guess(self, dst):
		"""
		Placeholder for your prior (e.g., GeoIP database centroid).
		Defaulting to Null Island (0,0) for the skeleton.
		"""
		return (0.0, 0.0)

	def expected_latency(self, vp_loc, target_guess_loc):
		"""
		Model for expected latency based on current guess.
		"""
		distance_km = get_distance(vp_loc, target_guess_loc)
		# Fiber inefficiency factor of 1.3, assuming 100km per 1ms in straight fiber
		expected_rtt_ms = (distance_km * 1.3) / 100.0
		return expected_rtt_ms

	def evaluate_vp_utility(self, vp_loc, target_region):
		"""
		Evaluates a candidate VP based on the target's current FeasibleRegion.
		Maximizes expected constraint tightness while penalizing redundancy.
		"""
		current_guess_loc = target_region.get_location()
		distance_to_guess = get_distance(vp_loc, current_guess_loc)
		
		# 1. Expected Bounding Circle (Smaller is better)
		# Closer VPs are expected to return lower RTTs, resulting in tighter constraint circles.
		expected_rtt = (distance_to_guess * 1.5) / 100.0  
		
		# 2. Spatial Diversity against Existing Constraints
		diversity_score = 0
		existing_constraint_centers = [c[0] for c in target_region.constraints]
		
		if existing_constraint_centers:
			# Find how close this candidate is to VPs we've already measured from
			distances_to_used = [get_distance(vp_loc, used_loc) for used_loc in existing_constraint_centers]
			min_dist_to_used = min(distances_to_used)
			
			# Heavy penalty if we already have a constraint from this exact area
			if min_dist_to_used < 500:
				diversity_score = -1000  
			else:
				# Reward providing a constraint from a new angle/distance.
				# Capped so we don't just pick VPs on the other side of the planet.
				diversity_score = min(min_dist_to_used, 3000) * 0.1  

		# Maximize diversity, minimize expected RTT
		utility = diversity_score - expected_rtt
		return utility

	def measurements(self, budget):
		meas_dict = {}
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		
		# Build list of available (src, dst) pairs
		available_measurements = {} 
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in available_measurements:
						available_measurements[dst] = []
					available_measurements[dst].append(src)
		
		targets = list(available_measurements.keys())
		if not targets:
			return meas_dict

		# State tracking entirely delegated to FeasibleRegion
		target_regions = {dst: FeasibleRegion(dst, self.get_prior_guess(dst)) for dst in targets}
		
		# Track which sources we've actually executed to avoid pinging twice
		measurements_used = {dst: set() for dst in targets}
		
		pings_allocated = 0
		target_idx = 0
		
		while pings_allocated < budget:
			dst = targets[target_idx % len(targets)]
			
			available_srcs = [s for s in available_measurements[dst] if s not in measurements_used[dst]]
			
			if not available_srcs:
				target_idx += 1
				if all(len(measurements_used[t]) == len(available_measurements[t]) for t in targets):
					break 
				continue

			# 1. Rank available VPs by their utility against our CURRENT region
			best_src = None
			best_utility = -float('inf')
			
			for src in available_srcs:
				if src in self.vp_locations:
					vp_loc = self.vp_locations[src]
					
					# Pass the whole region object to the utility function
					utility = self.evaluate_vp_utility(vp_loc, target_regions[dst])
					
					if utility > best_utility:
						best_utility = utility
						best_src = src
			
			if best_src is None:
				best_src = available_srcs[0]

			# 2. Execute the measurement
			measurements_used[dst].add(best_src)
			actual_rtts = loc_loc_meas[best_src][dst]
			
			if best_src not in meas_dict:
				meas_dict[best_src] = {}
			meas_dict[best_src][dst] = actual_rtts
			pings_allocated += 1
			
			# 3. Add the measurement to the region (which auto-updates the guess)
			if best_src in self.vp_locations:
				min_actual_rtt = min(actual_rtts)
				target_regions[dst].add_measurement(self.vp_locations[best_src], min_actual_rtt)

			target_idx += 1

		return meas_dict

		