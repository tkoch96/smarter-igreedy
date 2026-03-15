import numpy as np
from scipy.optimize import minimize
from utils import get_distance

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

	def evaluate_vp_utility(self, vp_loc, current_guess_loc, used_vp_locs):
		"""
		Calculates utility based on expected tightness of the bounding circle,
		penalized by how redundant the measurement is geographically.
		"""
		distance_to_guess = get_distance(vp_loc, current_guess_loc)
		
		# 1. Expected Latency Penalty (closer to guess is better)
		expected_rtt = (distance_to_guess * 1.5) / 100.0 
		
		# 2. Geographic Diversity Reward (further from used VPs is better)
		diversity_score = 0
		if used_vp_locs:
			# Find the distance to the CLOSEST VP we've already used
			distances_to_used = [get_distance(vp_loc, used_loc) for used_loc in used_vp_locs]
			min_dist_to_used = min(distances_to_used)
			
			# If the VP is within 500km of one we already used, heavily penalize it.
			# If it's further away, it provides a good cross-section for triangulation.
			if min_dist_to_used < 500:
				diversity_score = -1000  # "We already pinged from this area, skip it"
			else:
				# Diminishing returns on being super far away from other probes
				diversity_score = min(min_dist_to_used, 3000) * 0.1 

		# We want to minimize expected RTT, but maximize diversity.
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

		# State tracking for the online learning process
		current_guesses = {dst: self.get_prior_guess(dst) for dst in targets}
		measurements_used = {dst: set() for dst in targets}
		
		# Keep track of the physical constraints (VP location, max radius in km) discovered so far
		constraints_history = {dst: [] for dst in targets}

		pings_allocated = 0
		target_idx = 0
		
		while pings_allocated < budget:
			dst = targets[target_idx % len(targets)]
			
			# Find VPs we haven't used for this target yet
			available_srcs = [s for s in available_measurements[dst] if s not in measurements_used[dst]]
			
			if not available_srcs:
				target_idx += 1
				if all(len(measurements_used[t]) == len(available_measurements[t]) for t in targets):
					break # All targets exhausted
				continue

			# 1. Rank available VPs by their utility against our current guess
			best_src = None
			best_utility = -float('inf')
			
			# Get the actual coordinates of the VPs we've already used for this target
			used_vp_locs = [self.vp_locations[s] for s in measurements_used[dst] if s in self.vp_locations]
			
			for src in available_srcs:
				if src in self.vp_locations:
					vp_loc = self.vp_locations[src]
					
					# Pass the used_vp_locs into the new utility function
					utility = self.evaluate_vp_utility(vp_loc, current_guesses[dst], used_vp_locs)
					
					if utility > best_utility:
						best_utility = utility
						best_src = src
			
			if best_src is None:
				best_src = available_srcs[0]

			# 2. "Execute" the measurement
			measurements_used[dst].add(best_src)
			actual_rtts = loc_loc_meas[best_src][dst]
			
			if best_src not in meas_dict:
				meas_dict[best_src] = {}
			meas_dict[best_src][dst] = actual_rtts
			pings_allocated += 1
			
			# 3. Incorporate new data to update the "Current Guess" (The ML / CBG Estimator)
			if best_src in self.vp_locations:
				vp_loc = self.vp_locations[best_src]
				min_actual_rtt = min(actual_rtts)
				max_radius_km = min_actual_rtt * 100.0 # Speed of light in fiber constraint
				
				# Add new constraint to history
				constraints_history[dst].append((vp_loc, max_radius_km))
				
				# Optimization Objective: Minimize distance violations for all known constraints
				def error_function(point):
					lat, lon = point
					penalty = 0
					for (src_lat, src_lon), max_dist in constraints_history[dst]:
						dist = get_distance((lat, lon), (src_lat, src_lon))
						if dist > max_dist:
							penalty += (dist - max_dist) ** 2
						else:
							# Gentle pull to the center of the valid intersection
							penalty += 0.001 * dist 
					return penalty

				# Use the previous guess as the starting point so the optimizer converges quickly
				initial_guess = np.array(current_guesses[dst])
				
				result = minimize(
					error_function, 
					initial_guess, 
					method='Nelder-Mead',
					bounds=[(-90, 90), (-180, 180)]
				)
				
				# Update our belief of where the target is
				current_guesses[dst] = (result.x[0], result.x[1])

			target_idx += 1

		return meas_dict

		