import numpy as np
from scipy.optimize import minimize

from utils import *

class FeasibleRegion:
	"""Tracks the feasible geographic region for a target based on RTT constraints."""
	
	def __init__(self, target_id, prior_guess=(0.0, 0.0)):
		self.target_id = target_id
		self.best_guess = np.array(prior_guess)
		# List of tuples: ((lat, lon), max_radius_km)
		self.constraints = []
		
		# --- CACHE INITIALIZATION ---
		self._cached_region_size = None
		
	def add_measurement(self, vp_loc, min_rtt):
		"""Adds a single speed-of-light constraint and updates the estimate."""
		self._append_constraint(vp_loc, min_rtt)
		self._update_estimate()

	def add_measurements_batch(self, measurements):
		"""Batch adds measurements to avoid re-optimizing after every single addition."""
		for vp_loc, min_rtt in measurements:
			self._append_constraint(vp_loc, min_rtt)
		self._update_estimate()

	def _append_constraint(self, vp_loc, min_rtt):
		# 1ms RTT = ~100km one-way distance in fiber
		max_radius_km = min_rtt * 100.0
		self.constraints.append((vp_loc, max_radius_km))
		
		# --- CACHE INVALIDATION ---
		# Any new constraint modifies the state, so we clear the saved value
		self._cached_region_size = None

	def get_region_size(self):
		"""Estimates the uncertainty radius of the current feasible region."""
		# --- CACHE HIT ---
		if self._cached_region_size is not None:
			return self._cached_region_size

		if not self.constraints:
			return 20037.0 # Half the Earth's circumference (max uncertainty)
			
		centroid = self.get_location()
		tightest_bound = float('inf')
		
		for (src_lat, src_lon), max_radius in self.constraints:
			# Distance from our centroid to the VP
			dist_to_vp = get_distance(centroid, (src_lat, src_lon))
			
			# The remaining distance from the centroid to the edge of this constraint's circle
			dist_to_edge = max_radius - dist_to_vp
			
			if dist_to_edge < tightest_bound:
				tightest_bound = dist_to_edge
				
		# --- CACHE MISS: Compute, store, and return ---
		self._cached_region_size = max(tightest_bound, 0.0)
		return self._cached_region_size
		
	def _update_estimate(self):
		"""Runs Nelder-Mead to find the point that best satisfies all constraints."""
		if not self.constraints:
			return
			
		# --- The Null Island Fix ---
		if len(self.constraints) == 1:
			# If we only have one circle, the best guess is the center of that circle.
			# This snaps the guess off (0,0) and onto the actual landmass.
			self.best_guess = np.array([self.constraints[0][0][0], self.constraints[0][0][1]])
			return
			
		def error_function(point):
			lat, lon = point
			penalty = 0
			for (src_lat, src_lon), max_dist in self.constraints:
				dist = get_distance((lat,lon), (src_lat, src_lon))
				if dist > max_dist:
					penalty += (dist - max_dist) ** 2
				else:
					# Gentle pull to the center of the valid intersection
					penalty += 0.001 * dist
			return penalty

		result = minimize(
			error_function,
			self.best_guess,
			method='Nelder-Mead',
			bounds=[(-90, 90), (-180, 180)],
			tol=1.0,
			options={'maxiter': 200},
		)
		
		self.best_guess = result.x

	def clone(self):
		"""
		Creates a lightning-fast, isolated copy of the region for parallel processing.
		Avoids the massive overhead of copy.deepcopy().
		"""
		# Initialize a blank region
		new_region = FeasibleRegion(self.target_id)
		
		# 1. Copy the numpy array so mutations don't bleed across threads
		new_region.best_guess = self.best_guess.copy()
		
		# 2. Shallow copy the list. (Since it only contains immutable tuples, 
		# appending/popping in the new list won't affect the original)
		new_region.constraints = self.constraints.copy()
		
		# 3. Copy the cached float state
		new_region._cached_region_size = self._cached_region_size
		
		return new_region
		
	def get_location(self):
		"""Returns the current estimated (lat, lon) tuple."""
		return (self.best_guess[0], self.best_guess[1])
		
	def distance_to(self, vp_loc):
		"""Utility for geolocators to easily evaluate distance to the current guess."""
		return get_distance(vp_loc, self.get_location())
