import numpy as np
import geopy.distance
from scipy.optimize import minimize

class FeasibleRegion:
	"""Tracks the feasible geographic region for a target based on RTT constraints."""
	
	def __init__(self, target_id, prior_guess=(0.0, 0.0)):
		self.target_id = target_id
		self.best_guess = np.array(prior_guess)
		# List of tuples: ((lat, lon), max_radius_km)
		self.constraints = [] 
		
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
		
	def _update_estimate(self):
		"""Runs Nelder-Mead to find the point that best satisfies all constraints."""
		if not self.constraints:
			return
			
		def error_function(point):
			lat, lon = point
			penalty = 0
			for (src_lat, src_lon), max_dist in self.constraints:
				dist = geopy.distance.geodesic((lat, lon), (src_lat, src_lon)).km
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
			bounds=[(-90, 90), (-180, 180)]
		)
		
		self.best_guess = result.x
		
	def get_location(self):
		"""Returns the current estimated (lat, lon) tuple."""
		return (self.best_guess[0], self.best_guess[1])
		
	def distance_to(self, vp_loc):
		"""Utility for geolocators to easily evaluate distance to the current guess."""
		return geopy.distance.geodesic(vp_loc, self.get_location()).km
