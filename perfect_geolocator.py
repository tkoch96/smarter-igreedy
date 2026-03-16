import geopy.distance, copy
from utils import *
from feasible_region_maintainer import FeasibleRegion

class Perfect_Geolocator:
	"""An oracle that prioritizes measurements based on RTT and spatial diversity to maximally reduce the feasible region."""
	def __init__(self):
		self.name = "smart_perfect"
		self.data = None
		self.measurement_order = []

	def set_data(self, data):
		self.data = data
		self.measurement_order = []
		
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		address_to_loc = self.data.get('address_to_loc', {})
		dst_to_src_rtts = {}
		
		# Group available sources by destination
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in dst_to_src_rtts:
						dst_to_src_rtts[dst] = []
					dst_to_src_rtts[dst].append((src, min(rtts)))
		
		ranked_dst_to_srcs = {}
		
		for dst, srcs in dst_to_src_rtts.items():
		    actual_target_loc = address_to_loc.get(dst)
		    if not actual_target_loc or not srcs:
		        continue

		    # Initialize the region for this target
		    current_region = FeasibleRegion(target_id=dst)
		    selected_srcs = []
		    remaining_srcs = srcs[:] # List of (vp_src, min_rtt)

		    while remaining_srcs:
		        best_idx = -1
		        best_error = float('inf')

		        # The Oracle simulates the future for every single candidate
		        for i, (cand_src, cand_rtt) in enumerate(remaining_srcs):
		            cand_loc = address_to_loc.get(cand_src)
		            if not cand_loc:
		                continue
		                
		            # Clone the region so we don't permanently alter the current state
		            simulated_region = copy.deepcopy(current_region)
		            simulated_region.add_measurement(cand_loc, cand_rtt)
		            
		            # Calculate the literal error of the new estimate against the ground truth
		            simulated_error = get_distance(simulated_region.get_location(), actual_target_loc)

		            if simulated_error < best_error:
		                best_error = simulated_error
		                best_idx = i

		        if best_idx != -1:
		            # Commit the best measurement to our actual region and save it
		            best_src, best_rtt = remaining_srcs.pop(best_idx)
		            current_region.add_measurement(address_to_loc[best_src], best_rtt)
		            selected_srcs.append((best_src, best_rtt))
		        else:
		            break
		            
		    ranked_dst_to_srcs[dst] = selected_srcs

		# Interleave them for the budget:
		# The 1st most valuable measurement for EVERY target is added first, then the 2nd, etc.
		max_srcs_for_a_dst = max((len(srcs) for srcs in ranked_dst_to_srcs.values()), default=0)
		
		for rank in range(max_srcs_for_a_dst):
			for dst, srcs in ranked_dst_to_srcs.items():
				if rank < len(srcs):
					self.measurement_order.append((srcs[rank][0], dst))

	def solve(self):
		pass

	def measurements(self, budget):
		selected = self.measurement_order[:budget]
		
		meas_dict = {}
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		for src, dst in selected:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]
			
		return meas_dict

