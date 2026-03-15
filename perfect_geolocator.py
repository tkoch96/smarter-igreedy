class Perfect_Geolocator:
	"""An oracle that perfectly prioritizes the most valuable measurements."""
	def __init__(self):
		self.name = "perfect"
		self.data = None
		self.measurement_order = []

	def set_data(self, data):
		self.data = data
		self.measurement_order = []
		
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		dst_to_src_rtts = {}
		
		# Invert to group available sources by destination
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in dst_to_src_rtts:
						dst_to_src_rtts[dst] = []
					dst_to_src_rtts[dst].append((src, min(rtts)))
		
		# Sort sources for each destination by lowest RTT (closest physical proximity)
		for dst in dst_to_src_rtts:
			dst_to_src_rtts[dst].sort(key=lambda x: x[1])

		# Interleave them: 
		# The 1st most valuable measurement for EVERY target is added first.
		# Then the 2nd most valuable, etc.
		max_srcs_for_a_dst = max((len(srcs) for srcs in dst_to_src_rtts.values()), default=0)
		
		for rank in range(max_srcs_for_a_dst):
			for dst, srcs in dst_to_src_rtts.items():
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