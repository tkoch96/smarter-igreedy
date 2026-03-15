import random

class Random_Geolocator:
	def __init__(self):
		self.name = "random"
		self.data = None
		self.measurement_order = []

	def set_data(self, data):
		self.data = data
		self.measurement_order = []
		
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					self.measurement_order.append((src, dst))
					
		# Shuffle to randomize the order of target acquisition
		random.shuffle(self.measurement_order)

	def solve(self):
		pass

	def measurements(self, budget):
		# Grab the allowed number of measurements based on the budget
		selected = self.measurement_order[:budget]
		
		meas_dict = {}
		loc_loc_meas = self.data.get('loc_loc_meas', {})
		for src, dst in selected:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]
			
		return meas_dict