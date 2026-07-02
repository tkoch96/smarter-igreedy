import random
from typing import Any

# {src_subnet: {dst_subnet: [rtt_ms, ...]}}
MeasData = dict[str, dict[str, list[float]]]
TargetData = dict[str, Any]


class Random_Geolocator:
	def __init__(self) -> None:
		self.name = "random"
		self.data: Optional[TargetData] = None
		self.measurement_order: list[tuple[str, str]] = []

	def set_data(self, data: TargetData) -> None:
		self.data = data
		self.measurement_order = []

		loc_loc_meas: MeasData = self.data.get('loc_loc_meas', {})
		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					self.measurement_order.append((src, dst))

		random.shuffle(self.measurement_order)

	def solve(self) -> None:
		pass

	def measurements(self, budget: int) -> MeasData:
		selected = self.measurement_order[:budget]

		meas_dict: MeasData = {}
		loc_loc_meas: MeasData = self.data.get('loc_loc_meas', {})
		for src, dst in selected:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]

		return meas_dict
