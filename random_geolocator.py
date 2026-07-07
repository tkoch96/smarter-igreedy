import random
from typing import Any, Optional

# {src_subnet: {dst_subnet: [rtt_ms, ...]}}
MeasData = dict[str, dict[str, list[float]]]
TargetData = dict[str, Any]


class Random_Geolocator:
	"""Random ping-order baseline.  Estimation is done downstream by the
	comparator's converter; `converter_mode` / `rtt_model` (both optional)
	override the comparator's defaults for THIS instance, so several
	random-selection strategies differing only in estimation can coexist
	in one run (e.g. random+nn vs random+additive vs random+additive_fiber).

	`order_seed`: seed a private RNG for the shuffle so that instances
	with the same seed produce the SAME measurement order — matched
	measurements across estimator variants.  None keeps the legacy
	behavior (global random module)."""

	def __init__(self, name: str = "random", converter_mode: Optional[str] = None,
	             rtt_model=None, order_seed: Optional[int] = None) -> None:
		self.name = name
		self.converter_mode = converter_mode
		self.rtt_model = rtt_model
		self.order_seed = order_seed
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

		# sort first when seeded: dict order is insertion order, and a
		# reproducible shuffle needs a reproducible starting sequence
		if self.order_seed is not None:
			self.measurement_order.sort()
			random.Random(self.order_seed).shuffle(self.measurement_order)
		else:
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
