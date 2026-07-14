import copy, tqdm
from typing import Any, Optional

from utils import LatLon, get_distance
from feasible_region_maintainer import FeasibleRegion

TargetData = dict[str, Any]
MeasData = dict[str, dict[str, list[float]]]


class Perfect_Geolocator:
	"""An oracle that prioritizes measurements based on RTT and spatial diversity to maximally reduce the feasible region."""

	def __init__(self, converter_mode: str = 'nearest_neighbor') -> None:
		self.name = "smart_perfect"
		self.data: Optional[TargetData] = None
		self.measurement_order: list[tuple[str, str]] = []
		# number of things to consider per target as a measurement to. Limits complexity to O(N)
		self.n_srcs_to_consider = 50
		# Estimation half (read by the harness's _converter_mode_for).
		# nearest_neighbor is the measured default: on the real merged mesh
		# (100x300 healthy world, b=2500, oracle selection) it beat every
		# model-based converter — NN 2382/1208 km < additive_em 2604/1518
		# < hard_circle 2906/1540 < em_gaussian 4222/3036 < gaussian
		# 4619/3586.  Fixed-slope circle models (hard OR soft) are
		# misspecified on real paths (median rtt ~2.3x geodesic time);
		# hard_circle additionally DEGRADES as measurements accumulate
		# (1147 -> 1540 median from b=1000 to 2500): each rtt that beats
		# the slope-implied distance is a circle excluding the truth.
		self.converter_mode = converter_mode

	def set_data(self, data: TargetData) -> None:
		self.data = data
		self.measurement_order = []

		loc_loc_meas: MeasData = self.data.get('loc_loc_meas', {})
		address_to_loc: dict[str, LatLon] = self.data.get('address_to_loc', {})
		dst_to_src_rtts: dict[str, list[tuple[str, float]]] = {}

		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in dst_to_src_rtts:
						dst_to_src_rtts[dst] = []
					dst_to_src_rtts[dst].append((src, min(rtts)))

		ranked_dst_to_srcs: dict[str, list[tuple[str, float]]] = {}

		for dst, srcs in tqdm.tqdm(dst_to_src_rtts.items(), desc="Picking best oracle measurements..."):
			actual_target_loc = address_to_loc.get(dst)
			if not actual_target_loc or not srcs:
				continue

			current_region = FeasibleRegion(target_id=dst)
			selected_srcs: list[tuple[str, float]] = []
			remaining_srcs: list[tuple[str, float]] = sorted(srcs, key=lambda x: x[1])[:self.n_srcs_to_consider]

			while remaining_srcs:
				best_idx = -1
				best_error = float('inf')

				for i, (cand_src, cand_rtt) in enumerate(remaining_srcs):
					cand_loc = address_to_loc.get(cand_src)
					if not cand_loc:
						continue

					old_guess = current_region.best_guess.copy()
					# Simulate with the SAME radius add_measurement would
					# commit (implied distance at the model slope × safety
					# multiplier) — a bare rtt×100 radius scored candidates
					# against ~27% looser circles than the ones kept.
					max_radius_km = (current_region.implied_distance_km(cand_rtt)
					                 * current_region.radius_multiplier)
					current_region.constraints.append((cand_loc, max_radius_km))
					current_region._update_estimate()

					simulated_error = get_distance(current_region.get_location(), actual_target_loc)

					current_region.constraints.pop()
					current_region.best_guess = old_guess

					if simulated_error < best_error:
						best_error = simulated_error
						best_idx = i

				if best_idx != -1:
					best_src, best_rtt = remaining_srcs.pop(best_idx)
					current_region.add_measurement(address_to_loc[best_src], best_rtt)
					selected_srcs.append((best_src, best_rtt))
				else:
					break

			ranked_dst_to_srcs[dst] = selected_srcs

		max_srcs_for_a_dst = max((len(srcs) for srcs in ranked_dst_to_srcs.values()), default=0)

		for rank in range(max_srcs_for_a_dst):
			for dst, srcs in ranked_dst_to_srcs.items():
				if rank < len(srcs):
					self.measurement_order.append((srcs[rank][0], dst))

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
