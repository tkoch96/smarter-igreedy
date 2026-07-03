import numpy as np
import multiprocessing
import time
import os
import pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Callable, Optional

from utils import LatLon, get_distance
from feasible_region_maintainer import FeasibleRegion, HARD_CIRCLE, ADDITIVE, DEFAULT_SLOPE, TARGET_OF_INTEREST
from probabilistic_helpers import (
	AdditiveLatencyModel, ADDITIVE_PRIOR_VAR_MS2, additive_batch_em,
)

DEBUG = False

TargetData = dict[str, Any]
MeasData = dict[str, dict[str, list[float]]]
# (best_src, utility_score)
VPCacheEntry = tuple[Optional[str], float]


class AdaptiveRTTModel:
	def __init__(self, alpha: float = 0.3) -> None:
		# 0.3 means 30% weight to the newest ping, 70% to history.
		self.alpha = alpha
		self.target_errors: dict[str, float] = {}
		self.debug = DEBUG

	def __call__(self, vp_loc: LatLon, target_region: FeasibleRegion, dst: Optional[str] = None) -> float:
		current_guess_loc = target_region.get_location()
		distance_to_guess = get_distance(vp_loc, current_guess_loc)
		base_rtt = distance_to_guess * DEFAULT_SLOPE / 100.0

		error_inflation = 0.0
		if dst is not None and dst in self.target_errors:
			error_inflation = self.target_errors[dst]

		return max(0.01, base_rtt + error_inflation)

	def update_error(self, dst: str, predicted_rtt: float, actual_rtt: float) -> None:
		"""Updates the moving average of the error for a given target."""
		current_error = actual_rtt - predicted_rtt
		if dst not in self.target_errors:
			self.target_errors[dst] = current_error
		else:
			self.target_errors[dst] = (self.alpha * current_error) + ((1 - self.alpha) * self.target_errors[dst])
		if self.debug:
			print("Current error for {} is now {}".format(dst, self.target_errors[dst]))


BASICALLY_GEOLOCATED = 200  # km, we've essentially geolocated this IP address


def default_expected_rtt_model(vp_loc: LatLon, target_region: FeasibleRegion) -> float:
	"""Default geometric RTT estimation."""
	current_guess_loc = target_region.get_location()
	distance_to_guess = get_distance(vp_loc, current_guess_loc)
	return distance_to_guess * DEFAULT_SLOPE / 100.0


def default_utility_evaluator(
	vp: str,
	dst: str,
	target_region: FeasibleRegion,
	vp_loc: LatLon,
	current_size: float,
	rtt_model_func: Callable,
	verb: bool,
) -> float:
	if current_size < BASICALLY_GEOLOCATED:
		# "Done" targets are deprioritised, not abandoned: the offset ranks
		# them below every unfinished target, but leftover budget still
		# flows to the least-certain done target (larger size first) via
		# its nearest VP. Region size is an optimistic proxy for true
		# error, so free refinement is never wasted.
		distance = get_distance(vp_loc, target_region.get_location())
		return -1000000.0 + current_size + 1.0 / (distance + 1.0)

	expected_rtt = rtt_model_func(vp_loc, target_region, dst)

	temp_region = target_region.clone()
	temp_region.add_measurement(vp_loc, expected_rtt)
	new_size = temp_region.get_region_size()
	area_reduction = current_size - new_size

	if area_reduction <= 0.001:
		distance = get_distance(vp_loc, target_region.get_location())
		return 1.0 / (distance + 1.0)

	return area_reduction


def additive_utility_evaluator(
	vp: str,
	dst: str,
	target_region: FeasibleRegion,
	vp_loc: LatLon,
	current_size: float,
	rtt_model_func: Callable,
	verb: bool,
) -> float:
	"""
	Utility for additive-mode regions.  Same structure as the default
	evaluator, but the expected RTT comes from the shared additive model
	(d/100 + μ̂_s + μ̂_t instead of slope × d/100), and the simulated
	constraint carries the candidate's src id so the clone weights it by
	1/(σ̂_s² + σ̂_t²).

	σ̂_dst enters twice.  Implicitly: the simulated ping's precision weight
	means it cannot fake a fit improvement on a noisy target.  Explicitly:
	the raw km reduction is discounted by a trust factor
	prior_var / (prior_var + σ̂_t²).  Without the discount a pathological
	target actually OUTBIDS finished ones — its statistical floor is huge,
	so one more ping promises a large absolute reduction (averaging does
	help under gaussian noise) — but that promise scales with σ̂_t exactly
	when the model deserves the least trust (real pathological routing is
	structured, not averaging-friendly).  The discount makes expected gain
	shrink as σ̂_t grows, which is what redirects budget away from
	hopeless targets.
	"""
	if current_size < BASICALLY_GEOLOCATED:
		distance = get_distance(vp_loc, target_region.get_location())
		return -1000000.0 + current_size + 1.0 / (distance + 1.0)

	dist_km = get_distance(vp_loc, target_region.get_location())
	expected_rtt, _var = target_region.model.predict(vp, dst, dist_km)

	temp_region = target_region.clone()
	temp_region.add_measurement(vp_loc, expected_rtt, src=vp)
	new_size = temp_region.get_region_size()
	area_reduction = current_size - new_size

	if area_reduction <= 0.001:
		return 1.0 / (dist_km + 1.0)

	var_t = target_region.model.var_t.get(dst, ADDITIVE_PRIOR_VAR_MS2)
	trust = ADDITIVE_PRIOR_VAR_MS2 / (ADDITIVE_PRIOR_VAR_MS2 + var_t)
	return area_reduction * trust


def _evaluate_vp_worker(
	vp: str,
	dst: str,
	target_region: FeasibleRegion,
	vp_loc: LatLon,
	current_size: float,
	utility_func: Callable,
	rtt_func: Callable,
	verb: bool,
) -> tuple[str, float]:
	if not target_region.constraints:
		return vp, 1000000.0

	utility_score = utility_func(vp, dst, target_region, vp_loc, current_size, rtt_func, verb)
	return vp, utility_score


class Iterative_Greedy_Geolocator:
	def __init__(
		self,
		max_workers: Optional[int] = None,
		utility_func: Optional[Callable] = None,
		rtt_func: Optional[Callable] = None,
		region_mode: str = HARD_CIRCLE,
		region_slope: float = DEFAULT_SLOPE,
		name: Optional[str] = None,
		model_refit_every: int = 1,
	) -> None:
		# Distinct names let several differently-configured greedys coexist
		# in one Geolocator_Comparator run (plot keys / cache filenames).
		self.name = name or "iterative_greedy"
		self.data: Optional[TargetData] = None
		self.vp_locations: dict[str, LatLon] = {}
		self.debug = DEBUG
		# Overlap methodology used for this greedy's own regions (selection
		# utility AND its reported estimates): HARD_CIRCLE or GAUSSIAN, with
		# a shared predictive slope (expected rtt = slope × d / 100).
		# get_region_size() returns km-equivalents in both modes, so
		# BASICALLY_GEOLOCATED and the size sentinels apply uniformly.
		self.region_mode = region_mode
		self.region_slope = region_slope
		# ADDITIVE mode: one shared AdditiveLatencyModel across all target
		# regions (X_src pools over targets), refit from all accumulated
		# measurements every `model_refit_every` actual pings.
		self.latency_model: Optional[AdditiveLatencyModel] = None
		self.model_refit_every = model_refit_every

		if utility_func is not None:
			self.utility_func: Callable = utility_func
		elif region_mode == ADDITIVE:
			self.utility_func = additive_utility_evaluator
		else:
			self.utility_func = default_utility_evaluator
		self.rtt_func: Callable = rtt_func or AdaptiveRTTModel()

		if max_workers is None:
			max_workers = multiprocessing.cpu_count()
		self.max_workers = max_workers
		self.executor = ProcessPoolExecutor(max_workers=self.max_workers)

		self.measurement_history: list[tuple[str, str]] = []
		self.target_regions: dict[str, FeasibleRegion] = {}
		self.measurements_used: dict[str, set[str]] = {}
		self.current_region_sizes: dict[str, float] = {}
		self.best_vp_cache: dict[str, VPCacheEntry] = {}

		self.iter = 0

		self.available_measurements: dict[str, list[str]] = {}
		self.targets: list[str] = []
		self.utility_tracking: list[dict[str, Any]] = []

	def set_data(self, data: TargetData) -> None:
		self.data = data
		self.vp_locations = data.get('address_to_loc', {})

	def get_prior_guess(self, dst: str) -> LatLon:
		return (0.0, 0.0)

	def get_current_estimates(self) -> dict[str, LatLon]:
		"""Returns the live, already-calculated locations to save time."""
		return {
			dst: region.get_location()
			for dst, region in self.target_regions.items()
			if region.constraints
		}

	def solve(self) -> None:
		self.measurement_history = []
		loc_loc_meas: MeasData = self.data.get('loc_loc_meas', {})
		self.available_measurements = {}

		for src, dsts in loc_loc_meas.items():
			for dst, rtts in dsts.items():
				if rtts:
					if dst not in self.available_measurements:
						self.available_measurements[dst] = []
					self.available_measurements[dst].append(src)

		self.targets = list(self.available_measurements.keys())
		if not self.targets:
			return

		if self.region_mode == ADDITIVE:
			self.latency_model = AdditiveLatencyModel()   # fresh per solve
		self.target_regions = {
			dst: FeasibleRegion(dst, self.get_prior_guess(dst),
			                    mode=self.region_mode, slope=self.region_slope,
			                    model=self.latency_model)
			for dst in self.targets
		}
		self.measurements_used = {dst: set() for dst in self.targets}
		self.current_region_sizes = {dst: 20037.0 for dst in self.targets}
		self.best_vp_cache = {}

		for dst in self.targets:
			self._update_best_vp_for_target(dst)

	def _update_best_vp_for_target(self, dst: str) -> None:
		best_src: Optional[str] = None
		best_utility = -float('inf')

		try:
			target_region = self.target_regions[dst]
		except KeyError:
			return

		available_srcs = [s for s in self.available_measurements[dst] if s not in self.measurements_used.get(dst, [])]

		if not available_srcs:
			self.best_vp_cache[dst] = (None, -float('inf'))
			return

		try:
			target_region = self.target_regions[dst]
		except KeyError:
			return
		current_size = target_region.get_region_size()
		verbs = [np.random.random() > .999 and self.iter > len(self.targets) for s in available_srcs]

		futures = [
			self.executor.submit(
				_evaluate_vp_worker,
				src,
				dst,
				target_region,
				self.vp_locations[src],
				current_size,
				self.utility_func,
				self.rtt_func,
				v,
			)
			for src, v in zip(available_srcs, verbs)
		]

		for future, v in zip(as_completed(futures), verbs):
			src, utility = future.result()
			if utility > best_utility:
				best_utility = utility
				best_src = src

		self.best_vp_cache[dst] = (best_src, best_utility)

	def measurements(self, budget: int, focus_batch_size: int = 500, pings_per_batch: int = 50) -> MeasData:
		loc_loc_meas: MeasData = self.data.get('loc_loc_meas', {})

		pings_in_current_batch = 0
		focus_group: list[str] = []

		while len(self.measurement_history) < budget:
			self.iter = len(self.measurement_history)
			focus_group_refreshed = False
			if pings_in_current_batch == 0 or not focus_group:
				sorted_cache = sorted(
					self.best_vp_cache.items(),
					key=lambda item: item[1][1] if item[1][0] is not None else -float('inf'),
					reverse=True,
				)
				focus_group = [item[0] for item in sorted_cache[:focus_batch_size]]
				pings_in_current_batch = 0
				focus_group_refreshed = True

			best_global_dst: Optional[str] = None
			best_global_src: Optional[str] = None
			best_global_utility = -float('inf')

			for dst in focus_group:
				src, utility = self.best_vp_cache.get(dst, (None, -float('inf')))
				if src is not None and utility > best_global_utility:
					best_global_utility = utility
					best_global_dst = dst
					best_global_src = src

			if best_global_dst is None:
				if focus_group_refreshed:
					# Even a fresh scan of the whole cache found no candidate:
					# every target is either geolocated (dropped from the
					# cache) or out of unused VPs. No useful ping remains, so
					# return what we have instead of spinning forever.
					break
				focus_group = []
				continue

			size_before = self.current_region_sizes[best_global_dst]
			expected_utility = best_global_utility

			self.measurements_used[best_global_dst].add(best_global_src)
			actual_rtts: list[float] = loc_loc_meas[best_global_src][best_global_dst]

			min_actual_rtt = min(actual_rtts)
			est_before = (self.target_regions[best_global_dst].get_location()
			              if self.target_regions[best_global_dst].constraints else None)
			if best_global_dst == TARGET_OF_INTEREST:
				print(len(self.target_regions[best_global_dst].constraints))
			# Additive mode defers the location step: the parameter refit
			# below must compute residuals against the PRE-ping estimate.
			# Updating the location first lets it absorb the pair's offset
			# into distance, zeroing the residuals the refit needs — μ̂_t
			# collapses into the wrong-fixed-point failure (params-first,
			# the pitfall documented in run_additive_em).
			self.target_regions[best_global_dst].add_measurement(
				self.vp_locations[best_global_src], min_actual_rtt,
				src=best_global_src,
				update_estimate=(self.latency_model is None))
			if best_global_dst == TARGET_OF_INTEREST:
				print(len(self.target_regions[best_global_dst].constraints))

			if self.latency_model is not None:
				# Shared-model bookkeeping: record the sample, refit the
				# per-node (μ, σ²) from every accumulated measurement against
				# the current (pre-ping for this target) estimates, THEN run
				# the pinged region's MAP under the new fit.
				self.latency_model.record(best_global_src, best_global_dst, actual_rtts)
				if len(self.measurement_history) % self.model_refit_every == 0:
					self.latency_model.refit(self.vp_locations,
					                         self.get_current_estimates())
				self.target_regions[best_global_dst].reoptimize()

			new_actual_size = self.target_regions[best_global_dst].get_region_size()
			actual_utility = size_before - new_actual_size

			predicted_rtt_used = self.rtt_func(
				self.vp_locations[best_global_src],
				self.target_regions[best_global_dst],
			)

			if hasattr(self.rtt_func, 'update_error'):
				self.rtt_func.update_error(best_global_dst, predicted_rtt_used, min_actual_rtt)

			self.current_region_sizes[best_global_dst] = new_actual_size

			row = {
				'ping_num': len(self.measurement_history) + 1,
				'target': best_global_dst,
				'src': best_global_src,
				'expected_util': expected_utility,
				'actual_util': actual_utility,
				'error': expected_utility - actual_utility,
				'predicted_rtt': predicted_rtt_used,
				'actual_rtt': min_actual_rtt,
			}
			if self.latency_model is not None:
				# The additive greedy's full belief state at selection time,
				# for post-hoc debugging (a driver with ground truth joins
				# these rows to see where beliefs went wrong; truth itself
				# never enters here).
				model = self.latency_model
				dist_before = (get_distance(self.vp_locations[best_global_src], est_before)
				               if est_before is not None else 0.0)
				model_pred_rtt, model_pred_var = model.predict(
					best_global_src, best_global_dst, dist_before)
				var_t = model.var_t.get(best_global_dst, ADDITIVE_PRIOR_VAR_MS2)
				row.update({
					'est_before': est_before,
					'est_after': self.target_regions[best_global_dst].get_location(),
					'size_before': size_before,
					'size_after': new_actual_size,
					'sigma_dst': model.sigma_dst(best_global_dst),
					'trust': ADDITIVE_PRIOR_VAR_MS2 / (ADDITIVE_PRIOR_VAR_MS2 + var_t),
					'mean_offset': model.mean_offset(best_global_src, best_global_dst),
					'model_pred_rtt': model_pred_rtt,
					'model_residual': min_actual_rtt - model_pred_rtt,
				})
				if os.environ.get('ADDITIVE_GREEDY_DEBUG'):
					print(f"[add-dbg] #{row['ping_num']:4d} {best_global_dst} <- {best_global_src}  "
					      f"exp_util={expected_utility:9.1f} act_util={actual_utility:9.1f}  "
					      f"size {size_before:7.0f}->{new_actual_size:7.0f}km  "
					      f"sig_t={row['sigma_dst']:6.2f} trust={row['trust']:.2f}  "
					      f"pred={model_pred_rtt:7.2f}ms act={min_actual_rtt:7.2f}ms "
					      f"resid={row['model_residual']:+8.2f}ms", flush=True)
			self.utility_tracking.append(row)

			self._update_best_vp_for_target(best_global_dst)
			self.measurement_history.append((best_global_src, best_global_dst))

			pings_in_current_batch += 1
			if pings_in_current_batch >= pings_per_batch:
				pings_in_current_batch = 0

			if self.iter > len(self.targets) and self.debug:
				target_guess = self.target_regions[best_global_dst].get_location()
				vp_loc = self.vp_locations[best_global_src]
				dist_km = get_distance(vp_loc, target_guess)
				predicted_rtt = self.rtt_func(vp_loc, self.target_regions[best_global_dst], dst=best_global_dst)
				simulated_radius = predicted_rtt * 100.0

				print("\n" + "="*70)
				print(f"🕵️  DEEP DIVE: Ping {self.iter}")
				print(f"Target: {best_global_dst} | Source VP: {best_global_src}")
				print("-" * 70)
				print("--- (a) WHY IT WAS SELECTED (The Expectation) ---")
				print(f"Target Current Guess (Lat/Lon) : {target_guess[0]:.4f}, {target_guess[1]:.4f}")
				print(f"VP Location (Lat/Lon)          : {vp_loc[0]:.4f}, {vp_loc[1]:.4f}")
				print(f"Distance (VP to Guess)         : {dist_km:.2f} km")
				print(f"Current Region Size            : {size_before:.2f} km^2")
				print(f"Algorithm's Expected RTT       : {predicted_rtt:.2f} ms")
				print(f"Simulated Constraint Radius    : {simulated_radius:.2f} km")
				print(f"--> Expected Area Reduction    : {expected_utility:.2f} km^2")

				actual_radius = min_actual_rtt * 100.0
				print("--- (b) WHAT IT ACTUALLY DID (The Reality) ---")
				print(f"Actual RTT Measured            : {min_actual_rtt:.2f} ms")
				print(f"Actual Constraint Radius       : {actual_radius:.2f} km")
				print(f"New Region Size                : {new_actual_size:.2f} km^2")
				print(f"--> Actual Area Reduction      : {actual_utility:.2f} km^2")
				print("="*70 + "\n")
				if np.random.random() > .99:
					exit(0)

		if self.latency_model is not None and self.latency_model.rtts_by_pair:
			# Estimation polish before estimates are read off: a fresh
			# params-first batch fit over everything measured so far (the
			# same estimator the additive_em converter uses).  The greedy's
			# incremental per-ping updates are good enough to DRIVE
			# SELECTION but ratchet offsets into distance over a run; the
			# NN-anchored batch alternation recovers them (see
			# additive_batch_em).  The fitted params also reseed the shared
			# model, so subsequent selection benefits.
			estimates, mu_s, var_s, mu_t, var_t = additive_batch_em(
				self.latency_model.rtts_by_pair, self.vp_locations)
			self.latency_model.mu_s, self.latency_model.var_s = mu_s, var_s
			self.latency_model.mu_t, self.latency_model.var_t = mu_t, var_t
			for dst, est in estimates.items():
				region = self.target_regions.get(dst)
				if region is not None and region.constraints:
					region.set_location(est)

		meas_dict: MeasData = {}
		for src, dst in self.measurement_history[:budget]:
			if src not in meas_dict:
				meas_dict[src] = {}
			meas_dict[src][dst] = loc_loc_meas[src][dst]

		return meas_dict

	def cleanup(self) -> None:
		if self.executor:
			self.executor.shutdown(wait=True)
