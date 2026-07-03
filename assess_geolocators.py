import multiprocessing
import traceback
import numpy as np, pickle, os
from scipy.optimize import minimize
from utils import *
from perfect_geolocator import Perfect_Geolocator
from pull_ripe_atlas_measurement_data import RipeAtlasPipeline
from random_geolocator import Random_Geolocator
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from feasible_region_maintainer import FeasibleRegion, HARD_CIRCLE, GAUSSIAN, EM_GAUSSIAN, ADDITIVE
from probabilistic_helpers import (
	GLOBAL_SIGMA_MS, GAUSSIAN_NOISE, ASYMMETRIC_NOISE, additive_batch_em,
)

from plot_results import *

def convert_measurements(measurements, target_data, mode):
	"""Module-level (picklable, comparator-free) estimation of measured
	targets — see Geolocator_Comparator.convert_measurements_to_locations
	for the semantics of each mode."""
	address_to_loc = target_data.get('address_to_loc', {})

	if mode == 'additive_em':
		pairs = {}
		for src, dsts in measurements.items():
			for dst, rtts in dsts.items():
				if rtts:
					pairs[(src, dst)] = [float(r) for r in rtts]
		estimates, _, _, _, _ = additive_batch_em(pairs, address_to_loc)
		return estimates

	estimated_locations = {}
	dst_to_src_rtts = {}
	for src, dsts in measurements.items():
		for dst, rtts in dsts.items():
			if not rtts:
				continue
			dst_to_src_rtts.setdefault(dst, {})[src] = min(rtts)

	for dst, src_rtts in dst_to_src_rtts.items():
		if mode == 'nearest_neighbor':
			closest_src = min(src_rtts, key=src_rtts.get)
			if closest_src in address_to_loc:
				estimated_locations[dst] = address_to_loc[closest_src]

		elif mode in ('hard_circle', 'great_circle_overlap_centroid'):
			region = FeasibleRegion(target_id=dst, mode=HARD_CIRCLE)
			for src, rtt in src_rtts.items():
				if src in address_to_loc:
					region.add_measurement(address_to_loc[src], max(0.0, rtt))
			if region.constraints:
				estimated_locations[dst] = region.get_location()

		elif mode == 'gaussian':
			region = FeasibleRegion(target_id=dst, mode=GAUSSIAN)
			for src, rtt in src_rtts.items():
				if src in address_to_loc:
					region.add_measurement(address_to_loc[src], rtt, sigma_ms=GLOBAL_SIGMA_MS)
			if region.constraints:
				estimated_locations[dst] = region.get_location()

		elif mode in ('em_gaussian', 'em_asymmetric'):
			noise = (ASYMMETRIC_NOISE if mode == 'em_asymmetric'
			         else GAUSSIAN_NOISE)
			region = FeasibleRegion(target_id=dst, mode=EM_GAUSSIAN,
			                        noise_model=noise)
			batch = [(address_to_loc[src], rtt)
			         for src, rtt in src_rtts.items() if src in address_to_loc]
			if batch:
				region.add_measurements_batch(batch)
				estimated_locations[dst] = region.get_location()

		else:
			raise ValueError(f"measurement_converter_mode {mode} not understood")

	return estimated_locations


def evaluate_geolocator(geolocator, target_data, converter_mode, budgets):
	"""Run one geolocator over the budget grid; returns its plot_data.
	Module-level and self-contained so the parallel path can run each
	(independent) geolocator in its own process."""
	address_to_loc = target_data.get('address_to_loc', {})
	all_targets = set()
	for dsts in target_data.get('loc_loc_meas', {}).values():
		all_targets.update(dsts.keys())

	geolocator.set_data(target_data)
	geolocator.solve()

	plot_data = {'budgets': [], 'errors': []}
	for budget in budgets:
		budgeted_measurements = geolocator.measurements(budget)
		if hasattr(geolocator, 'get_current_estimates'):
			estimated_locations = geolocator.get_current_estimates()
		else:
			estimated_locations = convert_measurements(
				budgeted_measurements, target_data, converter_mode)

		errors = []
		for dst in all_targets:
			if dst not in address_to_loc:
				continue
			if dst in estimated_locations:
				errors.append(get_distance(estimated_locations[dst], address_to_loc[dst]))
			else:
				errors.append(10000.0)

		if errors:
			avg_error = np.mean(errors)
			print(f"[{geolocator.name}] Budget: {budget:4d} | "
			      f"Targets Estimated: {len(estimated_locations):4d}/{len(all_targets)} | "
			      f"Avg Error: {avg_error:.2f} km", flush=True)
			plot_data['budgets'].append(budget)
			plot_data['errors'].append(avg_error)
	return plot_data


def _parallel_worker(geolocator, target_data, converter_mode, budgets, q):
	try:
		pd = evaluate_geolocator(geolocator, target_data, converter_mode, budgets)
		q.put((geolocator.name, pd, None))
	except Exception:
		q.put((geolocator.name, None, traceback.format_exc()))
	finally:
		if hasattr(geolocator, 'cleanup'):
			geolocator.cleanup()


class Geolocator_Comparator:
	def __init__(self):
		# Greedy variants carry their own estimates (get_current_estimates);
		# random and the selection-oracle are scored through the converter
		# (nearest_neighbor by default).
		self.geolocators = [
			Iterative_Greedy_Geolocator(region_mode=GAUSSIAN, region_slope=1.3,
			                            name='greedy_gaussian_1.3'),
			Iterative_Greedy_Geolocator(region_mode=GAUSSIAN, region_slope=1.05,
			                            name='greedy_gaussian_1.05'),
			Iterative_Greedy_Geolocator(region_mode=EM_GAUSSIAN,
			                            name='greedy_em'),
			# Shared-src/dst additive model; per-ping refits are O(pairs),
			# so throttle at real-mesh scale (selection reads slightly stale
			# params between refits; estimates come from the batch polish).
			Iterative_Greedy_Geolocator(region_mode=ADDITIVE,
			                            model_refit_every=25,
			                            name='greedy_additive'),
			Iterative_Greedy_Geolocator(region_mode=ADDITIVE,
			                            model_refit_every=25,
			                            selection='info_gain',
			                            name='greedy_additive_info'),
			Perfect_Geolocator(),
			Random_Geolocator(),
		]
		self.measurement_converter_mode = 'nearest_neighbor'
		self.target_data = None
		self.errors = {}

	def load_target_measurement_data(self):
		## loads all measurements from ripe atlas probes, and information about those probes
		cache_fn = os.path.join(CACHE_DIR, 'cached_target_data.pkl')
		if not os.path.exists(cache_fn):
			rap = RipeAtlasPipeline(start_date="2026-02-24", end_date="2026-02-24")
			rap.execute()
			self.target_data = rap.load_parsed_target_data()
			pickle.dump(self.target_data, open(cache_fn, 'wb'))
		else:
			self.target_data = pickle.load(open(cache_fn, 'rb'))
			for src in self.target_data['loc_loc_meas']:
				for dst in self.target_data['loc_loc_meas'][src]:
					self.target_data['loc_loc_meas'][src][dst] = [self.target_data['loc_loc_meas'][src][dst]]

	def convert_measurements_to_locations(self, measurements):
		"""
		Phase (b) estimation: turn a strategy's chosen measurements into
		location estimates (see run() for the two-phase design).

		The converter mode is the *estimation half* of a strategy, not a
		shared harness component. 'nearest_neighbor' (report the lowest-RTT
		VP's location) is the deliberately dumb estimator paired with the
		baselines; 'hard_circle'/'gaussian' are the overlap-computation
		methodology under development. Geolocators that carry their own
		estimator (Iterative_Greedy via get_current_estimates) never enter
		this function.
		"""
		return convert_measurements(measurements, self.target_data,
		                            self.measurement_converter_mode)

	def get_random_subsample(self, n=100):
		## Gets a random sample of all the measurement data, for testing
		print("Grabbing random subsample of {} sources".format(n))
		all_srcs = list(self.target_data['loc_loc_meas'])
		np.random.shuffle(all_srcs)
		all_srcs_subsample = all_srcs[0:n]
		new_target_data = {s:{} for s in all_srcs_subsample}
		for src in all_srcs_subsample:
			for dst in self.target_data['loc_loc_meas'][src]:
				if src == dst: continue
				try:
					new_target_data[dst]
					new_target_data[src][dst] = self.target_data['loc_loc_meas'][src][dst]
				except KeyError:
					pass
		self.target_data['loc_loc_meas'] = new_target_data

	def do_cache(self, geolocator):
		return {'smart_perfect': True, 'random': True}.get(geolocator.name, False)

	def run(self, min_budget=100, max_budget=2500, step=100, n_subsample=100,
	        parallel=False):
		## Two-phase comparison, analogous to a train/test split:
		##   (a) selection  - geolocator.measurements(budget) decides which pings
		##       to spend, under realistic information limits (no ground truth;
		##       the oracle deliberately cheats here, that is its job as an
		##       upper bound).
		##   (b) evaluation - the chosen measurements become location estimates
		##       and are scored against ground-truth probe locations (perfect
		##       information, unrealistic by design; ground truth is never fed
		##       back into selection or estimation).
		## Each strategy is a complete system of selection + estimation, so the
		## baselines are *supposed* to lack the overlap computation: random+NN
		## is the dumb whole-system baseline, greedy+overlap is ours.
		self.load_target_measurement_data()

		self.get_random_subsample(n=n_subsample)

		budgets = list(range(min_budget, max_budget + 1, step))
		self.plot_data = {}

		def cache_fn(geolocator):
			# n_subsample is part of the cache key: results from different
			# subsample sizes are not interchangeable.
			return os.path.join(CACHE_DIR, f"cached_results_{geolocator.name}_{self.measurement_converter_mode}_n{n_subsample}.pkl")

		to_run = []
		for geolocator in self.geolocators:
			if os.path.exists(cache_fn(geolocator)) and self.do_cache(geolocator):
				self.plot_data[geolocator.name] = pickle.load(open(cache_fn(geolocator), 'rb'))
			else:
				to_run.append(geolocator)

		if parallel:
			# Geolocators are independent objects — one child process each.
			# Plain (non-daemonic) Processes, not a Pool: the greedys spawn
			# their own inner worker pools, which daemonic pool workers are
			# not allowed to do. Watch CPU oversubscription: each greedy's
			# inner pool defaults to cpu_count workers — pass max_workers
			# to the greedy constructors when running many in parallel.
			q = multiprocessing.get_context('spawn').Queue()
			procs = []
			for geolocator in to_run:
				print(f"\n--- Launching {geolocator.name} (parallel) ---")
				p = multiprocessing.get_context('spawn').Process(
					target=_parallel_worker,
					args=(geolocator, self.target_data,
					      self.measurement_converter_mode, budgets, q))
				p.start()
				procs.append(p)
			for _ in procs:
				name, pd, err = q.get()
				if err is not None:
					raise RuntimeError(f"{name} failed in parallel run:\n{err}")
				self.plot_data[name] = pd
			for p in procs:
				p.join()
		else:
			for geolocator in to_run:
				print(f"\n--- Running {geolocator.name} ---")
				self.plot_data[geolocator.name] = evaluate_geolocator(
					geolocator, self.target_data,
					self.measurement_converter_mode, budgets)

		for geolocator in to_run:
			if self.do_cache(geolocator) and geolocator.name in self.plot_data:
				pickle.dump(self.plot_data[geolocator.name],
				            open(cache_fn(geolocator), 'wb'))

		# Call the plotting function after all geolocators have run (or loaded)
		plot_error_over_budget(self.plot_data, os.path.join(FIG_DIR, "geolocator_results.pdf"))

if __name__ == "__main__":
	np.random.seed(31415)
	gc = Geolocator_Comparator()
	
	# 1. Load the data into gc.target_data
	gc.load_target_measurement_data()
	plot_ping_count_cdf(gc.target_data)
	
	# # 2. Call the diagnostic plot to see what your dataset actually looks like
	# print("Generating Latency vs. Distance diagnostic plot...")
	# plot_latency_vs_distance(gc.target_data, os.path.join(FIG_DIR, "latency_vs_distance.pdf"))
	
	# 3. Run your geolocator simulation as normal
	gc.run()

