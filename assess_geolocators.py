import multiprocessing
import sys
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
	FiberFloorRtt,
)

from plot_results import *

# internet_gmaps (the fiber atlas) lives inside this repo; its modules
# assume their own directory on sys.path (see its conftest.py)
IGM_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'internet_gmaps')
if IGM_DIR not in sys.path:
	sys.path.append(IGM_DIR)

FIBER_FIELD_CACHE = os.path.join(IGM_DIR, 'data', 'cache', 'policy_fields')


def convert_measurements(measurements, target_data, mode, rtt_model=None):
	"""Module-level (picklable, comparator-free) estimation of measured
	targets — see Geolocator_Comparator.convert_measurements_to_locations
	for the semantics of each mode.  `rtt_model` (additive_em mode only)
	swaps the geodesic d/100 base term for an injected model (fiber floor)."""
	address_to_loc = target_data.get('address_to_loc', {})

	if mode == 'additive_em':
		pairs = {}
		for src, dsts in measurements.items():
			for dst, rtts in dsts.items():
				if rtts:
					pairs[(src, dst)] = [float(r) for r in rtts]
		estimates, _, _, _, _ = additive_batch_em(pairs, address_to_loc,
		                                          rtt_model=rtt_model)
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


def evaluate_geolocator(geolocator, target_data, converter_mode, budgets,
                        rtt_model=None):
	"""Run one geolocator over the budget grid; returns its plot_data
	('budgets'/'errors' for plotting + 'per_target' {budget: {dst: err}}
	for post-hoc slicing).  Module-level and self-contained so the
	parallel path can run each (independent) geolocator in its own
	process."""
	address_to_loc = target_data.get('address_to_loc', {})
	all_targets = set()
	for dsts in target_data.get('loc_loc_meas', {}).values():
		all_targets.update(dsts.keys())

	geolocator.set_data(target_data)
	geolocator.solve()

	plot_data = {'budgets': [], 'errors': [], 'per_target': {}}
	for budget in budgets:
		budgeted_measurements = geolocator.measurements(budget)
		if hasattr(geolocator, 'get_current_estimates'):
			estimated_locations = geolocator.get_current_estimates()
		else:
			estimated_locations = convert_measurements(
				budgeted_measurements, target_data, converter_mode,
				rtt_model=rtt_model)

		per_target = {}
		for dst in all_targets:
			if dst not in address_to_loc:
				continue
			if dst in estimated_locations:
				per_target[dst] = get_distance(estimated_locations[dst], address_to_loc[dst])
			else:
				per_target[dst] = 10000.0

		if per_target:
			errors = list(per_target.values())
			avg_error = np.mean(errors)
			print(f"[{geolocator.name}] Budget: {budget:4d} | "
			      f"Targets Estimated: {len(estimated_locations):4d}/{len(all_targets)} | "
			      f"Avg Error: {avg_error:.2f} km | Median: {np.median(errors):.2f} km", flush=True)
			plot_data['budgets'].append(budget)
			plot_data['errors'].append(avg_error)
			plot_data['per_target'][budget] = per_target
	return plot_data


def _parallel_worker(geolocator, target_data, converter_mode, budgets, rtt_model, q):
	try:
		pd = evaluate_geolocator(geolocator, target_data, converter_mode, budgets,
		                         rtt_model=rtt_model)
		q.put((geolocator.name, pd, None))
	except Exception:
		q.put((geolocator.name, None, traceback.format_exc()))
	finally:
		if hasattr(geolocator, 'cleanup'):
			geolocator.cleanup()


def build_policy_estimator(vp_npz):
	"""Rebuild the policy-aware floor estimator on the latest built graph;
	module-level (picklable via functools.partial) so greedy workers can
	reconstruct it after unpickling a FiberFloorRtt."""
	from glob import glob
	from fiber_graph import FiberGraph
	from floor_query import PolicyFloorEstimator

	vs = np.load(vp_npz)
	npz = np.load(sorted(glob(os.path.join(IGM_DIR, 'data', 'graph_*.npz')))[-1])
	graph = FiberGraph(
		npz['node_lat'], npz['node_lon'], npz['edge_src'], npz['edge_dst'],
		npz['edge_rtt_ms'],
		edge_feature=npz['edge_feature'] if 'edge_feature' in npz else None,
		feature_names=tuple(npz['feature_names']) if 'feature_names' in npz else (),
	)
	return PolicyFloorEstimator(
		graph, vs['vp_lat'], vs['vp_lon'],
		cache_dir=FIBER_FIELD_CACHE, max_cached_fields=1024,
	)


def make_fiber_model(target_data, tag, slope=1.3, offset_ms=0.0):
	"""FiberFloorRtt whose estimator VP rows are the current mesh's
	sources.  VP coordinates go into an npz sidecar so pickled models
	carry only (factory, token)."""
	import functools

	addr = target_data['address_to_loc']
	vp_addrs = sorted(target_data['loc_loc_meas'])
	locs = [addr[a] for a in vp_addrs]
	os.makedirs(FIBER_FIELD_CACHE, exist_ok=True)
	vp_npz = os.path.join(FIBER_FIELD_CACHE, f'vps{tag or "_default"}.npz')
	np.savez(vp_npz,
	         vp_lat=np.array([l[0] for l in locs]),
	         vp_lon=np.array([l[1] for l in locs]))
	factory = functools.partial(build_policy_estimator, vp_npz=vp_npz)
	return FiberFloorRtt(estimator_factory=factory,
	                     cache_token=f'fiber{tag or "_default"}',
	                     slope=slope, offset_ms=offset_ms)


def facility_select_sources(m, addr, targets, n):
	"""Greedy facility location: pick up to n sources minimizing the
	targets' distance to their nearest selected measuring source (lazy
	greedy; facility-location gains are submodular so stale heap entries
	re-evaluated at the top are exact).  Once no pick can reduce any
	distance, remaining slots are filled by raw coverage count — those
	sources add measurement diversity but cannot move the floor.

	NOTE: reads target ground-truth locations — this CONSTRUCTS an
	oracle-placed-VP benchmark world (matching the floor sweep); it is not
	an inference-time choice and must never be used inside a strategy."""
	import heapq

	t_index = {t: i for i, t in enumerate(targets)}
	cur = np.full(len(targets), 10000.0)
	pairs = {}
	for src, dsts in m.items():
		if src not in addr:
			continue
		sloc = addr[src]
		ti, dd = [], []
		for t in dsts:
			i = t_index.get(t)
			if i is not None and t != src:
				ti.append(i)
				dd.append(get_distance(sloc, addr[t]))
		if ti:
			pairs[src] = (np.array(ti), np.array(dd))

	def gain(src):
		ti, dd = pairs[src]
		return float(np.sum(np.maximum(0.0, cur[ti] - dd)))

	heap = [(-gain(src), src) for src in pairs]
	heapq.heapify(heap)
	chosen = []
	while heap and len(chosen) < n:
		neg_g, src = heapq.heappop(heap)
		g = gain(src)
		if g <= 0.0:
			continue
		if g != -neg_g:
			heapq.heappush(heap, (-g, src))
			continue
		ti, dd = pairs[src]
		np.minimum.at(cur, ti, dd)
		chosen.append(src)
	if len(chosen) < n:
		spare = sorted(set(pairs) - set(chosen),
		               key=lambda s: (-len(pairs[s][0]), s))
		chosen += spare[: n - len(chosen)]
	return chosen


class Geolocator_Comparator:
	def __init__(self, geolocators=None, data_source='legacy'):
		# Greedy variants carry their own estimates (get_current_estimates);
		# random and the selection-oracle are scored through the converter.
		# Per-geolocator `converter_mode` / `rtt_model` attributes override
		# the comparator defaults, so estimation variants are settings on
		# instances rather than separate harnesses.
		self.geolocators = geolocators if geolocators is not None else [
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
		# 'legacy' = the symmetric daily-dump cache (cached_target_data.pkl);
		# 'merged' = internet_gmaps mesh_data.load_target_data(): daily mesh
		# + live campaign, min-RTT wins, SOL-suspects excluded.  The merged
		# mesh is ASYMMETRIC (campaign targets are dst-only with thin,
		# disjoint source sets), so use the n_targets sampling path with it.
		self.data_source = data_source
		self.target_data = None
		self.experiment_meta = {}
		self._subsampled = False
		self.errors = {}

	def load_target_measurement_data(self):
		## loads all measurements from ripe atlas probes, and information about those probes
		if self.data_source == 'merged':
			from mesh_data import load_target_data
			d = load_target_data()
			self.target_data = {
				'address_to_loc': dict(d['address_to_loc']),
				'loc_loc_meas': {s: {t: [float(r)] for t, r in dsts.items()}
				                 for s, dsts in d['loc_loc_meas'].items()},
			}
			return
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

	def get_random_subsample(self, n=100, n_targets=None, k_vps_per_target=None,
	                         min_target_coverage=15, coverage_depth=None,
	                         seed=31415, source_selection='coverage'):
		"""Subsample the mesh.  Two modes:

		n_targets=None (legacy): symmetric — n sources, which are also the
		targets (the dense daily-dump mesh shape).  Unchanged behavior.

		n_targets set: asymmetric — sample n_targets from eligible dsts
		(>= min_target_coverage sources anywhere in the mesh), then choose
		n sources by lazy-greedy coverage: each pick maximizes the number
		of sampled targets still below `coverage_depth` VPs that it
		measures.  Needed for the merged mesh, where campaign targets have
		thin, disjoint source sets (no shared core exists — measured:
		median 6 targets per campaign source, zero overlap with the 909
		daily probes).  `source_selection='facility'` swaps the coverage
		greedy for best-k facility location (floor-matched worlds; see
		facility_select_sources).  `k_vps_per_target` caps each target's
		VP count (runtime knob for large-n runs).  Targets left with zero
		chosen sources are dropped (counted in experiment_meta).
		"""
		self._subsampled = True
		if n_targets is None:
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
			return

		import heapq
		rng = np.random.default_rng(seed)
		coverage_depth = coverage_depth or k_vps_per_target or 10
		addr = self.target_data['address_to_loc']
		m = self.target_data['loc_loc_meas']

		srcs_of = {}
		for s, dsts in m.items():
			if s not in addr:
				continue
			for t in dsts:
				if t != s and t in addr:
					srcs_of.setdefault(t, []).append(s)
		eligible = sorted(t for t, ss in srcs_of.items()
		                  if len(ss) >= min_target_coverage)
		n_t = min(n_targets, len(eligible))
		targets = sorted(rng.choice(eligible, n_t, replace=False))
		tset = set(targets)

		src_targets = {}
		for s, dsts in m.items():
			if s in addr:
				ts = [t for t in dsts if t in tset and t != s]
				if ts:
					src_targets[s] = ts

		if source_selection == 'facility':
			chosen = facility_select_sources(m, addr, targets, n)
		else:
			# lazy-greedy coverage selection (gains only shrink, so a stale
			# heap top re-evaluated is exact); leftovers fill remaining
			# slots by raw target count once every target hit coverage_depth
			cov = {t: 0 for t in targets}
			heap = [(-len(ts), s) for s, ts in src_targets.items()]
			heapq.heapify(heap)
			chosen, leftovers = [], []
			while heap and len(chosen) < n:
				neg_g, s = heapq.heappop(heap)
				gain = sum(1 for t in src_targets[s] if cov[t] < coverage_depth)
				if gain != -neg_g:
					if gain > 0:
						heapq.heappush(heap, (-gain, s))
					else:
						leftovers.append((-gain, s))
					continue
				chosen.append(s)
				for t in src_targets[s]:
					cov[t] += 1
			if len(chosen) < n:
				leftovers.sort(key=lambda kv: (-len(src_targets[kv[1]]), kv[1]))
				chosen += [s for _, s in leftovers[: n - len(chosen)]]

		chosen_set = set(chosen)
		new_meas = {}
		dropped = 0
		for t in targets:
			ss = sorted(s for s in srcs_of[t] if s in chosen_set)
			if not ss:
				dropped += 1
				continue
			if k_vps_per_target and len(ss) > k_vps_per_target:
				ss = list(rng.choice(ss, k_vps_per_target, replace=False))
			for s in ss:
				new_meas.setdefault(s, {})[t] = list(m[s][t])

		involved = set(new_meas) | {t for d in new_meas.values() for t in d}
		self.target_data = {
			'address_to_loc': {a: tuple(map(float, addr[a])) for a in involved},
			'loc_loc_meas': new_meas,
		}
		n_pairs = sum(len(v) for v in new_meas.values())
		self.experiment_meta = {
			'n_sources': len(new_meas), 'n_targets': n_t - dropped,
			'targets_dropped_uncovered': dropped, 'n_pairs': n_pairs,
			'k_vps_per_target': k_vps_per_target, 'seed': seed,
			'source_selection': source_selection,
		}
		print(f"asymmetric subsample: {n_t - dropped} targets "
		      f"({dropped} dropped uncovered), {len(new_meas)} sources, "
		      f"{n_pairs} measured pairs", flush=True)

	def do_cache(self, geolocator):
		return {'smart_perfect': True, 'random': True}.get(geolocator.name, False)

	def _converter_mode_for(self, geolocator):
		return getattr(geolocator, 'converter_mode', None) or self.measurement_converter_mode

	def run(self, min_budget=100, max_budget=2500, step=100, n_subsample=100,
	        parallel=False, budgets=None, n_targets=None, k_vps_per_target=None,
	        tag='', fig_name=None):
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
		if self.target_data is None:
			self.load_target_measurement_data()
		if not self._subsampled:
			self.get_random_subsample(n=n_subsample, n_targets=n_targets,
			                          k_vps_per_target=k_vps_per_target)

		budgets = list(budgets) if budgets else list(range(min_budget, max_budget + 1, step))
		self.plot_data = {}
		# Experiment shape (<sources>src_<targets>dst) keys every artifact:
		# results from different subsample shapes are not interchangeable.
		shape = _shape_name(self.experiment_meta, n_subsample)

		def cache_fn(geolocator):
			mode = self._converter_mode_for(geolocator)
			return os.path.join(CACHE_DIR, f"cached_results_{geolocator.name}_{mode}_{shape}{tag}.pkl")

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
					      self._converter_mode_for(geolocator), budgets,
					      getattr(geolocator, 'rtt_model', None), q))
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
					self._converter_mode_for(geolocator), budgets,
					rtt_model=getattr(geolocator, 'rtt_model', None))
				if hasattr(geolocator, 'cleanup'):
					geolocator.cleanup()

		for geolocator in to_run:
			if self.do_cache(geolocator) and geolocator.name in self.plot_data:
				pickle.dump(self.plot_data[geolocator.name],
				            open(cache_fn(geolocator), 'wb'))

		# Full results (incl. per-target errors) for post-hoc slicing.
		run_record = {'plot_data': self.plot_data, 'budgets': budgets,
		              'meta': self.experiment_meta,
		              'address_to_loc': self.target_data['address_to_loc']}
		run_fn = os.path.join(CACHE_DIR, f"geolocator_run_{shape}{tag}.pkl")
		pickle.dump(run_record, open(run_fn, 'wb'))
		print(f"wrote {run_fn}", flush=True)

		# Call the plotting function after all geolocators have run (or loaded)
		fig_name = fig_name or f"geolocator_results_{shape}{tag}.pdf"
		plot_error_over_budget(self.plot_data, os.path.join(FIG_DIR, fig_name))


REGIONS = {
	'Europe': set('AL AD AT BA BE BG BY CH CY CZ DE DK EE ES FI FO FR GB GG GI GR '
	              'HR HU IE IM IS IT JE LI LT LU LV MC MD ME MK MT NL NO PL PT RO '
	              'RS RU SE SI SJ SK SM UA VA XK'.split()),
	'N.America': set('US CA MX GL PM BM'.split()),
	'C.America/Carib': set('AG AI AW BB BL BQ BS BZ CR CU CW DM DO GD GP GT HN HT '
	                       'JM KN KY LC MF MQ MS NI PA PR SV SX TC TT VC VG VI'.split()),
	'S.America': set('AR BO BR CL CO EC FK GF GY PE PY SR UY VE'.split()),
	'East Asia': set('CN HK JP KP KR MO TW'.split()),
	'SE Asia': set('BN ID KH LA MM MY PH SG TH TL VN'.split()),
	'South Asia': set('AF BD BT IN LK MV NP PK'.split()),
	'Oceania/Pacific': set('AS AU CK FJ FM GU KI MH MP NC NF NR NU NZ PF PG PW SB '
	                       'TK TO TV VU WF WS'.split()),
	'MidEast/Gulf': set('AE BH IL IQ IR JO KW LB OM PS QA SA SY TR YE'.split()),
	'Central Asia': set('AM AZ GE KG KZ MN TJ TM UZ'.split()),
}


def region_of(cc):
	for name, ccs in REGIONS.items():
		if cc in ccs:
			return name
	return 'Africa'   # remaining ccs are African (incl. islands)


def print_region_breakdown(plot_data, address_to_loc, budget):
	"""Per-region mean errors at one budget, from the recorded per-target
	errors (strategies without per_target data are skipped)."""
	import reverse_geocoder as rgc
	names = [n for n in sorted(plot_data)
	         if plot_data[n].get('per_target', {}).get(budget)]
	if not names:
		return
	targets = sorted(plot_data[names[0]]['per_target'][budget])
	ccs = [r['cc'] for r in rgc.search(
		[tuple(map(float, address_to_loc[t])) for t in targets],
		mode=1, verbose=False)]
	by_region = {}
	for t, cc in zip(targets, ccs):
		by_region.setdefault(region_of(cc), []).append(t)
	print(f"\n=== per-region mean error at b={budget} (n targets in parens) ===")
	print(f"{'region':>18s}       " + "".join(f"{n:>24s}" for n in names))
	for region in sorted(by_region, key=lambda r: -len(by_region[r])):
		ts = by_region[region]
		cells = ""
		for n in names:
			errs = plot_data[n]['per_target'][budget]
			v = [errs[t] for t in ts if t in errs]
			cells += f"{np.mean(v):18.0f} km   " if v else f"{'-':>24s}"
		print(f"{region:>18s} ({len(ts):4d}) {cells}")


# ---------------------------------------------------------------------------
# CLI / config-file settings (see README.md "Running experiments").
# Precedence per setting: command line > --config JSON > GEOLOC_* env var
# (the legacy interface) > built-in default.  With no settings at all, the
# historical default comparison runs unchanged.
# ---------------------------------------------------------------------------

_SETTINGS = {
	# name              (env var,                 default,             cast)
	'data':             ('GEOLOC_DATA',           'legacy',            str),
	'n_sources':        ('GEOLOC_NSRC',           100,                 int),
	'n_targets':        ('GEOLOC_NTGT',           None,                int),
	'vps_per_target':   ('GEOLOC_VPS_PER_TARGET', None,                int),
	'budgets':          ('GEOLOC_BUDGETS',        None,                str),
	'min_budget':       ('GEOLOC_MIN_BUDGET',     100,                 int),
	'max_budget':       ('GEOLOC_MAX_BUDGET',     2500,                int),
	'budget_step':      ('GEOLOC_BUDGET_STEP',    100,                 int),
	'fiber':            ('GEOLOC_FIBER',          False,               lambda v: v == '1'),
	'fiber_slope':      ('GEOLOC_FIBER_SLOPE',    1.3,                 float),
	'fiber_offset_ms':  ('GEOLOC_FIBER_OFFSET',   0.0,                 float),
	'strategies':       ('GEOLOC_STRATEGIES',     None,                str),
	'converter_mode':   ('GEOLOC_CONVERTER',      'nearest_neighbor',  str),
	'workers':          ('GEOLOC_WORKERS',        6,                   int),
	'oracle_candidates':('GEOLOC_ORACLE_CANDS',   50,                  int),
	'tag':              ('GEOLOC_TAG',            '',                  str),
	'fig_name':         ('GEOLOC_FIG_NAME',       None,                str),
	'seed':             ('GEOLOC_SEED',           31415,               int),
	'parallel':         ('GEOLOC_PARALLEL',       False,               lambda v: v == '1'),
	'breakdown':        ('GEOLOC_BREAKDOWN',      True,                lambda v: v != '0'),
	'floor_sweep_targets': ('GEOLOC_FLOOR_TARGETS', None,              str),
	'floor_sweep_sources': ('GEOLOC_FLOOR_SOURCES', '200,1000,0',      str),
	'floor_sweep_seeds':   ('GEOLOC_FLOOR_SEEDS',   3,                 int),
	'source_selection':    ('GEOLOC_SOURCE_SELECTION', 'coverage',     str),
}


def parse_settings(argv=None):
	"""Merge CLI > config JSON > env > defaults into a settings dict, plus
	`configured`: whether anything departs from the legacy default run."""
	import argparse
	import json

	p = argparse.ArgumentParser(
		description="Geolocation strategy comparison harness. "
		            "Settings, not scripts: exploration (selection) and "
		            "estimation variants are all flags here.",
		epilog="Every flag can also come from a --config JSON (same names) "
		       "or a GEOLOC_* env var; command line wins. See README.md.")
	p.add_argument('--config', help='JSON file of settings (keys = flag names)')
	p.add_argument('--replot', metavar='RUN_PKL',
	               help='regenerate figures + breakdown from an existing '
	                    'cache/geolocator_run_*.pkl without running anything')
	p.add_argument('--data', choices=('legacy', 'merged'),
	               help='legacy = symmetric daily-dump cache; merged = daily '
	                    'mesh + live campaign (asymmetric; use --n-targets)')
	p.add_argument('--n-sources', type=int, help='number of probes/VPs (default 100)')
	p.add_argument('--n-targets', type=int,
	               help='independent target count — enables asymmetric sampling '
	                    'with coverage-greedy source selection')
	p.add_argument('--vps-per-target', type=int, help='cap VPs per target (runtime knob)')
	p.add_argument('--source-selection', choices=('coverage', 'facility'),
	               help='asymmetric-sampling source choice: coverage (honest '
	                    'count-based greedy, default) or facility (best-k by '
	                    'greedy facility location — matches the oracle floor '
	                    'sweep; uses target ground truth to CONSTRUCT the '
	                    'benchmark world, so floors are as low as the source '
	                    'budget allows)')
	p.add_argument('--budgets', help='explicit comma list, e.g. 1500,3000,6000')
	p.add_argument('--min-budget', type=int)
	p.add_argument('--max-budget', type=int)
	p.add_argument('--budget-step', type=int)
	p.add_argument('--fiber', action='store_true', default=None,
	               help='add fiber-floor variants (random_additive_fiber, '
	                    'greedy_phased_fiber)')
	p.add_argument('--fiber-slope', type=float, help='inflation over the raw floor (default 1.3)')
	p.add_argument('--fiber-offset-ms', type=float)
	p.add_argument('--strategies', help='comma filter of strategy names to run')
	p.add_argument('--converter-mode', help='default estimation mode for '
	               'strategies without their own (default nearest_neighbor)')
	p.add_argument('--workers', type=int, help='greedy inner-pool size')
	p.add_argument('--oracle-candidates', type=int,
	               help='Perfect_Geolocator sources considered per target')
	p.add_argument('--tag', help='suffix for cache/figure filenames')
	p.add_argument('--fig-name', help='output figure filename '
	               '(default geolocator_results<tag>.pdf)')
	p.add_argument('--seed', type=int)
	p.add_argument('--parallel', action='store_true', default=None)
	p.add_argument('--no-breakdown', dest='breakdown', action='store_false',
	               default=None, help='skip the per-region error table')
	p.add_argument('--floor-sweep-targets', metavar='N1,N2,...',
	               help='instead of a comparison run, compute the full-'
	                    'coverage "perfect" floor (NN over the lowest-RTT '
	                    'measured VP, = smart_perfect at generous budget) '
	                    'per target count, for each --floor-sweep-sources')
	p.add_argument('--floor-sweep-sources', metavar='S1,S2,...',
	               help='source budgets for the floor sweep; 0 = all sources '
	                    '(default 200,1000,0)')
	p.add_argument('--floor-sweep-seeds', type=int,
	               help='sampling seeds per point (default 3)')
	args = vars(p.parse_args(argv))

	replot = args.pop('replot')
	config_path = args.pop('config')
	config = {}
	if config_path:
		with open(config_path) as fh:
			config = json.load(fh)

	settings = {}
	explicit = set()
	for name, (env, default, cast) in _SETTINGS.items():
		if args.get(name) is not None:
			settings[name] = args[name]
			explicit.add(name)
		elif name in config:
			settings[name] = config[name]
			explicit.add(name)
		elif os.environ.get(env) not in (None, ''):
			settings[name] = cast(os.environ[env])
			explicit.add(name)
		else:
			settings[name] = default
	if isinstance(settings['budgets'], str):
		settings['budgets'] = [int(b) for b in settings['budgets'].split(',')]
	settings['configured'] = bool(explicit - {'seed', 'workers'})
	settings['replot'] = replot
	return settings


def run_floor_sweep(s):
	"""The 'perfect' floor as a function of target count, per source budget.

	At generous budget, smart_perfect scored through the NN converter
	converges to the location of the target's lowest-RTT measured VP — no
	oracle ordering needed, so the floor is computed exactly.  Two floors:
	  nn floor  — distance to the lowest-RTT measured VP (what
	              smart_perfect/NN actually achieve at full coverage)
	  geo floor — distance to the NEAREST measured VP (the bound for ANY
	              estimator that reports a VP location; the gap between the
	              two is pure RTT-vs-distance mismatch)

	This is a floor, so the SOURCE choice cheats too: for each budget k the
	sources are picked by greedy facility location (each pick maximally
	reduces total distance-to-nearest-selected-source over the targets) —
	the best k sources an oracle could have requested, not the experiment
	sampler's coverage heuristic.  A k-budget selection is the first k
	picks of the larger budget's greedy, and targets are NESTED prefixes
	of one shuffle per seed, so curves are consistent along both axes.
	Targets with no selected measuring source count at the 10,000 km
	missing penalty (matching the harness).
	"""
	import heapq

	targets_grid = sorted(int(x) for x in str(s['floor_sweep_targets']).split(','))
	sources_grid = [int(x) for x in str(s['floor_sweep_sources']).split(',')]
	src_checkpoints = sorted(k for k in sources_grid if k)
	gc = Geolocator_Comparator(geolocators=[], data_source=s['data'])
	gc.load_target_measurement_data()
	addr = gc.target_data['address_to_loc']
	m = gc.target_data['loc_loc_meas']
	PENALTY = 10000.0

	# eligible targets (same rule as the experiment sampler) + per-source
	# (target_index, distance, min_rtt) arrays over all measured pairs
	cov = {}
	for src, dsts in m.items():
		if src not in addr:
			continue
		for t in dsts:
			if t != src and t in addr:
				cov[t] = cov.get(t, 0) + 1
	eligible = sorted(t for t, c in cov.items() if c >= 15)
	t_index = {t: i for i, t in enumerate(eligible)}
	src_pairs = {}
	for src, dsts in m.items():
		if src not in addr:
			continue
		sloc = addr[src]
		ti, dd, rr = [], [], []
		for t, rtts in dsts.items():
			i = t_index.get(t)
			if i is not None and t != src:
				ti.append(i)
				dd.append(get_distance(sloc, addr[t]))
				rr.append(min(rtts))
		if ti:
			src_pairs[src] = (np.array(ti), np.array(dd), np.array(rr))

	def floors_for(prefix_idx):
		"""One facility-location greedy over the target prefix; yields
		(k, nn_errs, geo_errs) at each source-budget checkpoint plus
		(0, ...) for the all-sources floor."""
		n = len(eligible)
		cur = np.zeros(n)          # 0 outside the prefix -> zero gain there
		cur[prefix_idx] = PENALTY
		best_rtt = np.full(n, np.inf)
		nn_err = np.full(n, PENALTY)

		def apply(src):
			ti, dd, rr = src_pairs[src]
			np.minimum.at(cur, ti, dd)
			better = rr < best_rtt[ti]
			ii = ti[better]
			best_rtt[ii] = rr[better]
			nn_err[ii] = dd[better]

		def gain(src):
			ti, dd, _ = src_pairs[src]
			return float(np.sum(np.maximum(0.0, cur[ti] - dd)))

		heap = [(-gain(src), src) for src in src_pairs]
		heapq.heapify(heap)
		chosen = 0
		out = []
		for k in src_checkpoints:
			while chosen < k and heap:
				neg_g, src = heapq.heappop(heap)
				g = gain(src)
				if g <= 0.0:
					continue          # adds nothing; never will (submodular)
				if g != -neg_g:
					heapq.heappush(heap, (-g, src))
					continue
				apply(src)
				chosen += 1
			out.append((k, nn_err[prefix_idx].copy(), cur[prefix_idx].copy()))
		if 0 in sources_grid:
			for src in src_pairs:
				apply(src)
			out.append((0, nn_err[prefix_idx].copy(), cur[prefix_idx].copy()))
		return out

	results = {}
	for k_seed in range(s['floor_sweep_seeds']):
		rng = np.random.default_rng(s['seed'] + k_seed)
		perm = rng.permutation(len(eligible))
		for n_t in targets_grid:
			prefix_idx = np.sort(perm[:min(n_t, len(eligible))])
			for k, nn, ge in floors_for(prefix_idx):
				cell = results.setdefault((k, n_t), {
					'nn_mean': [], 'nn_med': [], 'geo_mean': [], 'geo_med': []})
				cell['nn_mean'].append(float(nn.mean()))
				cell['nn_med'].append(float(np.median(nn)))
				cell['geo_mean'].append(float(ge.mean()))
				cell['geo_med'].append(float(np.median(ge)))

	for n_src in sources_grid:
		for n_t in targets_grid:
			cell = results[(n_src, n_t)]
			print(f"n_src={n_src or 'all':>5} n_targets={n_t:5d}: "
			      f"nn floor {np.mean(cell['nn_mean']):6.0f}/"
			      f"{np.mean(cell['nn_med']):6.0f}  "
			      f"geo floor {np.mean(cell['geo_mean']):6.0f}/"
			      f"{np.mean(cell['geo_med']):6.0f} km (mean/median, "
			      f"{s['floor_sweep_seeds']} seeds)", flush=True)

	fig_name = s['fig_name'] or f"oracle_floor_sweep{s['tag']}.pdf"
	plot_floor_sweep(results, targets_grid, sources_grid,
	                 os.path.join(FIG_DIR, fig_name))
	out = os.path.join(CACHE_DIR, f"floor_sweep{s['tag']}.pkl")
	pickle.dump({'results': results, 'targets_grid': targets_grid,
	             'sources_grid': sources_grid, 'settings': dict(s)},
	            open(out, 'wb'))
	print(f"wrote {out}", flush=True)


def _shape_name(meta, n_subsample):
	"""<sources>src_<targets>dst — the human-readable experiment shape used
	in figure/cache filenames (legacy symmetric runs: n on both sides)."""
	meta = meta or {}
	return (f"{meta.get('n_sources', n_subsample)}src_"
	        f"{meta.get('n_targets', n_subsample)}dst")


def replot_run(run_pkl, settings):
	"""Regenerate the error-vs-budget figure + region breakdown from a
	recorded run."""
	run = pickle.load(open(run_pkl, 'rb'))
	shape = _shape_name(run.get('meta'), '')
	fig_name = settings['fig_name'] or f"geolocator_results_{shape}.pdf"
	plot_error_over_budget(run['plot_data'], os.path.join(FIG_DIR, fig_name))
	if settings['breakdown']:
		print_region_breakdown(run['plot_data'], run['address_to_loc'],
		                       run['budgets'][-1])


def build_configured_comparator(s):
	"""Build the comparator + strategy set from a settings dict."""
	gc = Geolocator_Comparator(geolocators=[], data_source=s['data'])
	gc.measurement_converter_mode = s['converter_mode']
	gc.load_target_measurement_data()
	gc.get_random_subsample(n=s['n_sources'], n_targets=s['n_targets'],
	                        k_vps_per_target=s['vps_per_target'],
	                        seed=s['seed'],
	                        source_selection=s['source_selection'])

	fiber_model = (make_fiber_model(gc.target_data, s['tag'],
	                                slope=s['fiber_slope'],
	                                offset_ms=s['fiber_offset_ms'])
	               if s['fiber'] else None)
	oracle = Perfect_Geolocator()
	oracle.n_srcs_to_consider = s['oracle_candidates']
	geolocators = [
		Random_Geolocator(name='random', order_seed=s['seed']),
		Random_Geolocator(name='random_additive', converter_mode='additive_em',
		                  order_seed=s['seed']),
		Iterative_Greedy_Geolocator(region_mode=ADDITIVE, model_refit_every=25,
		                            selection='phased', max_workers=s['workers'],
		                            name='greedy_phased'),
		oracle,
	]
	if s['fiber']:
		geolocators += [
			Random_Geolocator(name='random_additive_fiber',
			                  converter_mode='additive_em',
			                  rtt_model=fiber_model, order_seed=s['seed']),
			Iterative_Greedy_Geolocator(region_mode=ADDITIVE, model_refit_every=25,
			                            selection='phased', max_workers=s['workers'],
			                            rtt_model=fiber_model,
			                            name='greedy_phased_fiber'),
		]
	if s['strategies']:
		keep = set(s['strategies'].split(','))
		geolocators = [g for g in geolocators if g.name in keep]
	gc.geolocators = geolocators
	return gc


if __name__ == "__main__":
	settings = parse_settings()
	np.random.seed(settings['seed'])
	if settings['replot']:
		replot_run(settings['replot'], settings)
	elif settings['floor_sweep_targets']:
		run_floor_sweep(settings)
	elif settings['configured']:
		import random as _random
		_random.seed(settings['seed'])
		gc = build_configured_comparator(settings)
		gc.run(n_subsample=settings['n_sources'],
		       min_budget=settings['min_budget'],
		       max_budget=settings['max_budget'],
		       step=settings['budget_step'],
		       budgets=settings['budgets'],
		       parallel=settings['parallel'],
		       tag=settings['tag'],
		       fig_name=settings['fig_name'])
		if settings['breakdown'] and gc.plot_data:
			last_b = (settings['budgets'] or [settings['max_budget']])[-1]
			print_region_breakdown(gc.plot_data,
			                       gc.target_data['address_to_loc'], last_b)
	else:
		gc = Geolocator_Comparator()

		# 1. Load the data into gc.target_data
		gc.load_target_measurement_data()
		plot_ping_count_cdf(gc.target_data)

		# # 2. Call the diagnostic plot to see what your dataset actually looks like
		# print("Generating Latency vs. Distance diagnostic plot...")
		# plot_latency_vs_distance(gc.target_data, os.path.join(FIG_DIR, "latency_vs_distance.pdf"))

		# 3. Run your geolocator simulation as normal
		gc.run()
