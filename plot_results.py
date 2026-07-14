import matplotlib.pyplot as plt
import numpy as np
from utils import get_distance


def plot_knob_grid_overview(arm_rows, knob_names, marginals, anchors,
                            output_filename):
	"""THE grid-search view: every arm ranked by final-budget mean error
	(top), the knob on/off indicator matrix aligned below it (scan what
	the good arms have in common), and each knob's paired marginal
	effect (right).

	arm_rows:   [(label, on_flags list[bool], mean_km, median_km)],
	            pre-sorted best→worst
	knob_names: row labels for the indicator matrix (len = len(on_flags))
	marginals:  {knob: (d_mean_km, d_median_km, on_wins, n_pairs)}
	anchors:    {label: (mean_km, median_km)} — horizontal reference lines
	"""
	n, K = len(arm_rows), len(knob_names)
	fig = plt.figure(figsize=(max(13, 0.22 * n + 7), 7))
	gs = fig.add_gridspec(2, 2, width_ratios=[3.2, 1.15],
	                      height_ratios=[2.4, 1], hspace=0.09, wspace=0.22)
	ax = fig.add_subplot(gs[0, 0])
	axm = fig.add_subplot(gs[1, 0], sharex=ax)
	axb = fig.add_subplot(gs[:, 1])

	x = np.arange(n)
	ax.plot(x, [r[2] for r in arm_rows], 'o', ms=4.5, color='tab:blue',
	        label='mean')
	ax.plot(x, [r[3] for r in arm_rows], 'o', ms=4.5, color='tab:orange',
	        label='median')
	anchor_colors = ['k', 'gray', 'tab:green', 'tab:purple']
	for i, (label, (am, amed)) in enumerate(anchors.items()):
		c = anchor_colors[i % len(anchor_colors)]
		ax.axhline(am, color=c, linestyle='--', linewidth=1.2,
		           label=f'{label} mean')
		ax.axhline(amed, color=c, linestyle=':', linewidth=1.2,
		           label=f'{label} median')
	vals = ([r[2] for r in arm_rows] + [r[3] for r in arm_rows]
	        + [v for pair in anchors.values() for v in pair])
	if max(vals) / max(min(vals), 1e-9) > 6:
		ax.set_yscale('log')
	ax.set_ylabel('error at final budget (km)')
	ax.grid(True, linestyle='--', alpha=0.5)
	ax.legend(fontsize=8, ncol=2)
	plt.setp(ax.get_xticklabels(), visible=False)

	mat = np.array([[1.0 if r[1][k] else 0.0 for r in arm_rows]
	                for k in range(K)])
	axm.imshow(mat, aspect='auto', cmap='Greys', vmin=0, vmax=1.4,
	           interpolation='nearest', extent=(-0.5, n - 0.5, K - 0.5, -0.5))
	axm.set_yticks(range(K))
	axm.set_yticklabels(knob_names, fontsize=9)
	axm.set_xticks(np.arange(-0.5, n, 1), minor=True)
	axm.set_yticks(np.arange(-0.5, K, 1), minor=True)
	axm.grid(which='minor', color='w', linewidth=0.5)
	axm.tick_params(which='minor', length=0)
	axm.set_xlabel('arms, ranked best → worst by mean error (dark = knob ON)')

	_marginal_bars(axb, marginals)

	plt.savefig(output_filename, dpi=300, bbox_inches='tight')
	plt.clf()
	plt.close()
	print(f"Knob-grid overview saved to {output_filename}", flush=True)


def _marginal_bars(ax, marginals):
	knobs = list(marginals)
	ypos = np.arange(len(knobs))
	ax.barh(ypos + 0.19, [marginals[k][0] for k in knobs], height=0.36,
	        label='mean effect')
	ax.barh(ypos - 0.19, [marginals[k][1] for k in knobs], height=0.36,
	        label='median effect')
	ax.set_yticks(ypos)
	ax.set_yticklabels([f"{k}  ({marginals[k][2]}/{marginals[k][3]} ON-wins)"
	                    for k in knobs], fontsize=9)
	ax.invert_yaxis()
	ax.axvline(0, color='k', linewidth=0.8)
	ax.set_xlabel('paired ON−OFF effect (km)\nnegative = knob helps')
	ax.set_title('Knob marginal effects')
	ax.legend(fontsize=8)
	ax.grid(True, axis='x', linestyle='--', alpha=0.5)


def plot_knob_grid(curves, marginals, output_filename):
	"""Knob-grid figure: mean + median error-vs-budget for a selected
	subset of arms (left, middle) and each knob's paired marginal effect
	at the final budget (right).

	curves:    {label: {'budgets': [...], 'mean': [...], 'median': [...],
	                    'style': optional matplotlib kwargs}}
	marginals: {knob: (d_mean_km, d_median_km, on_wins, n_pairs)}
	"""
	fig, axes = plt.subplots(1, 3, figsize=(17, 5))
	for label, c in curves.items():
		style = dict(linewidth=2, alpha=0.85)
		style.update(c.get('style', {}))
		axes[0].plot(c['budgets'], c['mean'], label=label, **style)
		axes[1].plot(c['budgets'], c['median'], label=label, **style)
	for ax, ttl in ((axes[0], 'Mean error'), (axes[1], 'Median error')):
		ax.set_title(f'{ttl} vs budget')
		ax.set_xlabel('# measurements')
		ax.set_ylabel('error (km)')
		ax.grid(True, linestyle='--', alpha=0.5)
	axes[0].legend(fontsize=8)

	_marginal_bars(axes[2], marginals)

	plt.tight_layout()
	plt.savefig(output_filename, dpi=300)
	plt.clf()
	plt.close()
	print(f"Knob-grid figure saved to {output_filename}", flush=True)

def plot_error_over_budget(results_data, output_filename):
	"""
	Plots the average geolocation error against the measurement budget.
	
	results_data: dict
		Format expected:
		{
			'geolocator_name': {
				'budgets': [100, 200, 300, ...],
				'errors': [5000.5, 3000.2, 1500.0, ...]
			},
			...
		}
	"""
	plt.figure(figsize=(10, 6))
	
	# Define a set of markers/line styles to distinguish multiple strategies visually
	markers = ['o', 's', '^', 'D', 'v', 'p', '*']
	
	for i, (name, data) in enumerate(results_data.items()):
		budgets = data['budgets']
		errors = data['errors']
		
		plt.plot(
			budgets, 
			errors, 
			label=name.capitalize(), 
			marker=markers[i % len(markers)], 
			linewidth=2, 
			markersize=6,
			alpha=0.8
		)

	# Formatting the plot for readability
	plt.title('Average Geolocation Error vs. Measurement Budget', fontsize=14, fontweight='bold')
	plt.xlabel('Measurement Budget (Number of Pings)', fontsize=12)
	plt.ylabel('Average Error (km)', fontsize=12)
	plt.grid(True, linestyle='--', alpha=0.6)
	plt.legend(fontsize=10, loc='upper right')
	plt.tight_layout()

	# Save and/or show
	plt.savefig(output_filename, dpi=300)
	print(f"\nPlot saved successfully to {output_filename}")
	# plt.show() # Uncomment if you are running this in a Jupyter Notebook or want a popup

def plot_floor_sweep(results, targets_grid, sources_grid, output_filename):
	"""'Perfect' floor vs target count, one color per source budget.
	Solid = NN floor (lowest-RTT measured VP — smart_perfect's full-coverage
	converged error), dotted = geometric floor (nearest measured VP — the
	bound for any VP-reporting estimator).  Faint markers = individual
	sampling seeds; lines join seed means."""
	colors = {n_src: c for n_src, c in zip(
		sources_grid, ['tab:red', 'tab:blue', 'tab:green', 'tab:purple', '0.4'])}
	fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6))
	for ax, stat, ttl in ((ax1, 'mean', 'mean error'),
	                      (ax2, 'med', 'median error')):
		for n_src in sources_grid:
			c = colors[n_src]
			lbl = f"{n_src or 'all'} sources"
			for kind, ls in (('nn', '-'), ('geo', ':')):
				key = f'{kind}_{stat}'
				ys = [np.mean(results[(n_src, n_t)][key]) for n_t in targets_grid]
				ax.plot(targets_grid, ys, color=c, ls=ls, marker='o', ms=3,
				        label=f"{lbl} ({'NN' if kind == 'nn' else 'geometric'} floor)")
				for n_t in targets_grid:
					ax.plot([n_t] * len(results[(n_src, n_t)][key]),
					        results[(n_src, n_t)][key],
					        ls='', marker='.', ms=2.5, color=c, alpha=0.35)
		ax.set_xlabel('number of targets included')
		ax.set_ylabel(f'"perfect" floor, {ttl} (km)')
		ax.set_xscale('log')
		ax.grid(alpha=0.3, which='both')
		ax.legend(fontsize=7)
	fig.suptitle('Full-coverage oracle floor vs target count — each source '
	             'budget = best-k sources (greedy facility location); '
	             'targets nested per seed', fontsize=10)
	fig.tight_layout()
	plt.savefig(output_filename, dpi=200, bbox_inches='tight')
	plt.close(fig)
	print(f"Floor sweep plot saved to {output_filename}")


def plot_latency_vs_distance(target_data, output_filename="latency_vs_distance.png"):
	"""
	Plots the actual measured minimum RTT against the true geographic distance.
	This helps you visualize the 'speed of light' floor and the variance in routing.
	"""
	loc_loc_meas = target_data.get('loc_loc_meas', {})
	address_to_loc = target_data.get('address_to_loc', {})
	
	distances = []
	rtts = []
	
	for src, dsts in loc_loc_meas.items():
		if src not in address_to_loc:
			continue
		src_loc = address_to_loc[src]
		
		for dst, rtt_list in dsts.items():
			if dst not in address_to_loc or not rtt_list:
				continue
			dst_loc = address_to_loc[dst]
			
			# Use the minimum RTT as the best proxy for propagation delay
			min_rtt = min(rtt_list)
			dist_km = get_distance(src_loc, dst_loc)
			
			distances.append(dist_km)
			rtts.append(min_rtt)

	if not distances:
		print("No valid src/dst location pairs found to plot.")
		return

	plt.figure(figsize=(10, 6))
	
	# Scatter the actual measurements (use high transparency 'alpha' to see density)
	plt.scatter(distances, rtts, alpha=0.1, color='blue', s=10, label='Actual Measurements')
	
	# Plot the theoretical "Speed of Light in Fiber" floor (approx 100km per 1ms)
	max_dist = max(distances)
	sol_x = np.array([0, max_dist])
	sol_y = sol_x / 100.0  
	plt.plot(sol_x, sol_y, color='red', linestyle='--', linewidth=2, label='SOL in Fiber (100km/ms)')
	
	plt.title('Empirical Latency vs. Geographic Distance', fontsize=14, fontweight='bold')
	plt.xlabel('Distance (km)', fontsize=12)
	plt.ylabel('Minimum RTT (ms)', fontsize=12)
	
	# Zoom in on the most relevant part of the graph (adjust these bounds if needed)
	plt.xlim(0, max_dist)
	plt.ylim(0, np.percentile(rtts, 99)) # Cut off the top 1% of massive outliers for readability
	
	plt.grid(True, linestyle='--', alpha=0.6)
	plt.legend(fontsize=12, loc='upper left')
	plt.tight_layout()
	
	plt.savefig(output_filename, dpi=300)
	plt.clf()
	plt.close()


def plot_ping_count_cdf(target_data, output_filename="figures/ping_count_cdf.pdf"):
	"""
	Plots the CDF of the number of unique destinations pinged by each source (Out-Degree),
	AND the CDF of the number of sources reaching each destination (In-Degree).
	This accurately visualizes the bi-directional 'meshiness' of the dataset.
	"""
	import matplotlib.pyplot as plt
	import numpy as np
	from collections import defaultdict
	
	loc_loc_meas = target_data.get('loc_loc_meas', {})
	
	# 1. Track both out-degree (per source) and in-degree (per destination)
	out_degrees = []
	in_degree_tracker = defaultdict(int)
	all_unique_sources = set(loc_loc_meas.keys())
	all_unique_dsts = set()
	
	for src, dsts in loc_loc_meas.items():
		valid_dsts_for_this_src = 0
		
		for dst, rtts in dsts.items():
			# Check if measurement is valid (handles both lists and floats depending on upstream parsing)
			if rtts is not None and (not isinstance(rtts, list) or len(rtts) > 0):
				valid_dsts_for_this_src += 1
				in_degree_tracker[dst] += 1
				all_unique_dsts.add(dst)
				
		out_degrees.append(valid_dsts_for_this_src)
		
	in_degrees = list(in_degree_tracker.values())
		
	if not out_degrees or not in_degrees:
		print("No valid measurement data found to plot CDF.")
		return

	# 2. Calculate the CDFs
	sorted_out = np.sort(out_degrees)
	p_out = np.arange(1, len(sorted_out) + 1) / len(sorted_out)
	
	sorted_in = np.sort(in_degrees)
	p_in = np.arange(1, len(sorted_in) + 1) / len(sorted_in)

	# 3. Plotting
	plt.figure(figsize=(10, 6))
	
	# Plot Out-Degree
	plt.step(sorted_out, p_out, where='post', color='#1f77b4', linewidth=2.5, 
			 label='Out-degree (Destinations reached per Source)')
			 
	# Plot In-Degree
	plt.step(sorted_in, p_in, where='post', color='#ff7f0e', linewidth=2.5, linestyle='-', 
			 label='In-degree (Sources reaching per Destination)')
	
	# Add vertical lines to show where a "Perfect Full Mesh" would be
	total_dsts = len(all_unique_dsts)
	total_srcs = len(all_unique_sources)
	
	if total_dsts > 0:
		plt.axvline(x=total_dsts, color='#1f77b4', linestyle='--', alpha=0.5, 
					linewidth=2, label=f'Ideal Out-degree ({total_dsts} Dsts)')
	if total_srcs > 0:
		plt.axvline(x=total_srcs, color='#ff7f0e', linestyle='--', alpha=0.5, 
					linewidth=2, label=f'Ideal In-degree ({total_srcs} Srcs)')

	# Formatting
	plt.title('Bi-Directional Mesh Density: In-Degree vs Out-Degree', fontsize=14, fontweight='bold')
	plt.xlabel('Number of Connections (Degree)', fontsize=12)
	plt.ylabel('CDF (Fraction of Nodes)', fontsize=12)
	
	# Set X-axis to start at 0 and end slightly past the max possible targets/sources
	max_val = max(max(sorted_out), max(sorted_in), total_dsts, total_srcs)
	plt.xlim(0, max_val * 1.05)
	plt.ylim(0, 1.05)
	
	plt.grid(True, linestyle='--', alpha=0.6)
	plt.legend(fontsize=11, loc='lower right')
	plt.tight_layout()
	
	plt.savefig(output_filename, dpi=300)
	plt.clf()
	plt.close()
	print(f"\nBi-directional CDF plot saved successfully to {output_filename}")
