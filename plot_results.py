import matplotlib.pyplot as plt
import numpy as np
from utils import get_distance

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
