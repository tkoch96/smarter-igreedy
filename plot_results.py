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
    
    print("Extracting latency and distance pairs...")
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


