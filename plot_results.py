import matplotlib.pyplot as plt

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