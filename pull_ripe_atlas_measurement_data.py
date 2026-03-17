import os, tqdm, numpy as np
import glob
import time
import json
import bz2
import requests
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from pull_ripe_atlas_probe_data import RipeAtlasProbePipeline
from utils import *

DATA_DIR = "data"
FIG_DIR = "figures"

class RipeAtlasPipeline:
	def __init__(self, start_date, end_date, max_workers=2):
		self.start_date_str = start_date
		self.end_date_str = end_date
		self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
		self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
		self.max_workers = max_workers
		
		# Enforce directory structure
		self.raw_dir = os.path.join(DATA_DIR, "raw_dumps")
		self.parsed_dir = os.path.join(DATA_DIR, "parsed_dumps")
		os.makedirs(self.raw_dir, exist_ok=True)
		os.makedirs(self.parsed_dir, exist_ok=True)
		
		# Modern RIPE Atlas data-store endpoint
		self.base_url = "https://data-store.ripe.net/datasets/atlas-daily-dumps"

		# Load metadata once when the pipeline spins up
		print("Loading probe metadata...")
		probe_metadata_obj = RipeAtlasProbePipeline(start_date=self.start_date_str, end_date=self.end_date_str)
		self.probe_metadata = probe_metadata_obj.export_latest_probes()

	def _get_hourly_targets(self):
		"""Generate a list of (date_obj, hour) tuples for the requested range."""
		delta = self.end_date - self.start_date
		targets = []
		for i in range(delta.days + 1):
			current_date = self.start_date + timedelta(days=i)
			# RIPE generates 24 files per day (0000 to 2300)
			for hour in range(24):
				targets.append((current_date, hour))
		return targets

	def _build_url(self, target_date, hour):
		"""Construct the URL for the hourly data-store format."""
		date_str = target_date.strftime("%Y-%m-%d")
		hour_str = f"{hour:02d}00" 
		
		filename = f"ping-{date_str}T{hour_str}.bz2"
		url = f"{self.base_url}/{date_str}/{filename}"
		return url, filename

	def download_dump(self, target_tuple):
		"""Downloads the raw .bz2 file, but skips entirely if already parsed."""
		target_date, hour = target_tuple
		url, filename = self._build_url(target_date, hour)
		raw_path = os.path.join(self.raw_dir, filename)
		temp_path = raw_path + ".tmp"
		
		# NEW IDEMPOTENCY CHECK: Check if the final parsed file already exists!
		parsed_filename = filename.replace('.bz2', '_summary.json')
		parsed_path = os.path.join(self.parsed_dir, parsed_filename)
		
		if os.path.exists(parsed_path):
			# We already have the aggregated data. Skip downloading and processing.
			return None
		
		# Fallback idempotency check: fast skip download if the raw file exists from a previous aborted run
		if os.path.exists(raw_path) and os.path.getsize(raw_path) > 0:
			return raw_path

		max_retries = 3
		for attempt in range(max_retries):
			try:
				# Use a tuple for timeouts: (connect_timeout, read_timeout)
				with requests.get(url, stream=True, timeout=(15, 120)) as r:
					r.raise_for_status()
					
					# Write to a temporary file first
					with open(temp_path, 'wb') as f:
						# Increased chunk size to 1MB for better throughput
						for chunk in r.iter_content(chunk_size=1024 * 1024):
							f.write(chunk)
				
				# Download complete: rename temp file to final raw_path
				os.rename(temp_path, raw_path)
				return raw_path
				
			except requests.exceptions.HTTPError as e:
				# 404s usually mean RIPE just didn't generate a file for that hour
				if r.status_code == 404:
					print(f"Skipping {filename}: Not found on server (404).")
					break # No point retrying a 404
				print(f"HTTP Error for {filename}: {e}")
			except Exception as e:
				print(f"Attempt {attempt + 1}/{max_retries} failed for {filename}: {e}")
				if attempt < max_retries - 1:
					time.sleep(5 * (attempt + 1)) # Simple linear backoff
				else:
					print(f"Giving up on {filename} after {max_retries} attempts.")
		
		# Clean up the dangling temporary file if all attempts failed
		if os.path.exists(temp_path):
			os.remove(temp_path)
			
		return None

	def process_dump(self, raw_path):
		"""Streams the .bz2 archive, aggregates on the fly, and saves a tiny summary."""
		if not raw_path:
			return None
			
		filename = os.path.basename(raw_path)
		parsed_filename = filename.replace('.bz2', '_summary.json')
		parsed_path = os.path.join(self.parsed_dir, parsed_filename)
		
		# Idempotency: if we already summarized it, we don't need to do it again
		if os.path.exists(parsed_path):
			# Clean up the raw file if it was left behind from a previous run
			if os.path.exists(raw_path):
				os.remove(raw_path)
			return parsed_path

		hourly_summary = {}

		with bz2.open(raw_path, "rt") as f:
			for line in f:
				try:
					record = json.loads(line)
					src = record.get("src_addr", "")
					dst = record.get("dst_addr", "")
					probe = record.get("prb_id")
					rtt = record.get("min", -1)
					
					# Filter junk immediately
					if rtt == -1 or ':' in src or ':' in dst:
						continue
						
					# Get subnet routing
					probe_data = self.probe_metadata.get(probe)
					if not probe_data or probe_data.get('address_v4') is None:
						continue
						
					probe_24 = convert_32_to_24(probe_data['address_v4'])
					dst_24 = convert_32_to_24(dst)
					
					# Aggregate in memory
					if probe_24 not in hourly_summary:
						hourly_summary[probe_24] = {}
					if dst_24 not in hourly_summary[probe_24]:
						hourly_summary[probe_24][dst_24] = []
						
					hourly_summary[probe_24][dst_24].append(rtt)
					
				except (json.JSONDecodeError, KeyError, TypeError):
					continue
		
		# Save the tiny aggregated file
		with open(parsed_path, "w") as out_f:
			json.dump(hourly_summary, out_f)
			
		# CRITICAL SPACE SAVER: Delete the massive raw file now that we're done with it
		os.remove(raw_path)
		
		return parsed_path

	def export_latest_measurements(self):
		"""Merges the tiny summary files into a single export dictionary."""
		self.execute()
		export = {
			"meas": {},
		}
		
		# Loop through the highly compressed summary files
		for fn in glob.glob(os.path.join(self.parsed_dir, "*_summary.json")):
			with open(fn, 'r') as f:
				hourly_summary = json.load(f)
				
				# Merge the hourly summary into the master export dict
				for src_24, dst_dict in hourly_summary.items():
					if src_24 not in export["meas"]:
						export["meas"][src_24] = {}
						
					for dst_24, rtts in dst_dict.items():
						if dst_24 not in export["meas"][src_24]:
							export["meas"][src_24][dst_24] = []
							
						export["meas"][src_24][dst_24].extend(rtts)

		return export

	def load_parsed_target_data(self):
		"""Filters the dataset against physical limitations directly from disk to save RAM."""
		# Ensure pipeline has run before trying to load data
		self.execute()
		print("Loading target data...")
		# Build the location dictionary using the already-loaded metadata
		address_to_loc = {
			convert_32_to_24(probe['address_v4']): (probe['latitude'], probe['longitude']) 
			for probe in self.probe_metadata.values() 
			if (probe.get('address_v4') is not None and 
				probe.get('latitude') is not None and 
				probe.get('longitude') is not None)
		}

		full_mesh_probe_meas = {}
		min_rtt_cache = {} # Cache distance math to save CPU cycles

		# Read summary files one by one, keeping memory footprint tiny
		for fn in tqdm.tqdm(glob.glob(os.path.join(self.parsed_dir, "*_summary.json")), desc="Looking through jsons..."):
			with open(fn, 'r') as f:
				hourly_summary = json.load(f)
				
				for src, dst_dict in hourly_summary.items():
					# Skip immediately if we don't have location data
					if src not in address_to_loc:
						continue
						
					for dst, rtts in dst_dict.items():
						if dst not in address_to_loc:
							continue
							
						# Use cached minimum RTT or calculate it if it's our first time seeing this pair
						cache_key = (src, dst)
						if cache_key not in min_rtt_cache:
							dist_km = get_distance(address_to_loc[src], address_to_loc[dst])
							# Speed of light in fiber is approx 100km per 1ms
							min_rtt_cache[cache_key] = dist_km / 100.0
							
						min_possible_rtt_ms = min_rtt_cache[cache_key]
						
						# Filter out any measurements that violate physics
						valid_rtts = [rtt for rtt in rtts if rtt >= min_possible_rtt_ms]
						
						# Only store data we are actually keeping
						if valid_rtts:
							if src not in full_mesh_probe_meas:
								full_mesh_probe_meas[src] = {}
							if dst not in full_mesh_probe_meas[src]:
								full_mesh_probe_meas[src][dst] = np.min(valid_rtts)
							else:
								full_mesh_probe_meas[src][dst] = np.min(valid_rtts + [full_mesh_probe_meas[src][dst]])

		return {
			'address_to_loc': address_to_loc,
			'loc_loc_meas': full_mesh_probe_meas,
		}

	def execute(self):
		"""Orchestrates the downloading and parsing of hourly dumps."""
		targets = self._get_hourly_targets()
		raw_files = []
		
		print(f"--- Phase 1: Downloading {len(targets)} hourly chunks in parallel ---")
		with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
			future_to_target = {executor.submit(self.download_dump, t): t for t in targets}
			for future in as_completed(future_to_target):
				result = future.result()
				if result:
					raw_files.append(result)
					
		print(f"--- Phase 2: Parsing {len(raw_files)} archives in parallel ---")
		with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
			future_to_file = {executor.submit(self.process_dump, f): f for f in raw_files}
			for future in as_completed(future_to_file):
				future.result() 
				
		print("Pipeline Execution Complete.")

# Example Trigger
if __name__ == "__main__":
	# Test a single recent day to verify the hourly fetching logic
	pipeline = RipeAtlasPipeline(
		start_date="2026-02-24", 
		end_date="2026-02-24", 
		max_workers=2  # Kept lower to prevent out-of-memory or thermal throttling
	)
	
	# Instead of just execute(), let's run the whole chain to test the final output
	final_data = pipeline.load_parsed_target_data()
	print(f"Loaded {len(final_data['loc_loc_meas'])} valid source subnets.")