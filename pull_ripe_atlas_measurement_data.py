import os, glob, numpy as np
import bz2
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from pull_ripe_atlas_probe_data import RipeAtlasProbePipeline
from utils import *

DATA_DIR = "data"
FIG_DIR = "figures"

class RipeAtlasPipeline:
	def __init__(self, start_date, end_date, max_workers=8):
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
		"""Downloads the raw .bz2 file with a rapid idempotency check."""
		target_date, hour = target_tuple
		url, filename = self._build_url(target_date, hour)
		raw_path = os.path.join(self.raw_dir, filename)
		
		# Idempotency check: fast skip if already downloaded completely
		if os.path.exists(raw_path) and os.path.getsize(raw_path) > 0:
			return raw_path

		try:
			with requests.get(url, stream=True, timeout=30) as r:
				r.raise_for_status()
				with open(raw_path, 'wb') as f:
					for chunk in r.iter_content(chunk_size=8192):
						f.write(chunk)
			return raw_path
		except requests.exceptions.HTTPError as e:
			# RIPE occasionally misses an hour if their backend restarts
			print(f"Skipping {filename} (Not found or HTTP error): {e}")
			return None
		except Exception as e:
			print(f"Connection error for {filename}: {e}")
			return None

	def process_dump(self, raw_path):
		"""Streams the .bz2 archive and extracts required data."""
		if not raw_path:
			return None
			
		filename = os.path.basename(raw_path)
		parsed_filename = filename.replace('.bz2', '_parsed.json')
		parsed_path = os.path.join(self.parsed_dir, parsed_filename)
		
		# Idempotency check
		if os.path.exists(parsed_path):
			return parsed_path

		filtered_results = []
		with bz2.open(raw_path, "rt") as f:
			for line in f:
				try:
					try:
						record = json.loads(line)
						filtered_results.append({"dst": record["dst_addr"], "src": record["src_addr"],
							"probe": record["prb_id"], "rtt": record["min"]})	
						if np.random.random() > .9999:
							break
					except json.JSONDecodeError:
						continue
				except Exception as e:
					pass							
			with open(parsed_path, "w") as out_f:
				json.dump(filtered_results, out_f)
				
			return parsed_path
		

	def load_parsed_target_data(self):
		### returns probes
		## dst -> {id: probe id, loc: (lat,lon)}
		### returns measurements
		## id -> id -> (min) rtt measurement

		parsed_probe_measurements = self.export_latest_measurements()

		probe_metadata_obj = RipeAtlasProbePipeline(start_date="2026-01-01", end_date="2026-01-31")
		probe_metadata = probe_metadata_obj.export_latest_probes()

		probe_24s = {convert_32_to_24(prb['address_v4']): None for prb in probe_metadata.values() if prb['address_v4'] is not None}

		full_mesh_probe_meas = {}
		for src, m in parsed_probe_measurements['meas'].items():
			for dst in m:
				try:
					# This measurement is a measurement from a probe to a probe, keep
					probe_24s[src]
					probe_24s[dst]
					try:
						full_mesh_probe_meas[src]
					except KeyError:
						full_mesh_probe_meas[src] = {}
					full_mesh_probe_meas[src][dst] = m[dst]					
				except KeyError:
					pass

		return {
			'address_to_loc': {convert_32_to_24(probe['address_v4']): (probe['latitude'], probe['longitude']) for probe in probe_metadata.values() if (probe['address_v4'] is not None and probe['latitude'] is not None and probe['longitude'] is not None)},
			'loc_loc_meas': full_mesh_probe_meas,
		}


	def export_latest_measurements(self):
		## given parsed dumps, export src -> dst -> latency
		self.execute()
		probe_metadata_obj = RipeAtlasProbePipeline(start_date="2026-01-01", end_date="2026-01-31")
		probe_metadata = probe_metadata_obj.export_latest_probes()
		export = {
			"meas": {},
		}
		for fn in glob.glob(os.path.join(self.parsed_dir, "*")):
			all_data = json.load(open(fn, 'r'))
			for row in all_data:
				if ':' in row['src'] or ':' in row['dst']: continue # ignore v6 for now
				if row['rtt'] == -1: continue
				## get src /24 from the probe ID
				if probe_metadata[row['probe']]['address_v4'] is None: continue
				probe_24 = convert_32_to_24(probe_metadata[row['probe']]['address_v4'])
				dst_24 = convert_32_to_24(row['dst'])
				try:
					export["meas"][probe_24]
				except KeyError:
					export["meas"][probe_24] = {}
				try:
					export["meas"][probe_24][dst_24].append(row['rtt'])
				except KeyError:
					export["meas"][probe_24][dst_24] = [row['rtt']]

		return export

	def execute(self):
		### Pulls hourly dumps from ripe atlas, 
		## does some basic parsing to make them smaller
		targets = self._get_hourly_targets()[0:4]
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
		start_date="2026-01-24", 
		end_date="2026-01-24", 
		max_workers=6 
	)
	pipeline.execute()

	