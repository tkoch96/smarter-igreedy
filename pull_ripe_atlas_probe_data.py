import os
import bz2
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

ProbeDict = dict[str, dict]


class RipeAtlasProbePipeline:
	def __init__(self, start_date: str, end_date: str, max_workers: int = 4) -> None:
		self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
		self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
		self.max_workers = max_workers

		self.raw_dir = "data/probe_data/raw_dumps"
		self.parsed_dir = "data/probe_data/parsed_dumps"
		os.makedirs(self.raw_dir, exist_ok=True)
		os.makedirs(self.parsed_dir, exist_ok=True)

		self.base_url = "https://ftp.ripe.net/ripe/atlas/probes/archive"

	def _get_daily_targets(self) -> list[datetime]:
		"""Generate a list of datetime objects for the requested range."""
		delta = self.end_date - self.start_date
		return [self.start_date + timedelta(days=i) for i in range(delta.days + 1)]

	def _build_url(self, target_date: datetime) -> tuple[str, str]:
		"""Construct the expected RIPE Atlas probe dump URL."""
		year = target_date.strftime("%Y")
		month = target_date.strftime("%m")
		date_str = target_date.strftime("%Y%m%d")

		filename = f"{date_str}.json.bz2"
		url = f"{self.base_url}/{year}/{month}/{filename}"
		return url, filename

	def download_dump(self, target_date: datetime) -> Optional[str]:
		"""Downloads the raw .bz2 file with a rapid idempotency check."""
		url, filename = self._build_url(target_date)
		raw_path = os.path.join(self.raw_dir, filename)

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
			print(f"Skipping {filename} (Not found or HTTP error): {e}")
			return None
		except Exception as e:
			print(f"Connection error for {filename}: {e}")
			return None

	def process_dump(self, raw_path: Optional[str]) -> Optional[str]:
		"""Reads the .bz2 archive and extracts required probe metadata."""
		if not raw_path:
			return None

		filename = os.path.basename(raw_path)
		date_str = filename.split('.')[0]
		parsed_filename = f"probes_{date_str}_parsed.json"
		parsed_path = os.path.join(self.parsed_dir, parsed_filename)

		if os.path.exists(parsed_path):
			return parsed_path

		filtered_probes: list[dict] = []
		try:
			with bz2.open(raw_path, "rt") as f:
				data = json.load(f)

			probe_list = data.get("objects", data) if isinstance(data, dict) else data

			for probe in probe_list:
				filtered_probes.append({
					"prb_id": probe.get("id"),
					"address_v4": probe.get("address_v4"),
					"address_v6": probe.get("address_v6"),
					"prefix_v4": probe.get("prefix_v4"),
					"prefix_v6": probe.get("prefix_v6"),
					"country": probe.get("country_code"),
					"latitude": probe.get("latitude"),
					"longitude": probe.get("longitude"),
					"asn_v4": probe.get("asn_v4"),
					"asn_v6": probe.get("asn_v6"),
					"status": probe.get("status_name"),
				})

			with open(parsed_path, "w") as out_f:
				json.dump(filtered_probes, out_f, indent=2)

			return parsed_path
		except Exception as e:
			print(f"Error processing {raw_path}: {e}")
			return None

	def export_latest_probes(self) -> ProbeDict:
		"""
		Reads all parsed probe dumps in chronological order and returns a dictionary
		mapping prb_id -> probe metadata. Newer files overwrite older probe entries.
		"""
		self.execute()
		probe_dict: ProbeDict = {}

		parsed_files = [f for f in os.listdir(self.parsed_dir) if f.endswith("_parsed.json")]
		parsed_files.sort()

		for filename in parsed_files:
			filepath = os.path.join(self.parsed_dir, filename)
			try:
				with open(filepath, 'r') as f:
					daily_probes: list[dict] = json.load(f)
					for probe in daily_probes:
						prb_id = probe.get("prb_id")
						if prb_id is not None:
							probe_dict[prb_id] = probe
			except Exception as e:
				print(f"Error reading {filepath}: {e}")

		print(f"Exported latest metadata for {len(probe_dict)} unique probes.")
		return probe_dict

	def execute(self) -> None:
		targets = self._get_daily_targets()
		raw_files: list[str] = []

		print(f"--- Phase 1: Downloading {len(targets)} daily probe dumps in parallel ---")
		with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
			future_to_target = {executor.submit(self.download_dump, t): t for t in targets}
			for future in as_completed(future_to_target):
				result = future.result()
				if result:
					raw_files.append(result)

		print(f"--- Phase 2: Parsing {len(raw_files)} probe archives in parallel ---")
		with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
			future_to_file = {executor.submit(self.process_dump, f): f for f in raw_files}
			for future in as_completed(future_to_file):
				future.result()

		print("Pipeline Execution Complete.")


if __name__ == "__main__":
	pipeline = RipeAtlasProbePipeline(
		start_date="2026-02-01",
		end_date="2026-02-28",
		max_workers=4,
	)
	pipeline.execute()
