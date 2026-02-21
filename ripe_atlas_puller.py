import os
import bz2
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed

class RipeAtlasPipeline:
    def __init__(self, start_date, end_date, ipv="v4", subtype="builtin", max_workers=8):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.ipv = ipv
        self.subtype = subtype  # 'builtin' or 'udm' (user-defined measurements)
        self.max_workers = max_workers
        
        # Enforce directory structure
        self.raw_dir = "data/raw_dumps"
        self.parsed_dir = "data/parsed_dumps"
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.parsed_dir, exist_ok=True)
        
        # RIPE Atlas public data endpoint
        self.base_url = "https://ftp.ripe.net/ripe/atlas/data"

    def _get_date_range(self):
        """Generate a list of datetime objects for the requested range."""
        delta = self.end_date - self.start_date
        return [self.start_date + timedelta(days=i) for i in range(delta.days + 1)]

    def _build_url(self, target_date):
        """Construct the expected RIPE Atlas dump URL."""
        date_str = target_date.strftime("%Y-%m-%d")
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")
        
        filename = f"ping-{self.ipv}-{self.subtype}-{date_str}.bz2"
        # Note: RIPE historically nests these by YYYY/MM/DD. 
        url = f"{self.base_url}/{year}/{month}/{day}/{filename}"
        return url, filename

    def download_dump(self, date_obj):
        """Downloads the raw .bz2 file with a rapid idempotency check."""
        url, filename = self._build_url(date_obj)
        raw_path = os.path.join(self.raw_dir, filename)
        
        # Idempotency check: extremely fast skip if it's already on disk
        if os.path.exists(raw_path) and os.path.getsize(raw_path) > 0:
            return raw_path

        try:
            # stream=True is critical here so we don't load a 10GB archive into RAM
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(raw_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return raw_path
        except requests.exceptions.HTTPError as e:
            print(f"File not found or HTTP error for {url}: {e}")
            return None

    def process_dump(self, raw_path):
        """Streams the .bz2 archive and extracts required data."""
        if not raw_path:
            return None
            
        filename = os.path.basename(raw_path)
        parsed_filename = filename.replace('.bz2', '_parsed.json')
        parsed_path = os.path.join(self.parsed_dir, parsed_filename)
        
        # Idempotency check: skip parsing if the parsed file already exists
        if os.path.exists(parsed_path):
            return parsed_path

        filtered_results = []
        try:
            # RIPE data is Newline Delimited JSON. We stream it line-by-line.
            with bz2.open(raw_path, "rt") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        
                        # --- INSERT YOUR PARSING/FILTERING LOGIC HERE ---
                        # Example: Only keep measurements where average RTT > 50ms
                        if record.get("avg", 0) > 50:
                            filtered_results.append({
                                "prb_id": record.get("prb_id"),
                                "dst_addr": record.get("dst_addr"),
                                "avg_rtt": record.get("avg"),
                                "timestamp": record.get("timestamp")
                            })
                    except json.JSONDecodeError:
                        continue
                        
            # Dump the filtered data locally
            with open(parsed_path, "w") as out_f:
                json.dump(filtered_results, out_f)
                
            return parsed_path
        except Exception as e:
            print(f"Error processing {raw_path}: {e}")
            return None

    def execute(self):
        dates = self._get_date_range()
        raw_files = []
        
        print(f"--- Phase 1: Downloading {len(dates)} days in parallel ---")
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {executor.submit(self.download_dump, d): d for d in dates}
            for future in as_completed(future_to_date):
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
    # RIPE data is massive. Start with a 3-day window to test your filtering logic.
    pipeline = RipeAtlasPipeline(
        start_date="2025-10-01", 
        end_date="2025-10-31", 
        ipv="v4",
        max_workers=4 
    )
    pipeline.execute()