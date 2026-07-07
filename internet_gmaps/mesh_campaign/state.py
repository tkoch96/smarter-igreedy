"""Campaign state: measured pairs, in-flight measurements, probe health.

sqlite so a daily cron can crash anywhere and resume; every mutation is
committed immediately. Pairs are stored directed (src probe pings dst
probe's address) but scheduling treats coverage as unordered — re-measuring
the reverse direction has little utility (caveat 4).
"""

import sqlite3
import time
from pathlib import Path

DB = Path(__file__).parent / "data" / "state.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS pairs (
    src INTEGER, dst INTEGER, status TEXT, min_rtt REAL, msm_id INTEGER, ts REAL,
    PRIMARY KEY (src, dst));
CREATE TABLE IF NOT EXISTS measurements (
    msm_id INTEGER PRIMARY KEY, dst_prb INTEGER, dst_ip TEXT, n_src INTEGER,
    created REAL, done INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS probe_health (
    prb_id INTEGER PRIMARY KEY, src_strikes INTEGER DEFAULT 0,
    dst_strikes INTEGER DEFAULT 0, ok_results INTEGER DEFAULT 0,
    sol_violations INTEGER DEFAULT 0);
"""

MAX_STRIKES = 3  # consecutive-ish failures before a probe is benched
MAX_SOL_VIOLATIONS = 5  # SOL-violating results before a location is distrusted


class CampaignState:
    def __init__(self, path=DB):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)

    # -- pairs ------------------------------------------------------------
    def attempted_pairs(self):
        """Unordered set of pairs already measured or in flight."""
        out = set()
        for s, d in self.db.execute("SELECT src, dst FROM pairs"):
            out.add((min(s, d), max(s, d)))
        return out

    def record_scheduled(self, msm_id, dst_prb, dst_ip, src_prbs):
        now = time.time()
        self.db.execute(
            "INSERT OR REPLACE INTO measurements VALUES (?,?,?,?,?,0)",
            (msm_id, dst_prb, dst_ip, len(src_prbs), now),
        )
        self.db.executemany(
            "INSERT OR IGNORE INTO pairs VALUES (?,?,'pending',NULL,?,?)",
            [(s, dst_prb, msm_id, now) for s in src_prbs],
        )
        self.db.commit()

    def record_result(self, src, dst, min_rtt):
        status = "ok" if min_rtt is not None else "failed"
        self.db.execute(
            "UPDATE pairs SET status=?, min_rtt=? WHERE src=? AND dst=?",
            (status, min_rtt, src, dst),
        )
        self.db.commit()

    def results(self, status="ok"):
        return list(
            self.db.execute(
                "SELECT src, dst, min_rtt FROM pairs WHERE status=?", (status,)
            )
        )

    def verified_dsts(self):
        """Probes whose address has answered at least one mesh ping —
        eligible for full-size batches (unverified dsts stay on probation)."""
        return {
            d for (d,) in self.db.execute(
                "SELECT DISTINCT dst FROM pairs WHERE status='ok'"
            )
        }

    # -- measurements -----------------------------------------------------
    def open_measurements(self, older_than_s=0.0):
        cutoff = time.time() - older_than_s
        return list(
            self.db.execute(
                "SELECT msm_id, dst_prb, dst_ip FROM measurements "
                "WHERE done=0 AND created < ?",
                (cutoff,),
            )
        )

    def close_measurement(self, msm_id):
        self.db.execute("UPDATE measurements SET done=1 WHERE msm_id=?", (msm_id,))
        # anything still pending on a closed measurement failed
        self.db.execute(
            "UPDATE pairs SET status='failed' WHERE msm_id=? AND status='pending'",
            (msm_id,),
        )
        self.db.commit()

    # -- probe health -------------------------------------------------------
    def _bump(self, prb_id, column, amount=1):
        self.db.execute(
            "INSERT INTO probe_health (prb_id) VALUES (?) "
            "ON CONFLICT(prb_id) DO NOTHING",
            (prb_id,),
        )
        self.db.execute(
            f"UPDATE probe_health SET {column} = {column} + ? WHERE prb_id=?",
            (amount, prb_id),
        )
        self.db.commit()

    def strike_src(self, prb_id):
        self._bump(prb_id, "src_strikes")

    def strike_dst(self, prb_id):
        self._bump(prb_id, "dst_strikes")

    def credit_ok(self, prb_id):
        self._bump(prb_id, "ok_results")

    def sol_violation(self, prb_id):
        self._bump(prb_id, "sol_violations")

    def benched_probes(self):
        """Probes to exclude: too many failures with no successes, or
        locations contradicted by speed-of-light violations."""
        out = set()
        q = self.db.execute(
            "SELECT prb_id, src_strikes, dst_strikes, ok_results, sol_violations "
            "FROM probe_health"
        )
        for prb, s_strikes, d_strikes, ok, sol in q:
            if sol >= MAX_SOL_VIOLATIONS:
                out.add(prb)
            elif ok == 0 and max(s_strikes, d_strikes) >= MAX_STRIKES:
                out.add(prb)
        return out
