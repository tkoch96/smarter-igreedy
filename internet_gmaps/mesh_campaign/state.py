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
CREATE TABLE IF NOT EXISTS surrogates (
    prb_id INTEGER PRIMARY KEY, ip TEXT, hop_rtt_ms REAL,
    local_rtt_ms REAL, ts REAL);
CREATE TABLE IF NOT EXISTS pair_history (
    src INTEGER, dst INTEGER, min_rtt REAL, rtt2 REAL, msm_id INTEGER, ts REAL);
CREATE INDEX IF NOT EXISTS idx_pair_history_pair ON pair_history(src, dst);
"""

MAX_STRIKES = 3  # consecutive-ish failures before a probe is benched
MAX_SOL_VIOLATIONS = 5  # SOL-violating results before a location is distrusted

# -- re-measure feature (uncorroborated RTT floors) -------------------------
# A pair's min RTT is trusted only if a second packet lands within
# REMEASURE_GAP_MS of it; otherwise the floor is one lucky/unlucky packet
# and the pair is re-pinged. The canonical exported RTT is the min over the
# HISTORY_KEEP most recent observations, so a path change ages out of the
# mesh instead of pinning a stale low forever.
HISTORY_KEEP = 10
REMEASURE_GAP_MS = 5.0
REMEASURE_MAX_PER_WEEK = 3  # observations/week before we accept the pair as-is


class CampaignState:
    def __init__(self, path=DB):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        try:  # pre-remeasure databases lack the measurement-kind column
            self.db.execute(
                "ALTER TABLE measurements ADD COLUMN kind TEXT DEFAULT 'coverage'"
            )
        except sqlite3.OperationalError:
            pass

    # -- pairs ------------------------------------------------------------
    def attempted_pairs(self):
        """Unordered set of pairs already measured or in flight.

        Exception: a FAILED pair whose dst got a surrogate address AFTER
        the attempt is retryable — the address that failed is no longer
        the address we ping, so the failure says nothing about the pair.
        Failures newer than the surrogate (i.e. against the surrogate
        itself) stay blocked as usual."""
        surr_ts = dict(self.db.execute("SELECT prb_id, ts FROM surrogates"))
        out = set()
        for s, d, status, ts in self.db.execute(
            "SELECT src, dst, status, ts FROM pairs"
        ):
            if status == "failed" and surr_ts.get(d, 0) > (ts or 0):
                continue
            out.add((min(s, d), max(s, d)))
        return out

    def record_scheduled(self, msm_id, dst_prb, dst_ip, src_prbs, kind="coverage"):
        now = time.time()
        self.db.execute(
            "INSERT OR REPLACE INTO measurements "
            "(msm_id, dst_prb, dst_ip, n_src, created, done, kind) "
            "VALUES (?,?,?,?,?,0,?)",
            (msm_id, dst_prb, dst_ip, len(src_prbs), now, kind),
        )
        # Re-measures target pairs that are already ok; their rows must NOT
        # flip to pending (a lost re-ping would erase a good RTT). Their
        # results are attributed through pair_history instead.
        if kind == "coverage":
            # ON CONFLICT: the only pairs the scheduler re-issues are FAILED
            # attempts forgiven because the dst since got a surrogate address
            # (attempted_pairs); their row must follow the new measurement or
            # result attribution breaks
            self.db.executemany(
                "INSERT INTO pairs VALUES (?,?,'pending',NULL,?,?) "
                "ON CONFLICT(src, dst) DO UPDATE SET "
                "status='pending', min_rtt=NULL, msm_id=excluded.msm_id, ts=excluded.ts "
                "WHERE pairs.status='failed'",
                [(s, dst_prb, msm_id, now) for s in src_prbs],
            )
        self.db.commit()

    def record_result(self, src, dst, min_rtt, rtt2=None, msm_id=None, protect_ok=False):
        """rtt2 is the second-lowest packet RTT (None if only one packet
        answered) — the corroboration signal for the re-measure trigger.
        protect_ok: a failed RE-measure never demotes an ok pair."""
        status = "ok" if min_rtt is not None else "failed"
        if min_rtt is None and protect_ok:
            row = self.db.execute(
                "SELECT status FROM pairs WHERE src=? AND dst=?", (src, dst)
            ).fetchone()
            if row and row[0] == "ok":
                return
        self.db.execute(
            "UPDATE pairs SET status=?, min_rtt=? WHERE src=? AND dst=?",
            (status, min_rtt, src, dst),
        )
        if min_rtt is not None:
            self.db.execute(
                "INSERT INTO pair_history VALUES (?,?,?,?,?,?)",
                (src, dst, min_rtt, rtt2, msm_id, time.time()),
            )
            self.db.execute(
                "DELETE FROM pair_history WHERE src=? AND dst=? AND rowid NOT IN "
                "(SELECT rowid FROM pair_history WHERE src=? AND dst=? "
                "ORDER BY ts DESC LIMIT ?)",
                (src, dst, src, dst, HISTORY_KEEP),
            )
        self.db.commit()

    def remeasure_candidates(
        self,
        gap_ms=REMEASURE_GAP_MS,
        max_per_week=REMEASURE_MAX_PER_WEEK,
    ):
        """Ok pairs whose most recent observation has an uncorroborated min:
        the two lowest packets are > gap_ms apart, or only one packet
        answered (rtt2 NULL, ranked worst). Pairs already observed
        max_per_week times in the past 7 days are left alone — chronically
        noisy pairs keep their windowed min rather than burning budget.
        Returns [(src, dst, gap_or_inf)] worst-gap first."""
        ok = {(s, d) for s, d, _ in self.results("ok")}
        week_ago = time.time() - 7 * 86400
        latest, recent = {}, {}
        for s, d, mn, r2, _msm, ts in self.db.execute(
            "SELECT src, dst, min_rtt, rtt2, msm_id, ts FROM pair_history"
        ):
            if (s, d) not in ok:
                continue
            if ts > week_ago:
                recent[(s, d)] = recent.get((s, d), 0) + 1
            cur = latest.get((s, d))
            if cur is None or ts > cur[0]:
                latest[(s, d)] = (ts, mn, r2)
        out = []
        for (s, d), (_ts, mn, r2) in latest.items():
            if recent.get((s, d), 0) >= max_per_week:
                continue
            gap = float("inf") if r2 is None else r2 - mn
            if gap > gap_ms:
                out.append((s, d, gap))
        out.sort(key=lambda t: -t[2])
        return out

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
                "SELECT msm_id, dst_prb, dst_ip, kind FROM measurements "
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

    # -- surrogates ---------------------------------------------------------
    def record_surrogate(self, prb_id, ip, hop_rtt_ms, local_rtt_ms):
        self.db.execute(
            "INSERT OR REPLACE INTO surrogates VALUES (?,?,?,?,?)",
            (prb_id, ip, hop_rtt_ms, local_rtt_ms, time.time()),
        )
        self.db.commit()

    def surrogates(self):
        """{prb_id: surrogate ip} for probes whose dead listed address has
        a verified in-network stand-in (see surrogates.py)."""
        return dict(self.db.execute("SELECT prb_id, ip FROM surrogates"))

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
