# mesh_campaign — diversity-first RIPE Atlas all↔all mesh

Grows a probe↔probe min-RTT mesh far beyond the free daily dumps, under
the account credit budget, most-informative pairs first. Verified live
2026-07-06: pilot msm 187132145-153, 45 pairs, 135 credits.

## Layout

- `atlas_api.py` — REST client (auth = `Authorization: Key` header; the
  old `?key=` query param is dead, and cousteau is in maintenance — plain
  requests). Key in `$RIPE_ATLAS_KEY` or `~/.ripe_atlas_key`.
- `inventory.py` — usable probes: connected + public v4 + geo + ASN +
  `system-ipv4-works` (12,123 of ~14.5k connected at last fetch).
- `scheduler.py` — tiered coverage greedy: T1 country pairs (all ~17k
  covered on day one), T1b endpoint rescue (never-successful (country,ASN)
  groups pinged as *sources* toward verified targets), T2 (country,ASN)
  pairs — the primary objective; ASN coverage rides along since every
  cc-asn pair is an ASN pair — T3 ASN pairs, T4 raw probe pairs; batches
  pairs dst-side into ≤100-source one-off pings.
- `state.py` — sqlite (crash-safe for cron): pair outcomes, in-flight
  measurements, probe health.
- `results.py` — result parsing + the trust machinery: src/dst strikes
  for silence (benched at 3 with zero successes), speed-of-light checks
  of claimed locations (benched at 5 violations; one bad probe can't
  bench its innocent partners since counts accumulate per-probe).
- `daily.py` — the cron entry: pull → parse → schedule → execute.
- `report.py` — pilot runner (isolated state db) + coverage projection.

## Run

```
python -m mesh_campaign.daily --budget-credits 300 --dry-run   # plan only
python -m mesh_campaign.daily --budget-credits 30000          # small real day
# crontab, once trusted (account limit 1M credits/day — stay under):
17 6 * * * cd <internet_gmaps> && <venv>/bin/python -m mesh_campaign.daily --budget-credits 300000 >> mesh_campaign/data/daily.log 2>&1
```

## Pilot findings (first 45 pairs, results final)

26 ok / 19 failed / 1 SOL violation — and the failure structure is the
lesson: **source-side success to live targets was 26/26 (100%)**; all 19
failures were toward 3 of the 10 never-screened destination addresses
(a Telstra mobile-range IP, and a RU probe that pings fine as a source
but whose own address never answers). Listed `address_v4` ≠ pingable
address for roughly a third of probes — consistent with the daily-dump
mesh keeping only ~909 of ~10k probes after bidirectional filtering.
The trust machinery also caught probe 1016203 claiming (-29.00, 24.00) —
the exact centroid of South Africa — via a speed-of-light violation.

Consequence, built in: **destination probation** (scheduler.py). An
address that has never answered gets at most PROBATION_MAX_SRC=5 sources;
one successful result promotes it to full 100-source batches. A dead
target burns 5 pairs, not 100, and screening is a free by-product of the
mesh itself. Expected steady-state pair failure after week one: low
single digits (source-side only).

## Deliberate limits

- RTTs are single-shot min-of-3; the free daily dumps stay the source for
  well-covered pairs — this campaign buys *coverage*, not repetition.
- Scheduler treats pairs as unordered (reverse direction ≈ no new info).
- Tier weights are static (no region-targeted tiers).
- The coverage projection does not model probation (day-1 country
  coverage really takes a few days while targets verify).

## Account limits (learned empirically 2026-07-06, refined 07-07)

The binding constraint is **100,000 results per ROLLING 24h window** (1
result = 1 src-dst pair) — not midnight-UTC days, and NOT the 1M credit
limit. At 3 packets/ping only ~300k credits/day are spendable. daily.py
enforces the rolling headroom automatically; a run started too soon after
yesterday's simply schedules fewer pairs.
Since credits are not binding, raising PING_PACKETS to 9-10 buys
min-of-10 RTTs (much tighter floors) for the same pair throughput.
