# Technical Deep Dive — Architecture Decisions, Queries & Measures

This document holds the full reasoning, queries, and measure logic behind the TfL Analytics Platform. It exists so the *why* behind every non-obvious decision is written down once, instead of living only in memory — useful both as project documentation and as interview preparation.

---

## 1. Why a Fact Constellation Schema?

The Gold layer began as a plain star schema:

```
fact_disruption (one row per 15-min poll, per line)
  ├── dim_line
  └── dim_date
```

This is correct for "what was line X's status at time Y?" — but it's the wrong grain for "how long did this disruption actually last?" A single real disruption that lasted 90 minutes would appear as **6 separate rows** in `fact_disruption` (one per poll), so naively counting rows or summing durations at that grain over-counts and under-represents true event duration.

**Solution:** add a second fact table, `event_duration`, at *event grain* — one row per real, deduplicated disruption — sharing the same `dim_line` dimension as `fact_disruption`. This is a textbook Fact Constellation Schema: two facts, different grain, shared dimension. A third fact table, `mtbd` (Mean Time Between Disruptions), aggregates `event_duration` further to a per-line reliability KPI.

**Why not just fix the grain in one table?** Because both grains are genuinely useful for different questions — "what's the live status" needs poll-level granularity; "how reliable is this line" needs event-level granularity. Forcing one table to serve both would mean either losing live-status fidelity or re-deriving event boundaries on every visual.

---

## 2. Event Deduplication Logic (the `is_new_event` problem)

**The problem:** the API is a snapshot poller, not a true event stream. The same disruption appears as a new row every 15 minutes for as long as it lasts. To compute true event duration, consecutive polls with the *same status, same line* need to be collapsed into one event with a single start and end time.

**Where it was built — and why it moved.** This was first attempted as a Power BI calculated column. That failed because **Direct Lake storage mode does not support calculated columns** — Direct Lake reads Delta tables directly from the Lakehouse without a full import/transformation step, so anything requiring row-by-row derived logic has to be computed *before* the data reaches the semantic model.

**Solution:** the deduplication logic was implemented in the **Gold layer PySpark notebook** (`03_transform_gold`), using a window function (`lag()`) to compare each row to the *previous* row for the same line, ordered by timestamp. A new event begins whenever either `status_description` **or** `disruption_reason` changes from the previous row (or there is no previous row) — checking both fields, not just status, means a genuinely new disruption with a different stated reason is still detected as a new event even if the severity label happens to be the same.

```python
window_spec = Window.partitionBy("line_id").orderBy("ingestion_timestamp")

fact_disruption = df_silver \
    .withColumn("date_id", date_format(col("ingestion_timestamp"), "yyyyMMddHHmm")) \
    .withColumn("prev_status", lag("status_description").over(window_spec)) \
    .withColumn("prev_reason", lag("disruption_reason").over(window_spec)) \
    .withColumn(
        "is_new_event",
        when(
            (col("prev_status").isNull()) |
            (col("prev_status") != col("status_description")) |
            (col("prev_reason") != col("disruption_reason")),
            1
        ).otherwise(0)
    ) \
    .select(
        monotonically_increasing_id().alias("fact_id"),
        col("line_id"), col("date_id"), col("status_severity"), col("status_description"),
        col("disruption_reason"), col("disruption_from"), col("disruption_to"),
        col("is_active_disruption"), col("disruption_category"), col("closure_text"),
        col("is_new_event"), col("ingestion_timestamp")
    )
```

`event_duration` is then built by taking a **running sum** of `is_new_event` over the same window to produce a stable `event_group` ID per real disruption, then grouping by `(line_id, event_group)` to get the true start/end of each event:

```python
df_with_groups = fact_disruption.withColumn(
    "event_group", spark_sum("is_new_event").over(window_spec)
)

event_duration = df_with_groups.groupBy("line_id", "event_group") \
    .agg(
        spark_min("ingestion_timestamp").alias("event_start"),
        spark_max("ingestion_timestamp").alias("event_end"),
        spark_min("status_description").alias("status_description"),
        spark_min("disruption_reason").alias("disruption_reason")
    ) \
    .withColumn("duration_minutes", (unix_timestamp("event_end") - unix_timestamp("event_start")) / 60) \
    .withColumn("date_id", date_format(col("event_start"), "yyyyMMddHHmm")) \
    .join(dim_line.select("line_id", "line_name"), on="line_id", how="left")
```

The `mtbd` table (Mean Time Between Disruptions per line) is registered as a Gold Delta table downstream of `event_duration`, but its exact build script is a separate step not reproduced here — document it the same way if you want full parity in this file.

This same conceptual pattern (detect a change → cumulative-sum the change flag → group) was reused later in KQL using `prev()` and `row_cumsum()`, when the question came up of whether the real-time heatmap could reflect exact disruption boundaries instead of fixed time buckets (see §4).

**A real, code-confirmed limitation worth noting:** in the Silver transformation (`02_transform_silver`), only the *first* element of the `lineStatuses` array is parsed (`col("parsed.lineStatuses")[0][...]`). The TfL API can in principle return multiple simultaneous status entries for one line; if that happens, only the first is captured and any secondary status/reason is silently dropped. In practice this is rare, but it's an honest limitation to list rather than something to claim doesn't exist.

---

## 3. Severity-Weighted Ranking System

Used identically in both the **Disruption Timeline heatmap** and the **Most Affected Lines bar chart** on the KQL real-time dashboard, so the two visuals can never disagree about which lines are worst-affected.

**Logic:**
1. Exclude non-genuine disruption statuses: `Good Service`, `Planned Closure`, `Service Closed`, `Reduced Service`, `Special Service`
2. Weight the remaining statuses by severity:
   - `Severe Delays` → 3
   - `Part Closure` / `Part Suspended` → 2
   - everything else genuine (e.g. `Minor Delays`) → 1
3. Sum the weighted severity per line over the time window to produce a ranking, worst to best

**Rationale:** a simple disruption *count* treats one severe closure the same as one minor delay, which doesn't reflect real operational impact. Weighting means, for example, **10 minor delays ≈ 3 severe delays** in cumulative impact — which is a fairer, more realistic comparison.

---

## 4. KQL Queries (Real-Time Dashboard)

### Disruption Timeline (heatmap) — final validated version
```kusto
let SeverityByLine = LineStatus
| where timestamp between (startTime .. endTime)
| where status !in (
    "Good Service", "Planned Closure", "Service Closed",
    "Reduced Service", "Special Service"
)
| extend Severity = case(
    status == "Severe Delays", 3,
    status in ("Part Closure", "Part Suspended"), 2,
    1
)
| summarize MaxSeverity = max(Severity) by line_name, bin(timestamp, 15m);
let LineRank = SeverityByLine
| summarize TotalSeverity = sum(MaxSeverity) by line_name
| order by TotalSeverity desc, line_name asc
| project line_name, rank = row_number();
SeverityByLine
| join kind=inner LineRank on line_name
| project line_name, timestamp, MaxSeverity, rank
| order by rank asc, timestamp asc
```

### Most Affected Lines (bar chart) — final validated version
```kusto
LineStatus
| where timestamp between (startTime .. endTime)
| where status !in (
    "Good Service", "Planned Closure", "Service Closed",
    "Reduced Service", "Special Service"
)
| extend Severity = case(
    status == "Severe Delays", 3,
    status in ("Part Closure", "Part Suspended"), 2,
    1
)
| summarize MaxSeverity = max(Severity) by line_name, bin(timestamp, 15m)
| summarize TotalSeverityScore = sum(MaxSeverity) by line_name
| order by TotalSeverityScore asc, line_name desc
```

**Three real issues found and fixed during development of these two queries — all good interview anecdotes:**

1. **Missing time filter.** The bar chart query originally had no time filter at all, so it silently aggregated the *entire* history of the table, while the heatmap was scoped to the dashboard's selected time range. Fixed by connecting both queries to the dashboard's `Time range` parameter (`startTime`/`endTime`).
2. **Opposite visual-rendering conventions (the subtle one).** After fixing (1), the two visuals still disagreed on display order. The cause wasn't a sorting bug in the traditional sense — it's that the **heatmap visual renders the first row of the query result at the bottom of the chart, while the bar chart visual renders its first row at the top.** Making both queries' `order by` clauses textually identical (the first fix attempted) actually produces *opposite* visual order, because each visual interprets row order differently. The correct fix is **opposite `order by` directions** in the two queries (`desc` for the heatmap's ranking step, `asc` for the bar chart), which then renders identically on screen. This is a non-obvious platform behavior worth knowing.
3. **Non-deterministic tie-breaking.** Lines with exactly equal severity scores don't have a guaranteed order unless a secondary sort key is specified. An explicit `line_name` tie-break was added to both queries — `asc` for the heatmap's ranking step, `desc` for the bar chart — chosen so that, combined with each visual's opposite rendering convention, tied lines still land in the same relative position on screen in both visuals.

### Live state queries — verified

```kusto
// Active Disruptions Now
LineStatus
| summarize arg_max(timestamp, status) by line_name
| where status != "Good Service"
| summarize ActiveDisruptions = dcount(line_name)
```

```kusto
// Network Health %
LineStatus
| summarize arg_max(timestamp, status) by line_name
| summarize
    Total = dcount(line_name),
    Good = dcountif(line_name, status == "Good Service")
| extend HealthPct = round(todouble(Good) / todouble(Total) * 100, 1)
| project HealthPct
```

```kusto
// Live Line Status (multi-stat visual, all 11 lines)
LineStatus
| summarize arg_max(timestamp, status, reason) by line_name
| project line_name, status, reason
| order by line_name asc
```

All three use `arg_max(timestamp, ...) by line_name` to pull each line's *latest* known state rather than aggregating historically — essential, since these describe "right now," not "across the whole window." This is the same pattern used for the `Last Update` freshness indicator and the `Active Disruptions` detail table (see `kql/` folder for all seven dashboard queries).

**A real ordering bug to watch for:** during development, the bar chart's `order by` was once found with the opposite direction and tie-break to the heatmap's (`asc` + `line_name desc` instead of `desc` + `line_name asc`), silently reintroducing the exact inconsistency described above. Both queries' final `order by` clause must always match exactly — this is exactly the kind of regression that's easy to miss without a side-by-side check.

### Considered but not implemented: exact event boundaries on the heatmap
A heatmap is fundamentally a fixed grid (line × time bucket) — it can't natively display variable-length intervals. Two options were weighed:
- **Chosen:** shrink the bucket size from 1 hour to 15 minutes (matching the actual polling interval), which is the most precision a snapshot-based heatmap can honestly offer.
- **Considered, not built:** reconstruct exact event start/end times in KQL (mirroring the PySpark `is_new_event` logic, using `prev()` + `row_cumsum()`) and feed that into a separate table/list visual rather than the heatmap, since heatmaps can't render arbitrary-length intervals anyway. Left as a future enhancement — the 15-minute bucket already matches the data's true resolution.

---

## 5. DAX Measures (Power BI, Batch Dashboard) — verified formulas

```dax
Network Health % =
VAR LatestTime = MAX(fact_disruption[ingestion_timestamp])
RETURN
DIVIDE(
    CALCULATE(
        DISTINCTCOUNT(fact_disruption[line_id]),
        fact_disruption[status_description] = "Good Service",
        fact_disruption[ingestion_timestamp] = LatestTime
    ),
    CALCULATE(
        DISTINCTCOUNT(fact_disruption[line_id]),
        fact_disruption[ingestion_timestamp] = LatestTime
    ),
    0
)
```
Evaluated against the **latest timestamp in the current filter context** (not a global max), which is what makes this respond correctly to date slicers — this was a real bug, fixed during development (see below).

```dax
Good Service Lines =
VAR LatestTime = CALCULATE(MAX(fact_disruption[ingestion_timestamp]))
RETURN
CALCULATE(
    DISTINCTCOUNT(fact_disruption[line_id]),
    fact_disruption[status_description] = "Good Service",
    fact_disruption[ingestion_timestamp] = LatestTime
)
```

```dax
Active Disruptions =
CALCULATE(
    COUNTROWS(fact_disruption),
    fact_disruption[is_active_disruption] = TRUE()
)
```
Note: this counts raw `fact_disruption` rows flagged active, across whatever filter context is applied (e.g. the whole table, or a selected line) — it is a *volume of active-flag polling rows* metric, distinct from `Network Health %` / `Good Service Lines`, which are deliberately pinned to the latest timestamp only. Worth being able to explain this distinction clearly if asked in an interview.

```dax
Total Disruptions = COUNTROWS(fact_disruption)

Latest Ingestion Timestamp = MAX(fact_disruption[ingestion_timestamp])

Unique Disruption Events =
CALCULATE(
    SUM(fact_disruption[is_new_event]),
    fact_disruption[status_description] <> "Good Service"
)

Unique Events By Type =
SUMX(
    VALUES(fact_disruption[line_id]),
    CALCULATE(SUM(fact_disruption[is_new_event]))
)
```

```dax
Avg Disruption Duration (mins) =
CALCULATE(
    AVERAGE(event_duration[duration_minutes]),
    event_duration[status_description] <> "Good Service",
    event_duration[status_description] <> "Planned Closure",
    event_duration[status_description] <> "Service Closed",
    event_duration[status_description] <> "Reduced Service",
    event_duration[status_description] <> "Special Service"
)

Total Events (Duration) =
CALCULATE(
    COUNTROWS(event_duration),
    event_duration[status_description] <> "Good Service",
    event_duration[status_description] <> "Planned Closure",
    event_duration[status_description] <> "Service Closed",
    event_duration[status_description] <> "Reduced Service",
    event_duration[status_description] <> "Special Service"
)

Max Duration Minutes = MAX(event_duration[duration_minutes])

% Severe Delay Duration =
DIVIDE(
    CALCULATE(
        SUM(event_duration[duration_minutes]),
        event_duration[status_description] = "Severe Delays"
    ),
    SUM(event_duration[duration_minutes]),
    0
)

MTBD (minutes) = AVERAGE(mtbd[mtbd_minutes])
```

**A real inconsistency found and fixed:** the KQL severity-weighted ranking (§3) excludes five non-genuine statuses (`Good Service`, `Planned Closure`, `Service Closed`, `Reduced Service`, `Special Service`), but `Avg Disruption Duration` and `Total Events (Duration)` originally only excluded three — `Reduced Service` and `Special Service` were being counted as real disruptions in the batch dashboard while being correctly excluded in the real-time ranking. Harmonized by adding the two missing exclusions to both DAX measures, so batch and real-time now agree on what counts as a genuine disruption.

**A real bug fixed here:** `Network Health %` and `Lines with Good Service` were initially calculated against the *latest row in the whole table*, regardless of any date filter applied on the report page — so selecting a past date didn't change these two cards while every other visual on the page updated correctly. Fixed by changing the measures to find the latest timestamp *within the current filter context* instead of globally (the `VAR LatestTime = MAX(...)` pattern above, which respects whatever filters are applied).

---

## 6. Known Limitations (full detail)

1. **Disruption reason categorisation.** `disruption_reason` is free-text and not standardised, so it can't be reliably grouped for cost/cause analysis without NLP-based categorisation (a plausible future improvement, e.g. via an LLM API).
2. **No real cost data.** The TfL API has no passenger-impact or cost figures (e.g. replacement bus costs, contractual penalties). `duration_minutes` is used as an honest proxy, not a substitute.
3. **Polling granularity.** Data is collected every 15 minutes; disruptions shorter than that may not be captured at all, and some recorded events show `duration_minutes = 0` for this reason.
4. **One week of data.** Insufficient to detect genuinely chronic, long-term patterns — at least a month would be needed for that kind of claim.
5. **Line-level only.** The API reports status per line, not per station, so station-level analysis isn't possible with this data source.
6. **UTC vs BST.** Timestamps in the Eventhouse are stored in UTC; during British Summer Time this creates a visible one-hour offset against local wall-clock time on the dashboard. Identified, documented, and intentionally left as a known limitation rather than patched, to avoid introducing a fragile timezone-conversion layer for a portfolio-scale project.
7. **Only the first `lineStatuses` entry is parsed.** The Silver transformation reads index `[0]` of the API's `lineStatuses` array. On the rare occasion a line reports more than one simultaneous status, any entry beyond the first is silently dropped.

---

## 7. Architecture Decision Log (summary)

| Decision | Reason |
|---|---|
| Fact Constellation Schema | Separate polling-grain from event-grain data cleanly (§1) |
| Event dedup logic lives in PySpark, not DAX | Direct Lake doesn't support calculated columns (§2) |
| Severity-weighted ranking, shared between two visuals | Fair comparison + guaranteed visual consistency (§3) |
| 15-min KQL bucket instead of exact event reconstruction | Matches true data resolution without over-engineering a heatmap that can't show variable intervals anyway (§4) |
| Deterministic tie-breaking on both ranking queries, with intentionally opposite `order by` directions | Prevents two independently-evaluated visuals from silently disagreeing on tied values — and, because the heatmap and bar chart visuals render result-row order in opposite directions, matching displayed order required opposite query-level sort directions, not identical ones (§4) |
| Dual independent pipelines (batch + streaming) from one source | Creates a built-in cross-validation check on data integrity |
| Harmonized DAX exclusion list with KQL exclusion list | Two independently-built measures (batch) and queries (real-time) had silently drifted apart on what counts as a "genuine disruption" — found and fixed for consistency (§5) |
