# DAX Measures — TfL Semantic Model

All measures below are verified against the live semantic model. Grouped by the table they primarily query.

## `fact_disruption` — current-state KPIs (Network Overview page)

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
*% of lines on Good Service, evaluated at the latest timestamp within the current filter context — not a global/historical average. This filter-context-aware pattern is what makes the card respond correctly to date slicers.*

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
*Volume of active-flag polling rows in the current filter context — distinct from the latest-snapshot measures above.*

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

## `event_duration` — reliability & duration KPIs (Disruption Duration page)

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
```

## `mtbd` — line reliability

```dax
MTBD (minutes) = AVERAGE(mtbd[mtbd_minutes])
```

---

> Harmonized with the KQL severity-weighted ranking (§3 in `TECHNICAL_NOTES.md`): the two measures above originally excluded only three statuses, while the KQL real-time ranking excluded five. `Reduced Service` and `Special Service` were added here so both pipelines agree on what counts as a genuine disruption. See `TECHNICAL_NOTES.md` for the bug history behind the `VAR LatestTime` pattern used above.
