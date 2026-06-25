# 🚇 TfL London Underground Real-Time Analytics Platform

A dual-pipeline analytics platform built on **Microsoft Fabric** that monitors and analyses London Underground service disruptions — combining a **batch (historical)** pipeline for trend analysis with a **real-time (streaming)** pipeline for live operational monitoring.

> Built as an end-to-end portfolio project covering the full Medallion Architecture, dimensional modelling, semantic layer (DAX), and real-time KQL analytics on Microsoft Fabric.

---

## 📌 Business Problem

London Underground riders and operations teams need to answer two different kinds of questions:

- **"What's happening right now?"** → answered by the **real-time dashboard**
- **"What patterns exist over time, and which lines are most reliability-challenged?"** → answered by the **batch dashboard**

This project deliberately builds *both*, using two independent pipelines fed from the same source — which also creates a natural cross-check: if the batch and real-time numbers agree, the data pipeline is trustworthy.

---

## 🏗️ Architecture

```
                         TfL Unified API (polled every 15 min)
                                      │
                ┌─────────────────────┴─────────────────────┐
                │                                            │
         BATCH PIPELINE                              STREAMING PIPELINE
                │                                            │
        ┌───────▼────────┐                          ┌────────▼─────────┐
        │ Bronze Lakehouse│                          │  Eventstream      │
        │ (raw JSON)      │                          │  (Python →        │
        └───────┬────────┘                          │  azure-eventhub)   │
                │ PySpark                            └────────┬─────────┘
        ┌───────▼────────┐                                    │
        │ Silver Lakehouse│                          ┌────────▼─────────┐
        │ (cleaned, dedup)│                          │  Eventhouse (KQL)  │
        └───────┬────────┘                          │  Table: LineStatus │
                │ PySpark (window fn: lag())         └────────┬─────────┘
        ┌───────▼─────────────┐                                │
        │ Gold Lakehouse       │                       ┌────────▼─────────┐
        │ Fact Constellation:  │                       │ KQL Real-Time     │
        │ fact_disruption      │                       │ Dashboard          │
        │ event_duration        │                       │ (auto-refresh)     │
        │ mtbd                  │                       └───────────────────┘
        │ dim_line / dim_date   │
        └───────┬───────────────┘
                │ Semantic Model (DAX)
        ┌───────▼───────────────┐
        │ Power BI Direct Lake   │
        │ Dashboard (4 pages)    │
        └────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | TfL Unified API | Live line status data, polled every 15 min |
| Orchestration | Fabric Notebook Scheduler | Automated pipeline runs |
| Storage — Raw | Fabric Lakehouse (Bronze) | Raw JSON, preserved for auditability |
| Storage — Clean | Fabric Lakehouse (Silver) | Parsed, deduplicated Delta tables |
| Storage — Analytics | Fabric Lakehouse (Gold) | Fact Constellation Schema |
| Transformation | PySpark (Fabric Notebooks) | Parsing, deduplication, event reconstruction |
| Streaming | Azure Event Hubs protocol + Fabric Eventstream | Real-time ingestion |
| Time-series store | Fabric Eventhouse (KQL Database) | Real-time data store |
| Query language | KQL (Kusto Query Language) | Real-time analytics |
| Semantic layer | Fabric Semantic Model | Relationships & DAX measures |
| BI — Batch | Power BI (Direct Lake) | Historical / trend dashboards |
| BI — Real-time | KQL Real-Time Dashboard | Live operational monitoring |

---

## 📊 Data Model

The Gold layer started as a simple star schema (`fact_disruption`, `dim_line`, `dim_date`) and was deliberately extended into a **Fact Constellation Schema** by adding two more fact tables that share `dim_line`:

| Table | Grain | Purpose |
|---|---|---|
| `fact_disruption` | One row per 15-min poll | Raw disruption status over time |
| `event_duration` | One row per *real disruption event* (deduplicated) | True start/end time and duration of each disruption |
| `mtbd` | One row per line | Mean Time Between Disruptions — a reliability KPI |

**Why two fact tables instead of one?** Polling snapshots and real-world disruption events are different things — a single disruption can span many 15-minute polls. Collapsing everything into one fact table would force a single granularity onto two genuinely different questions ("what was the status at time X?" vs "how long did this specific disruption actually last?"). Full rationale and the deduplication logic are in [`TECHNICAL_NOTES.md`](./TECHNICAL_NOTES.md).

---

## 📈 Dashboards

### Batch — Power BI (Direct Lake), 4 pages
1. **Network Overview** — live network health snapshot, KPI cards
2. **Disruption Analysis** — disruption trends by line, reason, time of day
3. **Line Deep Dive** — per-line drill-through detail
4. **Disruption Duration** — Top 10 Longest Disruptions, MTBD, % Severe Delay Duration, Avg Disruption Duration

### Real-time — KQL Real-Time Dashboard
- **Live Line Status** — multi-stat view of all 11 lines with conditional colour formatting
- **Active Disruptions Now** / **Network Health %** — current-state KPIs (using `arg_max` to always reflect the *latest* poll, not a historical aggregate)
- **Disruption Timeline** (heatmap) and **Most Affected Lines** (bar chart) — both driven by an identical **severity-weighted ranking system**, so the two visuals never contradict each other (see Key Decisions below)
- **Active Disruptions** table

**Cross-validation:** during development, the batch and real-time pipelines independently agreed on Network Health % at the same point in time — a good signal that the data pipeline is sound, since two separately-built systems converged on the same answer without being designed to match.

---

## 🧠 Key Engineering Decisions

A few decisions worth highlighting (full detail in [`TECHNICAL_NOTES.md`](./TECHNICAL_NOTES.md)):

1. **Event deduplication moved to PySpark.** Power BI's Direct Lake storage mode doesn't support calculated columns, so the logic that turns repeated 15-min polls into distinct disruption *events* was implemented upstream, in the Gold layer notebook, using a PySpark window function.
2. **Fact Constellation over a single fact table**, to cleanly separate polling-grain data from event-grain data (see Data Model above).
3. **Severity-weighted ranking algorithm** for "which line is most affected" — a simple count of disruptions would treat one severe closure the same as one minor delay. Instead, statuses are weighted (Severe Delays = 3, Part Closure/Suspended = 2, Minor Delays = 1) and summed per line, so that frequent minor issues can outrank a single severe one — which is often operationally true.
4. **Deterministic tie-breaking across visuals.** Two visuals computing the same ranking from similar queries can disagree on tied values unless a secondary sort key is added — a subtle real-world data-consistency issue that was found and fixed during development.

---

## 🔍 Key Findings

- Victoria and District have consistently ranked among the most-affected lines across the monitored window
- Off-peak hours show the highest concentration of planned closures
- The batch and real-time pipelines independently produced matching Network Health figures, validating the overall data pipeline

---

## ⚠️ Known Limitations

Documented honestly rather than hidden — full list and reasoning in [`TECHNICAL_NOTES.md`](./TECHNICAL_NOTES.md):

- `disruption_reason` is free-text and couldn't be reliably categorised for cost analysis
- No real cost/passenger-impact data exists in the API; duration is used as a proxy
- 15-minute polling granularity means disruptions shorter than that may be under-recorded
- One week of data is not enough to detect chronic, long-term patterns
- The API reports line-level status only — no station-level granularity

---

## 🚀 Future Work

- Average delay per line measure (quick DAX addition)
- A dedicated Data Quality layer (deferred to the next portfolio project, to be designed in from the start rather than retrofitted)
- Orchestration with Apache Airflow (deferred — Fabric's own Data Pipeline already covers this need for this project; Airflow is planned for a future project where it adds more value)
- Proper secrets management via Azure Key Vault. Not used here because this project runs on a standalone Fabric Trial capacity (no linked Azure subscription) — and Fabric's identity can't be granted Key Vault access policies without that subscription linkage, which is a known platform constraint, not an oversight. Credentials are instead kept out of version control entirely (placeholders in committed code, real values only in the Fabric workspace). A production version of this project would provision a full Azure subscription specifically to enable Key Vault integration.

---

## 📁 Repository Structure

```
tfl-fabric-analytics/
├── README.md
├── TECHNICAL_NOTES.md
├── notebooks/
│   ├── 01_ingest_bronze.ipynb
│   ├── 02_transform_silver.ipynb
│   ├── 03_transform_gold.ipynb
│   └── 04_stream_to_eventhouse.ipynb
├── kql/
│   ├── active_disruptions_now.kql
│   ├── last_update.kql
│   ├── network_health_pct.kql
│   ├── live_line_status.kql
│   ├── disruption_timeline_heatmap.kql
│   ├── most_affected_lines.kql
│   └── active_disruptions_table.kql
├── dax/
│   └── measures.md
└── screenshots/
    ├── network_overview.png
    ├── disruption_duration.png
    └── realtime_dashboard.png
```

---

## 👤 Author

**Maria Nataqi** — Data Analyst transitioning into Data Engineering
DP-600 (Microsoft Fabric) · PL-300 · Databricks Analyst Specialisation
[GitHub](https://github.com/marianataqi) · [LinkedIn](https://linkedin.com/in/marianataqi)
