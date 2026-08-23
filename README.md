# E-Commerce Contact Centre Performance Monitoring System

End-to-end Azure data engineering pipeline for a simulated e-commerce contact centre. Ingests agent performance data via batch and streaming paths, stores and aggregates it in Azure SQL, and visualises team KPIs in a 7-page Power BI dashboard.

---

## Architecture

Lambda Architecture — two ingestion paths writing to a shared Azure SQL warehouse.

**Batch path (daily, ADF-orchestrated)**
```
Python (generate_data.py)
    → CSVs → ADLS Gen2 (bronze/{channel}/YYYY-MM-DD.csv)
    → ADF Copy Activity
    → Azure SQL: fact_daily_metrics
    → ADF: sp_aggregate_team_metrics
    → Azure SQL: agg_team_daily
    → Power BI
```

**Speed path (event-driven, near real-time)**
```
Python (kafka_producer.py)
    → Upstash Kafka (topic: agent-metrics)
    → Azure Function (kafka_consumer)
    → Azure SQL: fact_daily_metrics
```

Both paths write to the same fact table. The ADF stored procedure aggregates both into `agg_team_daily`, which is what Power BI reads.

---

## Business Context

A contact centre for an e-commerce platform with two service lines:

| Service Line | Who They Serve | Traffic |
|---|---|---|
| Customer Service | Buyers / end customers | High volume |
| Seller Service | Merchants / sellers | Low volume |

Each service line operates across 4 channels × 2 directions (inbound + outbound):

| Channel | KPIs |
|---|---|
| Call | AHT, items handled, resolution rate, CSAT, first-call resolution |
| Chat | AHT, items handled, resolution rate, CSAT, concurrent chats avg |
| Email | Items handled, avg response hours, SLA compliance, resolution rate, CSAT |
| Issue Resolution | Items handled, avg days to close, reopen rate, SLA compliance, resolution rate, CSAT |

**2 service lines × 4 channels × 2 directions = 16 teams, 500 agents, 90 days = 45,000 fact rows**

---

## Tech Stack

| Layer | Service | Tier |
|---|---|---|
| Data Lake | Azure Data Lake Storage Gen2 (`agentmonitoringdatalake`) | Free (5 GB/month) |
| Warehouse | Azure SQL Database (`agent_db`, server: `agent-monitoring-server`) | Free F0 (32 GB) |
| Orchestration | Azure Data Factory (`agent-monitoring-adf`) | Free (1,000 DIU-hrs/month) |
| Streaming | Upstash Kafka (topic: `agent-metrics`) | Free serverless |
| Functions | Azure Functions (`kafka_consumer`) | Free (1M executions/month) |
| BI | Power BI Desktop | Free |

---

## Schema (Star Schema in Azure SQL)

| Table | Rows | Description |
|---|---|---|
| `dim_service_lines` | 2 | Customer Service, Seller Service |
| `dim_channels` | 8 | 4 channels × 2 directions |
| `dim_teams` | 16 | One per channel/direction/service-line combination |
| `dim_agents` | 500 | Agent roster with team assignments |
| `dim_date` | 90 | Date dimension for the simulation window |
| `fact_daily_metrics` | 45,000 | Raw daily KPIs per agent; channel-specific columns NULL where not applicable |
| `agg_team_daily` | ~1,440 | Team-level aggregates refreshed daily via MERGE stored procedure |
| `pipeline_log` | 1 per run | ADF pipeline run status, row counts, and error messages |

---

## Repository Structure

```
├── sql/
│   ├── 01_schema.sql           # Dim + fact + agg table definitions
│   ├── 02_stored_procedure.sql # sp_aggregate_team_metrics (MERGE pattern, idempotent)
│   └── 03_pipeline_log.sql     # pipeline_log table + insert logic
├── scripts/
│   ├── generate_data.py        # Simulates 90 days of agent data, uploads to ADLS Gen2
│   └── kafka_producer.py       # Streams agent events to Upstash Kafka topic
├── azure_functions/
│   └── kafka_consumer/         # Azure Function: reads Kafka topic → writes to SQL
├── adf/
│   └── pipeline_export.json    # ARM template export of pl_daily_team_aggregation
├── powerbi/
│   └── agent_monitoring.pbix   # 7-page Power BI dashboard
├── screenshots/                # Dashboard screenshots
└── README.md
```

---

## ADF Pipeline: `pl_daily_team_aggregation`

- **Trigger:** Daily at 02:00 UTC
- **Parameter:** `ExecutionDate` (String)
- **Activities:**
  1. Lookup — row count check for the execution date
  2. If Condition — skips downstream if no rows found
  3. Copy Activity — loads Blob CSVs into `fact_daily_metrics`
  4. Execute Stored Procedure — runs `sp_aggregate_team_metrics`

The pipeline logs every run (success and failure) to `pipeline_log` via TRY/CATCH inside the stored procedure.

---

## Power BI Dashboard (7 Pages)

| Page | Contents |
|---|---|
| 1. Executive Overview | KPI cards + CSAT trend line |
| 2. Channel Performance | Channel KPIs vs targets |
| 3. Team Scorecard | 16 teams with RAG (Red/Amber/Green) formatting |
| 4. Customer vs Seller | Service line comparison |
| 5. Agent Drill-Down | 90-day breakdown for any individual agent |
| 6. Trends | Volume + quality metrics over 90 days |
| 7. Pipeline Health | `pipeline_log` status view |

---

## Real-World Ingestion Patterns

In a production environment, the Python-generated CSVs would be replaced by:

| Pattern | Source → ADLS Gen2 |
|---|---|
| Native connector | Genesys Cloud exports directly to Blob (zero code) |
| Webhook + Function | Zendesk event → Azure Function → Blob (near real-time) |
| ADF HTTP Activity | ADF pulls from any REST API → Blob (most common in DE roles) |

This project simulates the same landing-zone pattern — the ADF pipeline and SQL schema are identical to what production would use.

---

## License

[Prosperity Public License 3.0](LICENSE) — free for non-commercial use.
