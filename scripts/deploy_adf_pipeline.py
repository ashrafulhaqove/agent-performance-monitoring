"""
deploy_adf_pipeline.py

Creates all ADF resources for pl_daily_team_aggregation:
  - Linked service: Azure SQL Database  (ls_azure_sql)
  - Linked service: ADLS Gen2           (ls_adls)
  - Dataset: parameterised source CSV   (ds_bronze_csv)
  - Dataset: sink SQL table             (ds_fact_daily_metrics)
  - Pipeline: pl_daily_team_aggregation
      ForEach(channel) → CopyActivity → fact_daily_metrics
      SqlServerStoredProcedureActivity → sp_aggregate_team_metrics

Run:
  set -a && source .env && set +a
  python3 -u scripts/deploy_adf_pipeline.py
"""

import os
import json

from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    LinkedServiceResource,
    AzureSqlDatabaseLinkedService,
    AzureBlobFSLinkedService,
    DatasetResource,
    DelimitedTextDataset,
    AzureBlobFSLocation,
    AzureSqlTableDataset,
    PipelineResource,
    ParameterSpecification,
    ForEachActivity,
    CopyActivity,
    SqlServerStoredProcedureActivity,
    SqlServerStoredProcedureActivityTypeProperties,
    DelimitedTextSource,
    AzureSqlSink,
    DatasetReference,
    LinkedServiceReference,
    ActivityDependency,
    DependencyCondition,
    SecureString,
)

# ── Config ───────────────────────────────────────────────────────────────────

SUBSCRIPTION_ID = os.environ["AZURE_SUBSCRIPTION_ID"]
RESOURCE_GROUP  = "agent-monitoring-rg"
FACTORY_NAME    = "agent-monitoring-adf"
SQL_HOST        = os.environ["SQL_HOST"]
SQL_DATABASE    = os.environ["SQL_DATABASE"]
SQL_ADMIN       = os.environ["SQL_ADMIN"]
SQL_PASSWORD    = os.environ["SQL_PASSWORD"]
STORAGE_CONN    = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

# Parse account name from connection string for ADLS Gen2 URL
def _parse(conn, key):
    for part in conn.split(";"):
        if part.startswith(key + "="):
            return part[len(key) + 1:]
    raise ValueError(f"Key {key!r} not found in connection string")

ACCOUNT_NAME = _parse(STORAGE_CONN, "AccountName")
ACCOUNT_KEY  = _parse(STORAGE_CONN, "AccountKey")
ADLS_URL     = f"https://{ACCOUNT_NAME}.dfs.core.windows.net"

CHANNELS = ["call", "chat", "email", "issue_resolution"]

# ── Client ───────────────────────────────────────────────────────────────────

credential = AzureCliCredential()
client = DataFactoryManagementClient(credential, SUBSCRIPTION_ID)

# ── 1. Linked services ───────────────────────────────────────────────────────

print("Creating linked service: ls_azure_sql ...")
sql_conn_str = (
    f"integrated security=False;encrypt=True;connection timeout=30;"
    f"data source={SQL_HOST};initial catalog={SQL_DATABASE};"
    f"user id={SQL_ADMIN};password={SQL_PASSWORD}"
)
client.linked_services.create_or_update(
    RESOURCE_GROUP, FACTORY_NAME, "ls_azure_sql",
    LinkedServiceResource(
        properties=AzureSqlDatabaseLinkedService(
            type="AzureSqlDatabase",
            connection_string=SecureString(value=sql_conn_str),
        )
    )
)
print("  ls_azure_sql done.")

print("Creating linked service: ls_adls ...")
client.linked_services.create_or_update(
    RESOURCE_GROUP, FACTORY_NAME, "ls_adls",
    LinkedServiceResource(
        properties=AzureBlobFSLinkedService(
            type="AzureBlobFS",
            url=ADLS_URL,
            account_key=SecureString(value=ACCOUNT_KEY),
        )
    )
)
print("  ls_adls done.")

# ── 2. Datasets ──────────────────────────────────────────────────────────────

print("Creating source dataset: ds_bronze_csv ...")
client.datasets.create_or_update(
    RESOURCE_GROUP, FACTORY_NAME, "ds_bronze_csv",
    DatasetResource(
        properties=DelimitedTextDataset(
            type="DelimitedText",
            linked_service_name=LinkedServiceReference(reference_name="ls_adls"),
            parameters={
                "channel":   ParameterSpecification(type="String"),
                "file_date": ParameterSpecification(type="String"),
            },
            location=AzureBlobFSLocation(
                type="AzureBlobFSLocation",
                file_system="bronze",
                folder_path={
                    "value": "@dataset().channel",
                    "type":  "Expression",
                },
                file_name={
                    "value": "@concat(dataset().file_date, '.csv')",
                    "type":  "Expression",
                },
            ),
            first_row_as_header=True,
            column_delimiter=",",
        )
    )
)
print("  ds_bronze_csv done.")

print("Creating sink dataset: ds_fact_daily_metrics ...")
client.datasets.create_or_update(
    RESOURCE_GROUP, FACTORY_NAME, "ds_fact_daily_metrics",
    DatasetResource(
        properties=AzureSqlTableDataset(
            type="AzureSqlTable",
            linked_service_name=LinkedServiceReference(reference_name="ls_azure_sql"),
            table_name="fact_daily_metrics",
        )
    )
)
print("  ds_fact_daily_metrics done.")

# ── 3. Pipeline ──────────────────────────────────────────────────────────────

print("Creating pipeline: pl_daily_team_aggregation ...")

copy_activity = CopyActivity(
    type="Copy",
    name="copy_channel_csv_to_sql",
    inputs=[
        DatasetReference(
            reference_name="ds_bronze_csv",
            parameters={
                "channel":   {
                    "value": "@item().channel",
                    "type":  "Expression",
                },
                "file_date": {
                    "value": "@pipeline().parameters.ExecutionDate",
                    "type":  "Expression",
                },
            },
        )
    ],
    outputs=[DatasetReference(reference_name="ds_fact_daily_metrics")],
    source=DelimitedTextSource(type="DelimitedTextSource"),
    sink=AzureSqlSink(
        type="AzureSqlSink",
        sql_writer_use_table_lock=False,
    ),
)

foreach_activity = ForEachActivity(
    type="ForEach",
    name="foreach_channel",
    items_property={
        "value": (
            '@createArray('
            '{"channel":"call"},'
            '{"channel":"chat"},'
            '{"channel":"email"},'
            '{"channel":"issue_resolution"}'
            ')'
        ),
        "type": "Expression",
    },
    is_sequential=True,
    activities=[copy_activity],
)

sp_activity = SqlServerStoredProcedureActivity(
    type="SqlServerStoredProcedure",
    name="exec_sp_aggregate_team_metrics",
    linked_service_name=LinkedServiceReference(reference_name="ls_azure_sql"),
    type_properties=SqlServerStoredProcedureActivityTypeProperties(
        stored_procedure_name="sp_aggregate_team_metrics",
        stored_procedure_parameters={
            "ExecutionDate": {
                "value": {
                    "value": "@pipeline().parameters.ExecutionDate",
                    "type":  "Expression",
                },
                "type": "String",
            }
        },
    ),
    depends_on=[
        ActivityDependency(
            activity="foreach_channel",
            dependency_conditions=[DependencyCondition.SUCCEEDED],
        )
    ],
)

pipeline = client.pipelines.create_or_update(
    RESOURCE_GROUP, FACTORY_NAME, "pl_daily_team_aggregation",
    PipelineResource(
        parameters={"ExecutionDate": ParameterSpecification(type="String")},
        activities=[foreach_activity, sp_activity],
    ),
)
print(f"  Pipeline created: {pipeline.name}")

# ── 4. Write summary JSON to adf/ ────────────────────────────────────────────

summary = {
    "factory":          FACTORY_NAME,
    "resource_group":   RESOURCE_GROUP,
    "linked_services":  ["ls_azure_sql", "ls_adls"],
    "datasets":         ["ds_bronze_csv", "ds_fact_daily_metrics"],
    "pipelines":        ["pl_daily_team_aggregation"],
    "pipeline": {
        "parameters":  {"ExecutionDate": "String"},
        "activities": [
            {
                "name":    "foreach_channel",
                "type":    "ForEach",
                "items":   CHANNELS,
                "sequential": True,
                "inner":   "copy_channel_csv_to_sql",
            },
            {
                "name":       "exec_sp_aggregate_team_metrics",
                "type":       "SqlServerStoredProcedure",
                "procedure":  "sp_aggregate_team_metrics",
                "depends_on": ["foreach_channel"],
            },
        ],
    },
}

out_path = "adf/pipeline_summary.json"
with open(out_path, "w") as f:
    json.dump(summary, f, indent=2)
print(f"\nSummary written → {out_path}")
print("\nDone. ADF pipeline deployed successfully.")
