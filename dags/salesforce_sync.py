"""
salesforce_sync
===============
Extract Salesforce CRM data and land it in the BigQuery raw schema.

This DAG is the upstream origin of three tables that feed Clariva's dbt project:

    raw.raw_accounts      →  stg_accounts  →  dim_accounts
    raw.raw_opportunities →  stg_opportunities  →  fct_revenue, fct_pipeline, fct_win_rate
    raw.raw_order_lines   →  stg_order_lines  →  (order-line revenue model, planned)

Puxti cross-system links (declared via `puxti link`):
    task.airflow.salesforce_sync.extract_accounts      → source.clariva.raw_accounts
    task.airflow.salesforce_sync.extract_opportunities → source.clariva.raw_opportunities
    task.airflow.salesforce_sync.extract_order_lines   → source.clariva.raw_order_lines

Semantic note on raw_opportunities.amount
------------------------------------------
The `amount` field in raw_opportunities maps directly from Salesforce's standard
Amount field (API name: Amount). Historically this held the full deal value agreed
at close — one row per opportunity, one number for the whole deal.

In Q1 2024, Clariva's sales ops team migrated Salesforce to support line-item
pricing. Reps now price deals product by product; Salesforce computes Amount as
the sum of OpportunityLineItem.TotalPrice. Simultaneously, every closed deal now
generates one or more rows in OpportunityLineItems — extracted here as raw_order_lines.

As of that migration:
  - raw_opportunities.amount = Salesforce-computed sum of order line totals
  - raw_order_lines.total_price (summed per opportunity_id) = the canonical source
  - The two values agree at extract time but can diverge if order lines are edited
    after the opportunity is closed

Any dbt model that uses stg_opportunities.total_value to sum revenue is implicitly
relying on this Salesforce-side aggregation. If that aggregation logic changes, the
impact flows through this task into every downstream model.

Owner:     data-engineering@clariva.io
Oncall:    #data-alerts (Slack)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.http.hooks.http import HttpHook

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-alerts@clariva.io"],
}

# BigQuery destination config
BQ_PROJECT = Variable.get("gcp_project", default_var="clariva-data-prod")
BQ_RAW_DATASET = "raw"
SALESFORCE_CONN_ID = "salesforce_default"
BQ_CONN_ID = "google_cloud_default"

# Full-replace watermark: extract all records modified in the last N days.
# Set to a large window to ensure seed data is always fresh during demos.
INCREMENTAL_DAYS = int(Variable.get("salesforce_incremental_days", default_var="3"))


def _sf_query(soql: str) -> list[dict[str, Any]]:
    """Execute a SOQL query via the Salesforce REST API and return all records.

    Uses Airflow connection `salesforce_default` (type: HTTP, host: your instance URL,
    extras: {"login": "...", "password": "...", "security_token": "..."}).
    """
    hook = HttpHook(method="GET", http_conn_id=SALESFORCE_CONN_ID)
    # In production this would use simple-salesforce or the Salesforce Airflow provider.
    # Simplified here for demo clarity.
    response = hook.run(
        endpoint=f"/services/data/v58.0/query?q={soql}",
        headers={"Content-Type": "application/json"},
    )
    data = json.loads(response.text)
    records = data.get("records", [])
    # Paginate through nextRecordsUrl if present
    while next_url := data.get("nextRecordsUrl"):
        response = hook.run(endpoint=next_url)
        data = json.loads(response.text)
        records.extend(data.get("records", []))
    return records


def _bq_replace(table: str, rows: list[dict[str, Any]]) -> int:
    """Replace all rows in `raw.<table>` with the provided data.

    Uses WRITE_TRUNCATE so the raw table always reflects the latest full extract.
    Returns the number of rows written.
    """
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    client = hook.get_client()
    table_ref = f"{BQ_PROJECT}.{BQ_RAW_DATASET}.{table}"
    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=client.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    return len(rows)


@dag(
    dag_id="salesforce_sync",
    description="Extract Salesforce Accounts, Opportunities, and OpportunityLineItems to BigQuery raw schema.",
    schedule="0 4 * * *",  # 04:00 UTC daily — before dbt_clariva_run at 05:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["salesforce", "extract", "raw"],
    doc_md=__doc__,
)
def salesforce_sync():

    # ── 1. Connection health check ─────────────────────────────────────────────

    @task(task_id="check_salesforce_connection")
    def check_salesforce_connection() -> None:
        """Verify that the Salesforce API is reachable before extracting.

        Hits /services/data/ (version list endpoint) — read-only, no quota impact.
        Fails fast so the rest of the DAG doesn't start if credentials have expired.
        """
        hook = HttpHook(method="GET", http_conn_id=SALESFORCE_CONN_ID)
        response = hook.run(endpoint="/services/data/")
        assert response.status_code == 200, f"Salesforce API unreachable: {response.status_code}"
        logger.info("Salesforce API reachable.")

    # ── 2. Accounts ───────────────────────────────────────────────────────────

    @task(task_id="extract_accounts")
    def extract_accounts() -> list[dict[str, Any]]:
        """Extract all Salesforce Account records.

        Maps to raw.raw_accounts, which feeds stg_accounts → dim_accounts.

        Fields extracted:
          Id          → account_id   (Salesforce 18-char ID, normalised to lowercase)
          Name        → name         (company name as entered by the rep)
          Industry    → industry     (Salesforce Industry picklist value)
          AnnualRevenue → arr        (annual recurring revenue stored on the account record;
                                      this is the CRM-side ARR and may lag billing system ARR
                                      — see dim_accounts.crm_arr vs billing_arr)
          CustomerTier__c → tier     (custom field: 'smb' | 'mid-market' | 'enterprise')
          CreatedDate   → created_at

        Full replace — no incremental logic. Account record count is small enough
        that a full extract is cheaper than tracking deletes.
        """
        soql = (
            "SELECT Id, Name, Industry, AnnualRevenue, CustomerTier__c, CreatedDate "
            "FROM Account "
            "WHERE IsDeleted = false "
            "ORDER BY CreatedDate ASC"
        )
        records = _sf_query(soql)
        return [
            {
                "account_id": r["Id"].lower(),
                "name": r["Name"],
                "industry": r.get("Industry", ""),
                "arr": r.get("AnnualRevenue") or 0,
                "tier": (r.get("CustomerTier__c") or "smb").lower(),
                "created_at": r["CreatedDate"],
            }
            for r in records
        ]

    @task(task_id="load_accounts")
    def load_accounts(rows: list[dict[str, Any]]) -> int:
        """Load extracted account rows into raw.raw_accounts (full replace).

        Destination: {BQ_PROJECT}.raw.raw_accounts
        Write mode: WRITE_TRUNCATE
        """
        n = _bq_replace("raw_accounts", rows)
        logger.info("Loaded %d rows into raw.raw_accounts.", n)
        return n

    # ── 3. Opportunities ──────────────────────────────────────────────────────

    @task(task_id="extract_opportunities")
    def extract_opportunities() -> list[dict[str, Any]]:
        """Extract Salesforce Opportunity records.

        Maps to raw.raw_opportunities, which feeds:
          stg_opportunities → fct_revenue, fct_pipeline, fct_win_rate

        Fields extracted:
          Id            → opportunity_id
          AccountId     → account_id
          Name          → name           (free-text deal name, typically "Account — Product")
          StageName     → stage          (pipeline stage: Closed Won | Closed Lost |
                                          Proposal | Negotiation | Prospecting)
          Amount        → amount         (see semantic note below)
          CloseDate     → close_date     (actual or expected close date)
          CreatedDate   → created_at

        Semantic note on `amount`
        -------------------------
        Amount maps from Salesforce's standard Amount field (API name: Amount).

        Before Q1 2024: reps entered a single deal value. One row per opportunity.
        Amount = the full agreed deal price.

        After Q1 2024 migration to line-item pricing: Amount is now computed by
        Salesforce as SUM(OpportunityLineItem.TotalPrice) via a roll-up summary field.
        Reps price each product line separately; the total flows up to Amount.

        This means:
          - raw_opportunities.amount and SUM(raw_order_lines.total_price per opp) should
            agree at extract time, but Amount is a snapshot while order lines can be edited.
          - Downstream dbt models that use stg_opportunities.total_value to measure deal
            value are implicitly trusting this Salesforce-side roll-up. If Salesforce
            changes how Amount is computed, or if deals are repriced after close, the
            raw_opportunities extract will reflect that change immediately — but the
            semantic meaning of the field will have shifted without any schema change
            being visible in BigQuery.
          - The canonical source of deal value post-migration is raw_order_lines.

        Full replace on a 24-hour window. Opportunities updated before the window
        are assumed stable; if backfill is needed, set INCREMENTAL_DAYS to a larger value.
        """
        cutoff = (datetime.utcnow() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
        soql = (
            "SELECT Id, AccountId, Name, StageName, Amount, CloseDate, CreatedDate "
            "FROM Opportunity "
            "WHERE IsDeleted = false "
            f"AND LastModifiedDate >= {cutoff} "
            "ORDER BY CreatedDate ASC"
        )
        records = _sf_query(soql)
        return [
            {
                "opportunity_id": r["Id"].lower(),
                "account_id": r["AccountId"].lower(),
                "name": r["Name"],
                "stage": r["StageName"],
                "amount": r.get("Amount") or 0,
                "close_date": r["CloseDate"],
                "created_at": r["CreatedDate"],
            }
            for r in records
        ]

    @task(task_id="load_opportunities")
    def load_opportunities(rows: list[dict[str, Any]]) -> int:
        """Load extracted opportunity rows into raw.raw_opportunities (full replace).

        Destination: {BQ_PROJECT}.raw.raw_opportunities
        Write mode: WRITE_TRUNCATE

        Note: dbt's stg_opportunities renames `amount` to `total_value`. Any change
        to how Salesforce populates Amount flows through here into that renamed field
        and then into fct_revenue, fct_pipeline, and fct_win_rate without any schema
        change being detectable by column-level lineage tools.
        """
        n = _bq_replace("raw_opportunities", rows)
        logger.info("Loaded %d rows into raw.raw_opportunities.", n)
        return n

    # ── 4. Order lines ────────────────────────────────────────────────────────

    @task(task_id="extract_order_lines")
    def extract_order_lines() -> list[dict[str, Any]]:
        """Extract Salesforce OpportunityLineItem records.

        Maps to raw.raw_order_lines, which feeds stg_order_lines.

        This table was empty before Q1 2024. The migration to line-item pricing
        populated it retroactively for all closed deals and going forward for
        every new opportunity.

        Fields extracted:
          Id                    → order_line_id
          OpportunityId         → opportunity_id   (FK to raw_opportunities)
          PricebookEntry.Name   → product_name
          Quantity              → quantity
          UnitPrice             → unit_price
          TotalPrice            → total_price      (= quantity × unit_price)
          CreatedDate           → created_at

        Multiple rows per opportunity_id are normal and expected post-migration.
        Any model that JOINs raw_opportunities to raw_order_lines on opportunity_id
        must GROUP BY or use a subquery to avoid fan-out.
        """
        cutoff = (datetime.utcnow() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
        soql = (
            "SELECT Id, OpportunityId, PricebookEntry.Name, Quantity, UnitPrice, TotalPrice, CreatedDate "
            "FROM OpportunityLineItem "
            "WHERE IsDeleted = false "
            f"AND LastModifiedDate >= {cutoff} "
            "ORDER BY CreatedDate ASC"
        )
        records = _sf_query(soql)
        return [
            {
                "order_line_id": r["Id"].lower(),
                "opportunity_id": r["OpportunityId"].lower(),
                "product_name": r.get("PricebookEntry", {}).get("Name", ""),
                "quantity": r.get("Quantity") or 1,
                "unit_price": r.get("UnitPrice") or 0,
                "total_price": r.get("TotalPrice") or 0,
                "created_at": r["CreatedDate"],
            }
            for r in records
        ]

    @task(task_id="load_order_lines")
    def load_order_lines(rows: list[dict[str, Any]]) -> int:
        """Load extracted order line rows into raw.raw_order_lines (full replace).

        Destination: {BQ_PROJECT}.raw.raw_order_lines
        Write mode: WRITE_TRUNCATE
        """
        n = _bq_replace("raw_order_lines", rows)
        logger.info("Loaded %d rows into raw.raw_order_lines.", n)
        return n

    # ── 5. Validation ─────────────────────────────────────────────────────────

    @task(task_id="validate_raw_tables")
    def validate_raw_tables(
        accounts_loaded: int,
        opportunities_loaded: int,
        order_lines_loaded: int,
    ) -> None:
        """Assert minimum row counts on all three raw tables.

        Catches silent failures where the Salesforce API returned 0 records
        due to a permission change, SOQL filter bug, or API pagination error.
        Fails the DAG before dbt runs on empty or truncated tables.
        """
        checks = [
            ("raw_accounts", accounts_loaded, 1),
            ("raw_opportunities", opportunities_loaded, 1),
            ("raw_order_lines", order_lines_loaded, 0),  # 0 is valid pre-migration
        ]
        failed = []
        for table, count, minimum in checks:
            if count < minimum:
                failed.append(f"{table}: got {count} rows, expected >= {minimum}")
        if failed:
            raise ValueError("Raw table validation failed:\n" + "\n".join(failed))
        logger.info(
            "Validation passed — accounts: %d, opportunities: %d, order_lines: %d",
            accounts_loaded,
            opportunities_loaded,
            order_lines_loaded,
        )

    # ── 6. Trigger dbt ────────────────────────────────────────────────────────

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_clariva_run",
        trigger_dag_id="dbt_clariva_run",
        wait_for_completion=False,
        doc="Kick off the dbt run once all raw tables are loaded and validated.",
    )

    # ── Wire up dependencies ───────────────────────────────────────────────────

    sf_ready = check_salesforce_connection()

    acc_rows = extract_accounts()
    opp_rows = extract_opportunities()
    ol_rows = extract_order_lines()

    # All three extracts depend on the connection check
    sf_ready >> [acc_rows, opp_rows, ol_rows]

    acc_loaded = load_accounts(acc_rows)
    opp_loaded = load_opportunities(opp_rows)
    ol_loaded = load_order_lines(ol_rows)

    validated = validate_raw_tables(acc_loaded, opp_loaded, ol_loaded)
    validated >> trigger_dbt


salesforce_sync()
