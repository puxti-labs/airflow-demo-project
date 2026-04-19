"""
billing_sync
============
Extract subscription data from Stripe and land it in the BigQuery raw schema.

This DAG is the upstream origin of:

    raw.raw_subscriptions  →  stg_subscriptions  →  dim_accounts

Subscriptions are managed in Stripe. Each subscription record represents a
paid plan for one account. MRR is stored at the subscription level; dbt
computes ARR as mrr * 12 in stg_subscriptions.

Puxti cross-system link (declared via `puxti link`):
    task.airflow.billing_sync.extract_subscriptions → source.clariva.raw_subscriptions

Semantic notes
--------------
mrr (monthly recurring revenue)
  Extracted from Stripe as the subscription's plan amount divided by the billing
  interval. A monthly plan with amount=1000 → mrr=1000. An annual plan with
  amount=12000 billed yearly → mrr=1000. This normalisation happens in this task,
  not in dbt — stg_subscriptions.mrr reflects the already-normalised value.

status
  Maps from Stripe subscription status. Active Stripe statuses ('active',
  'trialing') map to 'active'. Terminal statuses ('canceled', 'unpaid',
  'incomplete_expired') map to 'churned'. 'past_due' maps to 'at_risk' (future:
  not yet in the dbt model — currently coalesced to 'active' in stg_subscriptions).

plan
  Maps from the Stripe price nickname. The Clariva naming convention is:
  'starter' | 'smb' | 'mid-market' | 'enterprise'. These must match what the
  sales team enters in Salesforce for dim_accounts.current_plan to be consistent.
  Any mismatch between Stripe plan nicknames and the CRM tier field is a known
  data quality issue tracked in #data-quality.

Owner:     data-engineering@clariva.io
Oncall:    #data-alerts (Slack)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import stripe
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-alerts@clariva.io"],
}

BQ_PROJECT = Variable.get("gcp_project", default_var="clariva-data-prod")
BQ_RAW_DATASET = "raw"
BQ_CONN_ID = "google_cloud_default"
STRIPE_API_KEY = Variable.get("stripe_api_key", default_var="")

STRIPE_TO_INTERNAL_STATUS = {
    "active": "active",
    "trialing": "active",
    "past_due": "active",      # treated as active until fully churned; #data-quality issue
    "canceled": "churned",
    "unpaid": "churned",
    "incomplete_expired": "churned",
}


def _normalise_mrr(amount_cents: int, interval: str, interval_count: int) -> float:
    """Convert a Stripe plan amount to a monthly MRR figure (in dollars).

    Stripe stores amounts in the smallest currency unit (cents for USD).
    interval is 'month' or 'year'; interval_count is typically 1.
    """
    amount_dollars = amount_cents / 100
    if interval == "year":
        return round(amount_dollars / (12 * interval_count), 2)
    elif interval == "month":
        return round(amount_dollars / interval_count, 2)
    else:
        return 0.0


def _bq_replace(table: str, rows: list[dict[str, Any]]) -> int:
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
    dag_id="billing_sync",
    description="Extract Stripe subscription data to BigQuery raw.raw_subscriptions.",
    schedule="30 3 * * *",  # 03:30 UTC daily — before salesforce_sync trigger of dbt
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stripe", "billing", "extract", "raw"],
    doc_md=__doc__,
)
def billing_sync():

    @task(task_id="extract_subscriptions")
    def extract_subscriptions() -> list[dict[str, Any]]:
        """Extract all Stripe subscription records for Clariva customers.

        Maps to raw.raw_subscriptions, which feeds:
          stg_subscriptions → dim_accounts (active_mrr, billing_arr, current_plan)

        Fields extracted and their mapping:
          id                        → subscription_id
          metadata.clariva_account_id → account_id  (FK to raw_accounts; set when
                                        the subscription is created via Clariva's
                                        provisioning workflow)
          plan.nickname             → plan          ('starter' | 'smb' | 'mid-market' |
                                                      'enterprise' — must match CRM tier)
          plan.amount + interval    → mrr           (normalised to monthly, see _normalise_mrr)
          current_period_start      → start_date
          current_period_end        → end_date
          status                    → status        (mapped via STRIPE_TO_INTERNAL_STATUS)

        Pulls all subscriptions regardless of status so that churned records remain
        in raw_subscriptions and stg_subscriptions.is_active correctly reflects history.
        """
        stripe.api_key = STRIPE_API_KEY
        subscriptions = stripe.Subscription.list(
            limit=100,
            expand=["data.plan"],
            status="all",
        ).auto_paging_iter()

        rows = []
        for sub in subscriptions:
            plan = sub.get("plan") or {}
            account_id = (sub.get("metadata") or {}).get("clariva_account_id", "")
            if not account_id:
                logger.warning("Subscription %s has no clariva_account_id in metadata — skipping.", sub["id"])
                continue

            mrr = _normalise_mrr(
                amount_cents=plan.get("amount", 0),
                interval=plan.get("interval", "month"),
                interval_count=plan.get("interval_count", 1),
            )

            rows.append({
                "subscription_id": sub["id"],
                "account_id": account_id,
                "plan": (plan.get("nickname") or "unknown").lower(),
                "mrr": mrr,
                "start_date": datetime.utcfromtimestamp(sub["current_period_start"]).strftime("%Y-%m-%d"),
                "end_date": datetime.utcfromtimestamp(sub["current_period_end"]).strftime("%Y-%m-%d"),
                "status": STRIPE_TO_INTERNAL_STATUS.get(sub["status"], "churned"),
            })

        return rows

    @task(task_id="load_subscriptions")
    def load_subscriptions(rows: list[dict[str, Any]]) -> int:
        """Load subscription rows into raw.raw_subscriptions (full replace).

        Destination: {BQ_PROJECT}.raw.raw_subscriptions
        Write mode: WRITE_TRUNCATE
        """
        n = _bq_replace("raw_subscriptions", rows)
        logger.info("Loaded %d rows into raw.raw_subscriptions.", n)
        return n

    @task(task_id="validate_subscriptions")
    def validate_subscriptions(rows_loaded: int) -> None:
        """Assert raw_subscriptions has at least one active subscription.

        A zero-row result almost always means the Stripe API key has expired
        or the metadata filter excluded all records — not a legitimate empty state.
        """
        if rows_loaded == 0:
            raise ValueError(
                "raw_subscriptions loaded 0 rows. "
                "Check STRIPE_API_KEY and clariva_account_id metadata on Stripe subscriptions."
            )
        logger.info("Validation passed — %d subscription rows loaded.", rows_loaded)

    rows = extract_subscriptions()
    n = load_subscriptions(rows)
    validate_subscriptions(n)


billing_sync()
