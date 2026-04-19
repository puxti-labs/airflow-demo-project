"""
dbt_clariva_run
===============
Run the Clariva dbt project after raw tables are loaded.

Triggered by salesforce_sync on success. Also runs on a fixed schedule
as a safety net in case the trigger is missed.

Pipeline:
  dbt deps → dbt source freshness → dbt run (staging) → dbt run (marts) → dbt test

Failure behaviour:
  - Source freshness failure: warns but does not block the run. Raw tables
    can be slightly stale during backfills without invalidating the models.
  - dbt run failure: stops immediately. No point running tests on broken models.
  - dbt test failure: alerts #data-quality but does not block downstream consumers.
    Tests are advisory — breaking a test should not take down the BI layer.

Owner:     data-engineering@clariva.io
Oncall:    #data-alerts (Slack)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["data-alerts@clariva.io"],
}

DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/dbt/clariva")
DBT_PROFILES_DIR = Variable.get("dbt_profiles_dir", default_var="/opt/dbt/profiles")
DBT_TARGET = Variable.get("dbt_target", default_var="prod")

DBT_CMD = (
    f"dbt --no-use-colors "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--target {DBT_TARGET}"
)


@dag(
    dag_id="dbt_clariva_run",
    description="Run the Clariva dbt project (staging + marts). Triggered by salesforce_sync.",
    schedule="0 5 * * *",  # 05:00 UTC daily fallback; normally triggered by salesforce_sync
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "transform", "clariva"],
    doc_md=__doc__,
)
def dbt_clariva_run():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps",
        doc="Install dbt packages declared in packages.yml.",
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness || true",
        doc=(
            "Check that raw source tables were loaded recently. "
            "Uses `|| true` so a freshness warning does not abort the run — "
            "alerts are handled by dbt's own freshness notifications."
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging",
        doc=(
            "Materialise all staging models (views). "
            "Staging models rename and lightly cast raw fields — "
            "no business logic lives here."
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts",
        doc=(
            "Materialise all mart models (tables). "
            "Marts join staging models and apply business logic. "
            "Models in scope: fct_revenue, fct_pipeline, fct_win_rate, dim_accounts."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test",
        doc=(
            "Run all dbt tests. Failures are alerted to #data-quality but do not "
            "block downstream consumers. A test failure should trigger a manual review, "
            "not an automatic rollback."
        ),
    )

    dbt_deps >> dbt_source_freshness >> dbt_run_staging >> dbt_run_marts >> dbt_test


dbt_clariva_run()
