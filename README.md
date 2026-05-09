# Clariva Airflow — Data Extraction Pipeline

```bash
git clone https://github.com/puxti-labs/airflow-demo-project
```

Upstream extraction DAGs for [puxti-demo-project](https://github.com/puxti-labs/puxti-demo-project).

These DAGs extract raw data from Clariva's operational systems and land it in the `raw` schema — the tables that dbt's staging models read from. Together with [puxti-demo-project](https://github.com/puxti-labs/puxti-demo-project), they demonstrate Puxti's cross-system FEEDS edge and the `puxti link` command.

---

## Data flow

```
Salesforce CRM          salesforce_sync
  Accounts         ─────────────────────►  raw.raw_accounts
  Opportunities    ─────────────────────►  raw.raw_opportunities
  Order Lines      ─────────────────────►  raw.raw_order_lines

Stripe Billing          billing_sync
  Subscriptions    ─────────────────────►  raw.raw_subscriptions

                                                    ▼
                                          dbt_clariva_run
                                            stg_accounts
                                            stg_opportunities
                                            stg_order_lines
                                            stg_subscriptions
                                                    ▼
                                            dim_accounts
                                            fct_revenue
                                            fct_pipeline
                                            fct_win_rate
```

---

## DAGs

### `salesforce_sync` — runs daily at 04:00 UTC

Extracts Salesforce Accounts, Opportunities, and OpportunityLineItems.

| Task | Output table | Key field |
|------|-------------|-----------|
| `extract_accounts` | `raw.raw_accounts` | `arr` — CRM-side annual recurring revenue |
| `extract_opportunities` | `raw.raw_opportunities` | `amount` — deal value (see semantic note below) |
| `extract_order_lines` | `raw.raw_order_lines` | `total_price` — per-line item price post-migration |

On success: triggers `dbt_clariva_run`.

### `billing_sync` — runs daily at 03:30 UTC

Extracts Stripe subscriptions.

| Task | Output table | Key field |
|------|-------------|-----------|
| `extract_subscriptions` | `raw.raw_subscriptions` | `mrr` — normalised monthly recurring revenue |

### `dbt_clariva_run` — runs daily at 05:00 UTC (fallback); normally triggered by `salesforce_sync`

Runs `dbt deps → source freshness → run staging → run marts → test`.

---

## The demo scenario — semantic change in `raw_opportunities.amount`

This project is the upstream half of a Puxti demo showing how a **semantic change in an Airflow task propagates downstream into dbt models**.

**The change:** In Q1 2024, Clariva migrated Salesforce to support line-item pricing. Each opportunity now has multiple order lines (`raw_order_lines`). The `amount` field in `raw_opportunities` changed from:

> "The total agreed deal price, entered manually by the rep"

to:

> "A Salesforce-computed roll-up: SUM of OpportunityLineItem.TotalPrice"

No column was renamed. No schema changed. But the *meaning* shifted — and every dbt model that uses `stg_opportunities.total_value` to measure revenue now relies on a Salesforce-side aggregation that can drift from the canonical source (`raw_order_lines`).

**How Puxti captures this:**

```bash
# Declare the cross-system link at onboarding time
puxti link \
  --from task.airflow.salesforce_sync.extract_opportunities \
  --to source.clariva.raw_opportunities \
  --description "extract_opportunities lands one row per Salesforce opportunity into raw.raw_opportunities. The amount field maps from Salesforce Amount — historically a manually-entered deal total, now a roll-up summary of order line prices post Q1 2024 migration."

# When the semantic meaning of amount changes, capture it
puxti capture \
  --entity source.clariva.raw_opportunities.amount \
  --before "Manually entered deal value. One number per opportunity representing the full agreed price." \
  --after "Salesforce roll-up: SUM(OpportunityLineItem.TotalPrice). Computed server-side. May diverge from raw_order_lines if lines are edited after close." \
  --description "Salesforce migration to line-item pricing changed how Amount is populated. It is now a derived field, not a source of truth. Downstream models should prefer SUM(raw_order_lines.total_price) grouped by opportunity_id." \
  --repo puxti-labs/puxti-demo-project
```

Puxti traverses the semantic graph from `raw_opportunities.amount` → `stg_opportunities.total_value` → `fct_revenue`, `fct_pipeline`, `fct_win_rate` and opens a PR with annotated diffs for each affected model.

---

## Setup

This repo is the Airflow half of the demo. For the full cross-system scenario you need both repos side by side.

**1. Clone both repos**

```bash
mkdir clariva-workspace && cd clariva-workspace
git clone https://github.com/puxti-labs/puxti-demo-project
git clone https://github.com/puxti-labs/airflow-demo-project
```

**2. Install Puxti**

```bash
pip install puxti==0.6.0
```

**3. Start Neo4j** (from puxti-demo-project, which includes `docker-compose.yml`)

```bash
cd puxti-demo-project && docker compose up -d && cd ..
```

**4. Set up Airflow**

```bash
cd airflow-demo-project
cp .env.example .env
# fill in credentials — see .env.example for all variables

pip install -r requirements.txt

# Set Airflow connections
airflow connections add salesforce_default \
  --conn-type http \
  --conn-host https://clariva.my.salesforce.com \
  --conn-extra '{"login": "user@clariva.io", "password": "...", "security_token": "..."}'

airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"project": "clariva-data-prod", "keyfile_path": "/path/to/sa.json"}'
```

**5. Set environment variables for Puxti**

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export GITHUB_TOKEN=ghp_...    # needed if you want Puxti to open real PRs
```

**6. Place `.puxti.yml` at the workspace root** (see Workspace config below)

**7. Verify Puxti is configured**

```bash
puxti config    # shows resolved env vars and .puxti.yml location
puxti health    # checks Neo4j, Anthropic key, dbt manifest, GitHub token
```

---

## Workspace config (`.puxti.yml`)

Place this file at the root of the `clariva-workspace/` directory — one level above both repo clones:

```
clariva-workspace/
├── .puxti.yml
├── puxti-demo-project/    ← git clone https://github.com/puxti-labs/puxti-demo-project
└── airflow-demo-project/  ← git clone https://github.com/puxti-labs/airflow-demo-project
```

```yaml
version: 1

connectors:
  dbt:
    project_dir: ./puxti-demo-project
    repo: puxti-labs/puxti-demo-project
    base_branch: main

  airflow:
    project_dir: ./airflow-demo-project
    repo: puxti-labs/airflow-demo-project
    dags_dir: dags/
    base_branch: main
```

With this in place, `--repo` and `--dbt-project-dir` resolve automatically from any
subdirectory inside the workspace.

---

## Puxti cross-system links

Declared once per source, typically at onboarding time. Run these from anywhere inside
the workspace after placing `.puxti.yml`:

```bash
puxti link --from task.airflow.salesforce_sync.extract_accounts      --to source.clariva.raw_accounts      --description "Extracts Salesforce Account records. arr maps from Salesforce AnnualRevenue — CRM-entered, may lag billing system ARR."
puxti link --from task.airflow.salesforce_sync.extract_opportunities  --to source.clariva.raw_opportunities  --description "Extracts Salesforce Opportunities. amount is a roll-up of order line prices post Q1 2024 migration."
puxti link --from task.airflow.salesforce_sync.extract_order_lines    --to source.clariva.raw_order_lines    --description "Extracts OpportunityLineItems. One row per product per deal. This table was empty before Q1 2024."
puxti link --from task.airflow.billing_sync.extract_subscriptions     --to source.clariva.raw_subscriptions  --description "Extracts Stripe subscriptions. mrr is normalised to monthly from Stripe plan amount and interval."
```

---

## Project structure

```
airflow-demo-project/
├── dags/
│   ├── salesforce_sync.py    # Salesforce → raw.raw_accounts/opportunities/order_lines
│   ├── billing_sync.py       # Stripe → raw.raw_subscriptions
│   └── dbt_clariva_run.py    # dbt run triggered after raw tables are loaded
├── plugins/                  # Custom operators (empty for demo)
├── requirements.txt
├── .env.example
└── README.md
```

---

## Further reading

- [puxti-demo-project](https://github.com/puxti-labs/puxti-demo-project) — dbt counterpart for this demo
- [Puxti CLI — install and full command reference](https://github.com/puxti-labs/puxti)
