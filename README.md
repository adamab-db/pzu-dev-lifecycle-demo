# PZU Dev Lifecycle Demo

Databricks Asset Bundle demonstrating the full development lifecycle for insurance claims data:

- **Medallion architecture** (Bronze → Silver → Gold) via SDP pipeline
- **Data quality expectations** built into the pipeline
- **Multi-environment deployment** (dev / staging / prod) from one codebase
- **CI/CD** via GitHub Actions
- **Unit and quality tests** as notebook-based jobs

## Quick Start

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run the ETL job
databricks bundle run claims_etl_job -t dev
```

## Environments

| Target | Schema | Prefix | Trigger |
|--------|--------|--------|---------|
| dev | dev_claims | `[dev user]` | Manual (`bundle deploy`) |
| staging | staging_claims | `stg_` | PR to main |
| prod | prod_claims | _(none)_ | Push to main |

## CI/CD

- **PR to main** → deploys to staging, runs pipeline
- **Push to main** → deploys to prod, runs pipeline

Requires `SP_CLIENT_ID` and `SP_CLIENT_SECRET` GitHub secrets for a Databricks service principal with account-level OAuth.
