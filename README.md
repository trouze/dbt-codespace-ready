# dbt Fusion Codespace Starter

A batteries-included GitHub Codespace for dbt development — no local install required.

Opens a VS Code environment with the [dbt Fusion engine](https://docs.getdbt.com/docs/core/pip-install#install-dbt-fusion) (Rust-based) and the official [dbt VS Code extension](https://marketplace.visualstudio.com/items?itemName=dbtLabsInc.dbt) pre-installed.

## Quick start

**Start a new dbt project** — creates a repo in your org from this template, then open a Codespace from your new repo via Code → Codespaces:

[![Use this template](https://img.shields.io/badge/Use_this_template-238636?style=for-the-badge&logo=github&logoColor=white)](https://github.com/new?template_name=dbt-codespace-ready&template_owner=trouze)

**Try it out** — opens a Codespace directly from this template without creating a repo:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?template_repository=trouze/dbt-codespace-ready)

*Codespaces work best in Chrome or Firefox. Safari users may hit Service Worker errors — [enable third-party cookies](https://github.com/orgs/community/discussions/26316) if so.*

---

## Setup

### 1. Configure Codespaces secrets

Before creating your Codespace, add these repository secrets so credentials are injected automatically at startup:

| Secret | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Account identifier, e.g. `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_DATABASE` | Target database |
| `SNOWFLAKE_WAREHOUSE` | Virtual warehouse name |
| `SNOWFLAKE_SCHEMA` | Dev schema (defaults to `dev`) |
| `SNOWFLAKE_ROLE` | Role (optional) |

Set them at: **Repository → Settings → Secrets and variables → Codespaces**

Docs: [Specifying recommended secrets for a repository](https://docs.github.com/en/codespaces/setting-up-your-project-for-codespaces/configuring-dev-containers/specifying-recommended-secrets-for-a-repository)

### 2. Initialize your project

The `profiles.yml` at the root already references the secrets above. Update `dbt_project.yml` with your project name and adjust the model configurations to match your layering convention.

### 3. Run dbt

```bash
dbt debug       # verify connection
dbt deps        # install packages
dbt run         # execute models
dbt test        # run tests
```

---

## What's included

| Component | Detail |
|---|---|
| **Engine** | dbt Fusion (Rust binary — fast, no Python dependency) |
| **Extension** | dbt Labs VS Code extension with DAG explorer, model preview, autocomplete |
| **profiles.yml** | Snowflake template wired to Codespaces secrets via `env_var()` |
| **dbt_project.yml** | Minimal skeleton with `staging` (views) and `marts` (tables) layers |

---

## Connecting a different warehouse

Swap out the `outputs.dev` block in `profiles.yml` for any adapter dbt Fusion supports. See [dbt connection profiles docs](https://docs.getdbt.com/docs/core/connect-data-platform/profiles).
