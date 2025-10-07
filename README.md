# Digital Euro Association (DEA) MiCAR Tracker

This project tracks issuers of Electronic Money Tokens (EMTs) and Crypto-Asset Service Providers (CASPs) under the MiCAR framework. The dashboard is a simple static site that reads data from a public Google Sheets document and visualises it using HTML and Tailwind CSS.

## Data source

`update-data.js` pulls the live EMT register from Google Sheets, but CASP and non-compliant entity data now come from the checked-in outputs of the Python pipelines under `out/`. This keeps the public dashboard responsive even if the ESMA endpoints are momentarily unavailable.

During each run the script parses the CSVs, converts each row into a JavaScript object and then rewrites `index.html` with the refreshed data and updated source dates. CASP snapshot dates are derived from the Python diff results rather than a Google Sheet cell.

## ESMA pipelines (CASPs and non-compliant entities)

The repository ships with lightweight Python pipelines that pull the latest CASP and non-compliant registers directly from ESMA, normalise the datasets, and record row-level deltas between runs. CASP service codes are shortened to the website-friendly labels used previously (e.g. `a.` → `custody`).

### Requirements

Install the Python dependencies once:

```bash
pip install -r requirements.txt
```

### Running the pipeline

```bash
python -m pipeline.run_all
```

Both pipelines automatically scrape the ESMA MiCA landing page to discover the current CSV URLs. In environments without outbound internet access (such as CI), you can override the sources by setting `CASP_CSV_URL` and/or `NON_COMPLIANT_CSV_URL` to either direct HTTP URLs or local file paths.

Outputs are written to the `out/` directory:

- `casps.csv` / `casps.json` – tidy CASP records ready for the dashboard
- `casps_delta.csv` – `new`/`update`/`remove` actions based on a persistent SQLite snapshot under `data/state.sqlite`
- `non_compliant.csv` / `non_compliant.json` – normalised non-compliant entities
- `non_compliant_delta.csv` – change log for the non-compliant register
- `meta.json` – aggregated metadata keyed by dataset name (CASPs, non-compliant, etc.)

Before writing fresh outputs the pipelines create timestamped backups under `data/backups/<dataset>/`, and they refuse to overwrite existing files if the newly downloaded dataset is empty. This guards against transient network issues and makes it easy to revert to a previous snapshot if required.

All original ESMA columns are preserved alongside the normalised ones under a `raw_` prefix, so you can troubleshoot or extend the cleaning logic without losing fidelity.

## Configuration

The dashboard update script still reads EMT locations from the `CSV_URL` and `DATE_URL` environment variables. CASP and non-compliant inputs default to the pipeline outputs under `out/`, but you can override them with the `CASPS_PATH` and `NON_COMPLIANT_PATH` environment variables if you need to supply alternative files.
