# Digital Euro Association (DEA) MiCAR Tracker

This project tracks issuers of Electronic Money Tokens (EMTs) and Crypto-Asset Service Providers (CASPs) under the MiCAR framework. The dashboard is a simple static site that reads data from a public Google Sheets document and visualises it using HTML and Tailwind CSS.

## Data source

`update-data.js` downloads a CSV export of the ESMA registers from Google Sheets, ensuring the dashboard reflects the latest EMT and CASP information published by the regulator.

The script parses the CSV, converts each row into a JavaScript object and then rewrites `index.html` with the new data and a human‑friendly source date.

## CASP Python pipeline

The repository also ships with a lightweight Python pipeline that pulls the latest CASP register directly from ESMA, normalises the dataset, shortens the service codes (e.g. `a.` → `custody`), and records row-level deltas between runs.

### Requirements

Install the Python dependencies once:

```bash
pip install -r requirements.txt
```

### Running the pipeline

```bash
python3 pipeline/casps_pipeline.py
```

The script automatically scrapes the ESMA MiCA landing page to discover the current CASP CSV URL. In environments without outbound internet access (such as CI), you can override the source by setting `CASP_CSV_URL` to a direct HTTP URL or a local file path.

Outputs are written to the `out/` directory:

- `casps.csv` / `casps.json` – tidy records ready for the dashboard
- `casps_delta.csv` – `new`/`update`/`remove` actions based on a persistent SQLite snapshot under `data/state.sqlite`
- `meta.json` – timestamp and summary statistics for observability

The pipeline keeps the original ESMA columns alongside the normalised ones under a `raw_` prefix so you can troubleshoot or extend the cleaning logic without losing fidelity.

## Configuration

The script reads CSV locations from the `CSV_URL` and `DATE_URL` environment variables. If they are not set, default URLs pointing to the public Google Sheet are used.
