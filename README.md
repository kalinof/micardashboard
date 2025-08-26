# MiCAR EMT Dashboard

This project tracks issuers of Electronic Money Tokens (EMTs) under the MiCAR framework. The dashboard is a simple static site that reads data from a public Google Sheets document and visualises it using HTML and Tailwind CSS.

## Data source

`update-data.js` downloads a CSV export of the ESMA EMT register from Google Sheets.

The script parses the CSV, converts each row into a JavaScript object and then rewrites `index.html` with the new data and a human‑friendly source date.

## Configuration

The script reads CSV locations from the `CSV_URL` and `DATE_URL` environment variables. If they are not set, default URLs pointing to the public Google Sheet are used.
