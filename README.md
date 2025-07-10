# MiCAR EMT Dashboard

This project tracks issuers of Electronic Money Tokens (EMTs) under the MiCAR framework. The dashboard is a simple static site that reads data from a public Google Sheets document and visualises it using HTML and Tailwind CSS.

## Data source

`update-data.js` downloads a CSV export of the ESMA EMT register from Google Sheets.

The script parses the CSV, converts each row into a JavaScript object and then rewrites `index.html` with the new data and an updated source date.

## Building CSS

The dashboard uses Tailwind CSS compiled locally. After cloning the repo, install dependencies and build the stylesheet:

```bash
npm install
npm run build
```

This generates `assets/style.css` referenced by `index.html`.
