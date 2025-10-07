const fs = require('fs');
const path = require('path');

const { csvUrl, dateUrl, nonCompliantPath, caspsPath } = require('./config');

const BACKUP_ROOT = path.join(__dirname, 'data', 'backups');
const STATE_FILE = path.join(__dirname, 'data', 'dashboard_state.json');
const META_FILE = path.join(__dirname, 'out', 'meta.json');

function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
        const char = line[i];

        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += char;
        }
    }

    result.push(current);
    return result;
}

function csvToArray(str) {
    const lines = str.split('\n');
    const headers = parseCSVLine(lines[0]);
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        if (!lines[i].trim()) {
            continue;
        }
        const values = parseCSVLine(lines[i]);
        const obj = {};
        headers.forEach((header, index) => {
            obj[header.trim()] = values[index] ? values[index].trim() : '';
        });
        result.push(obj);
    }

    return result.filter(
        row => row['#'] && row['#'] !== '' && row['#'] !== 'nan' && !Number.isNaN(parseInt(row['#'], 10))
    );
}

function csvToArrayGeneric(str) {
    const lines = str.split('\n');
    if (!lines.length || !lines[0].trim()) {
        return [];
    }
    const headers = parseCSVLine(lines[0]);
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        if (!lines[i].trim()) {
            continue;
        }
        const values = parseCSVLine(lines[i]);
        const obj = {};
        headers.forEach((header, index) => {
            obj[header.trim()] = values[index] ? values[index].trim() : '';
        });
        result.push(obj);
    }

    return result;
}

function parseNumber(value) {
    if (!value || value === 'nan' || value === '') {
        return 0;
    }
    const num = parseInt(value, 10);
    return Number.isNaN(num) ? 0 : num;
}

async function fetchCsv(url) {
    console.log('🌐 Fetching:', url);
    const res = await fetch(url, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/csv,text/plain,*/*',
            'Accept-Language': 'en-US,en;q=0.9'
        },
        redirect: 'follow'
    });
    if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }
    return res.text();
}

function readDatasetCsv(dataset, primaryPath, fallbackName = path.basename(primaryPath)) {
    const resolvedPrimary = path.isAbsolute(primaryPath)
        ? primaryPath
        : path.join(__dirname, primaryPath);

    if (fs.existsSync(resolvedPrimary)) {
        const text = fs.readFileSync(resolvedPrimary, 'utf8');
        if (text.trim()) {
            return text;
        }
        console.warn(`⚠️ ${dataset} primary file was empty, searching backups:`, resolvedPrimary);
    } else {
        console.warn(`⚠️ ${dataset} primary file missing, searching backups:`, resolvedPrimary);
    }

    const candidates = getBackupDirectories(dataset);
    for (const candidate of candidates) {
        const fallbackPath = path.join(candidate, fallbackName);
        if (!fs.existsSync(fallbackPath)) {
            continue;
        }
        const text = fs.readFileSync(fallbackPath, 'utf8');
        if (text.trim()) {
            console.warn(`♻️ Using backup for ${dataset}:`, fallbackPath);
            return text;
        }
    }

    throw new Error(`No usable data found for ${dataset} (checked ${resolvedPrimary} and backups)`);
}

function getBackupDirectories(dataset) {
    const datasetDir = path.join(BACKUP_ROOT, dataset);
    if (!fs.existsSync(datasetDir)) {
        return [];
    }
    return fs
        .readdirSync(datasetDir)
        .filter(entry => fs.statSync(path.join(datasetDir, entry)).isDirectory())
        .sort((a, b) => b.localeCompare(a))
        .map(entry => path.join(datasetDir, entry));
}

function readMeta() {
    if (!fs.existsSync(META_FILE)) {
        return {};
    }
    try {
        return JSON.parse(fs.readFileSync(META_FILE, 'utf8'));
    } catch (error) {
        console.warn('⚠️ Could not parse meta file, ignoring.', error.message);
        return {};
    }
}

function isoDateOnly(value) {
    if (!value) {
        return '';
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return '';
    }
    return date.toISOString().split('T')[0];
}

function loadState() {
    if (!fs.existsSync(STATE_FILE)) {
        return {};
    }
    try {
        return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (error) {
        console.warn('⚠️ Could not parse dashboard state, starting fresh.', error.message);
        return {};
    }
}

function saveState(state) {
    fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function extractDatesFromCsv(csv) {
    const lines = csv.trim().split('\n');
    const dateMap = {};

    lines.forEach((line, index) => {
        if (!line.trim()) {
            return;
        }

        const values = parseCSVLine(line);
        const key = values[0] ? values[0].trim() : `row_${index}`;
        const value = values[1] ? values[1].trim() : '';

        if (key) {
            dateMap[key] = value;
        }
    });

    return dateMap;
}

function formatDate(dateStr) {
    if (!dateStr) {
        const today = new Date();
        const longDate = today.toLocaleDateString('en-GB', {
            day: 'numeric',
            month: 'long',
            year: 'numeric'
        });
        return {
            longDate,
            shortDate: longDate
        };
    }

    const parts = dateStr.split(/[\/\-]/);
    let day;
    let month;
    let year;
    if (parts[0].length === 4) {
        [year, month, day] = parts;
    } else {
        [day, month, year] = parts;
    }

    const dateObj = new Date(`${year}-${month}-${day}`);
    const longDate = dateObj.toLocaleDateString('en-GB', {
        day: 'numeric',
        month: 'long',
        year: 'numeric'
    });
    return {
        longDate,
        shortDate: longDate
    };
}

function convertToJsData(csvData) {
    const data = [];

    console.log('📋 CSV Headers:', Object.keys(csvData[0] || {}));
    console.log('📊 Processing', csvData.length, 'rows');

    csvData.forEach((row, index) => {
        if (row['#'] && row['Issuer (HQ)'] && row['Issuer (HQ)'] !== 'nan') {
            const item = {
                id: parseInt(row['#'], 10) || index + 1,
                issuer: row['Issuer (HQ)'] || '',
                state: row['Home State'] || '',
                authority: row['Competent Authority'] || '',
                tokens: row['Authorised EMT(s)'] || '',
                count: parseNumber(row['Tokens']),
                euro: parseNumber(row['Euro']),
                usd: parseNumber(row['USD']),
                czk: parseNumber(row['CZK']),
                gbp: parseNumber(row['GBP'])
            };
            data.push(item);
        }
    });

    return data;
}

function parseMultiValueField(value) {
    if (!value) {
        return [];
    }

    return value
        .split(/\||,|;|\s{2,}/)
        .map(entry => entry.trim())
        .filter(entry => entry.length > 0);
}

function convertToCaspsData(csvData) {
    return csvData
        .filter(row => (row['lei_name'] || '').trim())
        .map((row, index) => ({
            id: index + 1,
            name: row['lei_name'] ? row['lei_name'].trim() : '',
            authority: row['competent_authority'] ? row['competent_authority'].trim() : '',
            memberState: row['home_member_state'] ? row['home_member_state'].trim() : '',
            services: parseMultiValueField(row['service_codes']),
            websites: parseMultiValueField(row['website'])
        }));
}

function convertToNonCompliantData(csvData) {
    return csvData
        .filter(row => (row['lei_name'] || '').trim())
        .map((row, index) => ({
            id: index + 1,
            entity: row['lei_name'] ? row['lei_name'].trim() : '',
            country: row['home_member_state'] ? row['home_member_state'].trim() : '',
            authority: row['competent_authority'] ? row['competent_authority'].trim() : '',
            websites: parseMultiValueField(row['website']),
            isNew: String(row['is_new'] || '').toLowerCase() === 'true'
        }));
}

function updateHtmlFile(newData, emtLastUpdated, nonCompliantEntries, caspsEntries, caspsLastUpdated) {
    const htmlFile = 'index.html';

    if (!fs.existsSync(htmlFile)) {
        console.error('❌ HTML file not found:', htmlFile);
        return;
    }

    let htmlContent = fs.readFileSync(htmlFile, 'utf8');

    const patterns = ['const data = ', 'data = ', 'let data = '];
    let dataPattern = null;
    let dataStart = -1;
    let dataEnd = -1;
    for (const pattern of patterns) {
        const index = htmlContent.indexOf(`${pattern}[`);
        if (index !== -1) {
            dataPattern = pattern;
            dataStart = index;
            dataEnd = htmlContent.indexOf('];', dataStart) + 2;
            break;
        }
    }

    if (dataStart === -1 || dataEnd === -1) {
        console.error('❌ Could not find data array in HTML file');
        return;
    }

    const newDataString = `${dataPattern}${JSON.stringify(newData, null, 4)};`;
    const updatedHtml = htmlContent.substring(0, dataStart) + newDataString + htmlContent.substring(dataEnd);

    let finalHtml = updatedHtml;

    const nonCompliantStart = finalHtml.indexOf('const nonCompliantData = [');
    if (nonCompliantStart !== -1) {
        const nonCompliantEnd = finalHtml.indexOf('];', nonCompliantStart) + 2;
        const nonCompliantString = `const nonCompliantData = ${JSON.stringify(nonCompliantEntries || [], null, 4)};`;
        finalHtml = finalHtml.substring(0, nonCompliantStart) + nonCompliantString + finalHtml.substring(nonCompliantEnd);
    } else {
        console.error('❌ Could not find nonCompliantData array in HTML file');
    }

    const caspsStart = finalHtml.indexOf('const caspsData = [');
    if (caspsStart !== -1) {
        const caspsEnd = finalHtml.indexOf('];', caspsStart) + 2;
        const caspsString = `const caspsData = ${JSON.stringify(caspsEntries || [], null, 4)};`;
        finalHtml = finalHtml.substring(0, caspsStart) + caspsString + finalHtml.substring(caspsEnd);
    } else {
        console.error('❌ Could not find caspsData array in HTML file');
    }

    const { longDate: emtLongDate } = formatDate(emtLastUpdated);
    const { longDate: caspsLongDate } = formatDate(caspsLastUpdated);
    const updatedHtmlWithDate = finalHtml
        .replace(/Source: ESMA EMT Register\s*[–-]\s*Data as of [^<]+/, `Source: ESMA EMT Register – Data as of ${emtLongDate}`)
        .replace(/Source: ESMA CASPs Register\s*[–-]\s*Data as of [^<]+/, `Source: ESMA CASPs Register – Data as of ${caspsLongDate}`);

    fs.writeFileSync(htmlFile, updatedHtmlWithDate);
    console.log('✅ Dashboard updated successfully!');
    console.log(`📊 Updated with ${newData.length} issuers`);

    const totalTokens = newData.reduce((sum, item) => sum + item.count, 0);
    const euroTokens = newData.reduce((sum, item) => sum + item.euro, 0);
    const usdTokens = newData.reduce((sum, item) => sum + item.usd, 0);
    const gbpTokens = newData.reduce((sum, item) => sum + item.gbp, 0);
    const czkTokens = newData.reduce((sum, item) => sum + item.czk, 0);

    console.log('📈 Summary Statistics:');
    console.log(`   Total Issuers: ${newData.length}`);
    console.log(`   Total Tokens: ${totalTokens}`);
    console.log(`   EUR Tokens: ${euroTokens}`);
    console.log(`   USD Tokens: ${usdTokens}`);
    console.log(`   GBP Tokens: ${gbpTokens}`);
    console.log(`   CZK Tokens: ${czkTokens}`);
    if (Array.isArray(nonCompliantEntries)) {
        console.log(`   Non-compliant entities: ${nonCompliantEntries.length}`);
    }
    if (Array.isArray(caspsEntries)) {
        console.log(`   CASPs entities: ${caspsEntries.length}`);
    }
}

function ensureCsvResponseValid(csvText, label) {
    if (!csvText || /<html|<HTML|Temporary Redirect/i.test(csvText)) {
        console.error(`❌ ${label} returned unexpected HTML or empty content.`);
        console.error('📋 Raw response preview:', (csvText || '').substring(0, 500));
        process.exit(1);
    }
}

async function main() {
    console.log('🔄 Fetching data sources...');
    console.log('🌐 EMT data URL:', csvUrl);
    console.log('🌐 EMT date URL:', dateUrl);
    console.log('📂 Non-compliant dataset path:', nonCompliantPath);
    console.log('📂 CASPs dataset path:', caspsPath);

    try {
        const [emtCsv, dateCsv] = await Promise.all([
            fetchCsv(csvUrl),
            fetchCsv(dateUrl)
        ]);

        ensureCsvResponseValid(emtCsv, 'issuer feed');
        ensureCsvResponseValid(dateCsv, 'date feed');

        const nonCompliantCsv = readDatasetCsv('non_compliant', nonCompliantPath);
        const caspsCsv = readDatasetCsv('casps', caspsPath);
        const caspsDeltaCsv = readDatasetCsv('casps', path.join(__dirname, 'out', 'casps_delta.csv'), 'casps_delta.csv');

        const emtData = csvToArray(emtCsv);
        const jsData = convertToJsData(emtData);
        if (jsData.length === 0) {
            console.error('❌ No valid EMT data found! Check CSV format.');
            process.exit(1);
        }

        const nonCompliantArray = csvToArrayGeneric(nonCompliantCsv);
        console.log('🚨 Parsed', nonCompliantArray.length, 'non-compliant rows');
        const nonCompliantEntries = convertToNonCompliantData(nonCompliantArray);
        console.log('🚨 Converted to', nonCompliantEntries.length, 'non-compliant entities');

        const caspsArray = csvToArrayGeneric(caspsCsv);
        console.log('🏛️ Parsed', caspsArray.length, 'CASPs rows');
        const caspsEntries = convertToCaspsData(caspsArray);
        console.log('🏛️ Converted to', caspsEntries.length, 'CASPs entries');

        const caspsDeltaArray = csvToArrayGeneric(caspsDeltaCsv);
        const caspsHasChange = caspsDeltaArray.some(row => (row['action'] || '').trim());
        console.log('📅 CASPs diff actions detected:', caspsHasChange ? caspsDeltaArray.length : 0);

        const meta = readMeta();
        const caspsMeta = meta.casps || {};

        const state = loadState();
        const metaGeneratedDate = isoDateOnly(caspsMeta.generated_at);
        const metaLatestRecordDate = isoDateOnly(caspsMeta.latest_record_date);
        const preferredMetaDate = metaLatestRecordDate || metaGeneratedDate || '';

        if (!state.caspsLastUpdated && preferredMetaDate) {
            state.caspsLastUpdated = preferredMetaDate;
        }
        if (caspsHasChange) {
            state.caspsLastUpdated = preferredMetaDate || isoDateOnly(new Date().toISOString());
        }
        saveState(state);

        const dateMap = extractDatesFromCsv(dateCsv);
        const emtSheetDate = dateMap['snapshot_date'] || dateMap['emt_snapshot_date'] || '';
        const caspsSnapshotDate = state.caspsLastUpdated || preferredMetaDate || '';

        console.log('📅 EMT sheet date:', emtSheetDate);
        console.log('📅 CASPs snapshot date:', caspsSnapshotDate || '(not available)');

        updateHtmlFile(jsData, emtSheetDate, nonCompliantEntries, caspsEntries, caspsSnapshotDate);
    } catch (error) {
        console.error('❌ Error updating dashboard:', error);
        process.exit(1);
    }
}

main();

