const fs = require('fs');
const { csvUrl, dateUrl, nonCompliantUrl, caspsUrl } = require('./config');

function csvToArray(str, delimiter = ',') {
    const lines = str.split('\n');
    const headers = parseCSVLine(lines[0]);
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        if (lines[i].trim()) {
            const values = parseCSVLine(lines[i]);
            const obj = {};
            headers.forEach((header, index) => {
                obj[header.trim()] = values[index] ? values[index].trim() : '';
            });
            result.push(obj);
        }
    }

    return result.filter(row => row['#'] && row['#'] !== '' && row['#'] !== 'nan' && !isNaN(parseInt(row['#'])));
}

function csvToArrayGeneric(str) {
    const lines = str.split('\n');
    const headers = parseCSVLine(lines[0]);
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        if (lines[i].trim()) {
            const values = parseCSVLine(lines[i]);
            const obj = {};
            headers.forEach((header, index) => {
                obj[header.trim()] = values[index] ? values[index].trim() : '';
            });
            result.push(obj);
        }
    }

    return result;
}

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

function parseNumber(value) {
    if (!value || value === 'nan' || value === '') return 0;
    const num = parseInt(value);
    return isNaN(num) ? 0 : num;
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
    let day, month, year;
    if (parts[0].length === 4) {
        [year, month, day] = parts; // Format: YYYY-MM-DD
    } else {
        [day, month, year] = parts; // Format: DD-MM-YYYY or DD/MM/YYYY
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
        console.log(`Row ${index + 1}:`, {
            id: row['#'],
            issuer: row['Issuer (HQ)'],
            tokens: row['Tokens'],
            euro: row['Euro'],
            usd: row['USD']
        });

        if (row['#'] && row['Issuer (HQ)'] && row['Issuer (HQ)'] !== 'nan') {
            const item = {
                id: parseInt(row['#']) || index + 1,
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
            console.log('✅ Added:', item.issuer, 'with', item.count, 'tokens');
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
        .filter(row => (row['ae_lei_name'] || '').trim())
        .map((row, index) => ({
            id: index + 1,
            name: row['ae_lei_name'] ? row['ae_lei_name'].trim() : '',
            authority: row['ae_competentAuthority'] ? row['ae_competentAuthority'].trim() : '',
            memberState: row['ae_homeMemberState'] ? row['ae_homeMemberState'].trim() : '',
            services: parseMultiValueField(row['ac_serviceCode']),
            websites: parseMultiValueField(row['ae_website'])
        }));
}

const memberStateMap = {
    'AT': 'Austria',
    'BE': 'Belgium',
    'BG': 'Bulgaria',
    'HR': 'Croatia',
    'CY': 'Cyprus',
    'CZ': 'Czech Republic',
    'DK': 'Denmark',
    'EE': 'Estonia',
    'FI': 'Finland',
    'FR': 'France',
    'DE': 'Germany',
    'GR': 'Greece',
    'HU': 'Hungary',
    'IE': 'Ireland',
    'IT': 'Italy',
    'LV': 'Latvia',
    'LT': 'Lithuania',
    'LU': 'Luxembourg',
    'MT': 'Malta',
    'NL': 'Netherlands',
    'PL': 'Poland',
    'PT': 'Portugal',
    'RO': 'Romania',
    'SK': 'Slovakia',
    'SI': 'Slovenia',
    'ES': 'Spain',
    'SE': 'Sweden',
    'IS': 'Iceland',
    'LI': 'Liechtenstein',
    'NO': 'Norway',
    'UK': 'United Kingdom'
};

function mapMemberState(code) {
    if (!code) return '';
    const trimmed = code.trim();
    return memberStateMap[trimmed.toUpperCase()] || trimmed;
}

function convertToNonCompliantData(csvData) {
    const entries = [];
    const entryIndexByKey = new Map();

    csvData.forEach((row, index) => {
        const entity = row['Commercial Name'] || '';
        const authority = row['Competent Authority'] || '';
        const memberState = mapMemberState(row['Member State'] || '');
        const websites = (row['ae_website'] || '')
            .split('|')
            .map(site => site.trim())
            .filter(site => site.length > 0);
        const isNew = (row['Column 1'] || '').toLowerCase() === 'new';

        if (!entity) {
            return;
        }

        const dedupeKey = `${entity}::${memberState}::${websites.join('|')}`;
        if (entryIndexByKey.has(dedupeKey)) {
            const existingIndex = entryIndexByKey.get(dedupeKey);
            const existingEntry = entries[existingIndex];
            if (isNew && !existingEntry.isNew) {
                existingEntry.isNew = true;
                console.log('🔄 Updated existing non-compliant entry to NEW status:', entity);
            }
            return;
        }

        entryIndexByKey.set(dedupeKey, entries.length);

        entries.push({
            id: entries.length + 1,
            entity,
            country: memberState,
            authority,
            websites,
            isNew
        });

        console.log('🚨 Non-compliant entity added:', {
            entity,
            country: memberState,
            authority,
            websites,
            isNew
        });
    });

    return entries;
}

function updateHtmlFile(newData, emtLastUpdated, nonCompliantEntries, caspsEntries, caspsLastUpdated) {
    const htmlFile = 'index.html';

    if (!fs.existsSync(htmlFile)) {
        console.error('❌ HTML file not found:', htmlFile);
        return;
    }

    let htmlContent = fs.readFileSync(htmlFile, 'utf8');

    // Find the data array in the JavaScript section - try multiple patterns
    let dataStart = htmlContent.indexOf('const data = [');
    let dataEnd = htmlContent.indexOf('];', dataStart) + 2;
    let dataPattern = 'const data = ';

    if (dataStart === -1) {
        dataStart = htmlContent.indexOf('data = [');
        dataEnd = htmlContent.indexOf('];', dataStart) + 2;
        dataPattern = 'data = ';
    }

    if (dataStart === -1) {
        dataStart = htmlContent.indexOf('let data = [');
        dataEnd = htmlContent.indexOf('];', dataStart) + 2;
        dataPattern = 'let data = ';
    }

    if (dataStart === -1 || dataEnd === -1) {
        console.error('❌ Could not find data array in HTML file');
        console.log('🔍 Searching for data patterns...');

        // Show what patterns exist
        const patterns = ['const data', 'let data', 'var data', 'data ='];
        patterns.forEach(pattern => {
            const index = htmlContent.indexOf(pattern);
            if (index !== -1) {
                console.log(`Found "${pattern}" at position ${index}`);
                console.log('Context:', htmlContent.substring(index, index + 100));
            }
        });
        return;
    }

    // Replace the data array
    const newDataString = `${dataPattern}${JSON.stringify(newData, null, 4)};`;
    const updatedHtml = htmlContent.substring(0, dataStart) + newDataString + htmlContent.substring(dataEnd);

    const nonCompliantStart = updatedHtml.indexOf('const nonCompliantData = [');
    let finalHtml = updatedHtml;

    if (nonCompliantStart !== -1) {
        const nonCompliantEnd = updatedHtml.indexOf('];', nonCompliantStart) + 2;
        const nonCompliantString = `const nonCompliantData = ${JSON.stringify(nonCompliantEntries || [], null, 4)};`;
        finalHtml = updatedHtml.substring(0, nonCompliantStart) + nonCompliantString + updatedHtml.substring(nonCompliantEnd);
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

    // Log summary statistics
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
}

// Main execution
async function main() {
    console.log('🔄 Fetching data from Google Sheets...');
    console.log('🌐 Data URL:', csvUrl);
    console.log('🌐 Date URL:', dateUrl);
    console.log('🌐 Non-compliant URL:', nonCompliantUrl);
    console.log('🌐 CASPs URL:', caspsUrl);

    try {
        const [data, dateCsv, nonCompliantCsv, caspsCsv] = await Promise.all([
            fetchCsv(csvUrl),
            fetchCsv(dateUrl),
            fetchCsv(nonCompliantUrl),
            fetchCsv(caspsUrl)
        ]);
        console.log('📋 Raw CSV length:', data.length, 'characters');
        console.log('📋 First 200 characters:', data.substring(0, 200));

        // Final check if we still got HTML
        ensureCsvResponseValid(data, 'issuer feed');
        ensureCsvResponseValid(nonCompliantCsv, 'non-compliant feed');
        ensureCsvResponseValid(caspsCsv, 'CASPs feed');

        const csvArray = csvToArray(data);
        console.log('📊 Parsed', csvArray.length, 'valid rows');

        const jsData = convertToJsData(csvArray);
        console.log('📈 Converted to', jsData.length, 'JavaScript objects');

        if (jsData.length === 0) {
            console.error('❌ No valid data found! Check CSV format.');
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

        const dateMap = extractDatesFromCsv(dateCsv);
        const emtSheetDate = dateMap['snapshot_date'] || dateMap['emt_snapshot_date'] || '';
        const caspsSheetDate = dateMap['casps_snapshot_date'] || '';
        console.log('📅 EMT sheet date:', emtSheetDate);
        console.log('📅 CASPs sheet date:', caspsSheetDate);

        updateHtmlFile(jsData, emtSheetDate, nonCompliantEntries, caspsEntries, caspsSheetDate);
    } catch (error) {
        console.error('❌ Error fetching CSV:', error);
        process.exit(1);
    }
}

function ensureCsvResponseValid(csvText, label) {
    if (!csvText || /<html|<HTML|Temporary Redirect/i.test(csvText)) {
        console.error(`❌ ${label} returned unexpected HTML or empty content.`);
        console.error('📋 Raw response preview:', (csvText || '').substring(0, 500));
        process.exit(1);
    }
}

main();
