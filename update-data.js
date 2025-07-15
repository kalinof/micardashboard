const https = require('https');
const fs = require('fs');

// Your working CSV URL
const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRWudeV0zFLqB54658hCDUgSRFfy-ADeR2JMilO-oel74hjBr1CdIB2FWufxyR2yuQJGNaBPHNYG7vh/pub?gid=0&single=true&output=csv';
// Metadata CSV containing snapshot information
const metadataUrl = 'https://docs.google.com/spreadsheets/d/e/.../pub?gid=353293525&single=true&output=csv';

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

function parseKeyValueCSV(csvText) {
    const lines = csvText.trim().split(/\n/);
    const result = {};
    lines.forEach(line => {
        const [key, value] = parseCSVLine(line);
        if (key) {
            result[key.trim()] = value ? value.trim() : '';
        }
    });
    return result;
}

function parseNumber(value) {
    if (!value || value === 'nan' || value === '') return 0;
    const num = parseInt(value);
    return isNaN(num) ? 0 : num;
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

function updateHtmlFile(newData, snapshotDate) {
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
    let updatedHtml = htmlContent.substring(0, dataStart) + newDataString + htmlContent.substring(dataEnd);

    const dateObj = new Date(snapshotDate);
    const longDate = dateObj.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
    const shortDate = dateObj.toLocaleDateString('en-GB');

    updatedHtml = updatedHtml.replace(
        /Source: ESMA EMT Register, [^<]+/,
        `Source: ESMA EMT Register, ${longDate}`
    );
    updatedHtml = updatedHtml.replace(
        /Data as of \d{1,2}\/\d{1,2}\/\d{4}/,
        `Data as of ${shortDate}`
    );

    if (updatedHtml.includes('const snapshotDate')) {
        updatedHtml = updatedHtml.replace(/const snapshotDate = '[^']*';/, `const snapshotDate = '${snapshotDate}';`);
    } else {
        const insertPoint = updatedHtml.indexOf('// Data - will be updated by the automation script');
        if (insertPoint !== -1) {
            const endOfLine = updatedHtml.indexOf('\n', insertPoint);
            updatedHtml = updatedHtml.slice(0, endOfLine + 1) + `    const snapshotDate = '${snapshotDate}';\n` + updatedHtml.slice(endOfLine + 1);
        }
    }

    fs.writeFileSync(htmlFile, updatedHtml);
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
}

function extractRedirectUrl(htmlContent) {
    // Extract the redirect URL from the HTML response
    const match = htmlContent.match(/HREF="([^"]+)"/);
    if (match) {
        return match[1].replace(/&amp;/g, '&');
    }
    return null;
}

function fetchWithRedirect(url, maxRedirects = 3) {
    return new Promise((resolve, reject) => {
        console.log('🌐 Fetching:', url);

        const urlObj = new URL(url);
        const options = {
            hostname: urlObj.hostname,
            path: urlObj.pathname + urlObj.search,
            method: 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/csv,text/plain,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }
        };

        const req = https.request(options, (res) => {
            console.log('📡 Response status:', res.statusCode);

            let data = '';

            res.on('data', (chunk) => {
                data += chunk;
            });

            res.on('end', () => {
                // Check if it's a redirect HTML page
                if (data.includes('Temporary Redirect') && data.includes('HREF=')) {
                    if (maxRedirects > 0) {
                        const redirectUrl = extractRedirectUrl(data);
                        if (redirectUrl) {
                            console.log('🔄 Following HTML redirect to:', redirectUrl);
                            fetchWithRedirect(redirectUrl, maxRedirects - 1)
                                .then(resolve)
                                .catch(reject);
                            return;
                        }
                    }
                    reject(new Error('Too many redirects or unable to extract redirect URL'));
                    return;
                }

                resolve(data);
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.end();
    });
}

// Main execution
console.log('🔄 Fetching data from Google Sheets...');
console.log('🌐 Data URL:', csvUrl);
console.log('🌐 Metadata URL:', metadataUrl);

Promise.all([fetchWithRedirect(csvUrl), fetchWithRedirect(metadataUrl)])
    .then(([data, meta]) => {
        try {
            console.log('📋 Raw CSV length:', data.length, 'characters');
            console.log('📋 First 200 characters:', data.substring(0, 200));

            // Final check if we still got HTML
            if (data.includes('<HTML>') || data.includes('<html>') || data.includes('Temporary Redirect')) {
                console.error('❌ Still receiving HTML after redirect handling!');
                console.error('📋 Raw response:', data.substring(0, 500));
                process.exit(1);
            }

            const csvArray = csvToArray(data);
            console.log('📊 Parsed', csvArray.length, 'valid rows');

            const jsData = convertToJsData(csvArray);
            console.log('📈 Converted to', jsData.length, 'JavaScript objects');

            if (jsData.length === 0) {
                console.error('❌ No valid data found! Check CSV format.');
                process.exit(1);
            }

            const metadata = parseKeyValueCSV(meta);
            const snapshotDate = metadata['snapshot_date'];

            updateHtmlFile(jsData, snapshotDate);

        } catch (error) {
            console.error('❌ Error processing data:', error);
            console.error('Stack trace:', error.stack);
            process.exit(1);
        }
    })
    .catch(error => {
        console.error('❌ Error fetching CSV:', error);
        process.exit(1);
    });
