const https = require('https');
const fs = require('fs');

// Your correct CSV URL
const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRWudeV0zFLqB54658hCDUgSRFfy-ADeR2JMilO-oel74hjBr1CdIB2FWufxyR2yuQJGNaBPHNYG7vh/pub?output=csv';

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

function updateHtmlFile(newData) {
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

    // Update the date
    const today = new Date().toLocaleDateString('en-GB', { 
        day: 'numeric', 
        month: 'long', 
        year: 'numeric' 
    });
    const updatedHtmlWithDate = updatedHtml.replace(
        /Source: ESMA EMT Register, \d{1,2} \w+ \d{4}/,
        `Source: ESMA EMT Register, ${today}`
    );

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
}

// Main execution
console.log('🔄 Fetching data from Google Sheets...');
console.log('🌐 URL:', csvUrl);

https.get(csvUrl, (res) => {
    let data = '';

    res.on('data', (chunk) => {
        data += chunk;
    });

    res.on('end', () => {
        try {
            console.log('📋 Raw CSV length:', data.length, 'characters');
            console.log('📋 First 200 characters:', data.substring(0, 200));

            // Check if we got HTML instead of CSV
            if (data.includes('<HTML>') || data.includes('<html>')) {
                console.error('❌ Received HTML instead of CSV data!');
                console.error('🔧 Please check your Google Sheets publish settings');
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

            updateHtmlFile(jsData);

        } catch (error) {
            console.error('❌ Error processing data:', error);
            console.error('Stack trace:', error.stack);
            process.exit(1);
        }
    });

}).on('error', (error) => {
    console.error('❌ Error fetching CSV:', error);
    process.exit(1);
});