const https = require('https');
const fs = require('fs');

// Your CSV URL
const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRWudeV0zFLqB54658hCDUgSRFfy-ADeR2JMilO-oel74hjBr1CdIB2FWufxyR2yuQJGNaBPHNYG7vh/pub?output=csv';

function csvToArray(str, delimiter = ',') {
    // Handle quoted fields properly
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

    return result.filter(row => row['#'] && row['#'] !== '' && !isNaN(parseInt(row['#'])));
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

        if (row['#'] && row['Issuer (HQ)']) {
            const item = {
                id: parseInt(row['#']) || index + 1,
                issuer: row['Issuer (HQ)'] || '',
                state: row['Home State'] || '',
                authority: row['Competent Authority'] || '',
                tokens: row['Authorised EMT(s)'] || '',
                count: parseInt(row['Tokens']) || 0,
                euro: parseInt(row['Euro']) || 0,
                usd: parseInt(row['USD']) || 0,
                czk: parseInt(row['CZK']) || 0,
                gbp: parseInt(row['GBP']) || 0
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

    // Find the data array in the JavaScript section
    const dataStart = htmlContent.indexOf('const data = [');
    const dataEnd = htmlContent.indexOf('];', dataStart) + 2;

    if (dataStart === -1 || dataEnd === -1) {
        console.error('❌ Could not find data array in HTML file');
        console.log('🔍 Looking for alternative patterns...');

        // Try alternative patterns
        const altStart = htmlContent.indexOf('data = [');
        const altEnd = htmlContent.indexOf('];', altStart) + 2;

        if (altStart !== -1 && altEnd !== -1) {
            console.log('✅ Found alternative data pattern');
            const newDataString = `data = ${JSON.stringify(newData, null, 4)};`;
            const updatedHtml = htmlContent.substring(0, altStart) + newDataString + htmlContent.substring(altEnd);
            fs.writeFileSync(htmlFile, updatedHtml);
            console.log('✅ Dashboard updated successfully with alternative pattern!');
            return;
        }

        console.error('❌ Could not find any data array pattern');
        return;
    }

    // Replace the data array
    const newDataString = `const data = ${JSON.stringify(newData, null, 4)};`;
    const updatedHtml = htmlContent.substring(0, dataStart) + newDataString + htmlContent.substring(dataEnd);

    // Update the date in the HTML
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

    console.log('📈 Summary:');
    console.log(`   Total Issuers: ${newData.length}`);
    console.log(`   Total Tokens: ${totalTokens}`);
    console.log(`   EUR Tokens: ${euroTokens}`);
    console.log(`   USD Tokens: ${usdTokens}`);
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