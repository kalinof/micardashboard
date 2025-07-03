const https = require('https');
const fs = require('fs');

// Your CSV URL from Step 1
const csvUrl = 'https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&gid=0';

function csvToArray(str, delimiter = ',') {
    const headers = str.slice(0, str.indexOf('\n')).split(delimiter);
    const rows = str.slice(str.indexOf('\n') + 1).split('\n');

    return rows.map(row => {
        const values = row.split(delimiter);
        return headers.reduce((object, header, index) => {
            object[header.trim()] = values[index] ? values[index].trim() : '';
            return object;
        }, {});
    }).filter(row => row['#'] && row['#'] !== ''); // Filter out empty rows
}

function convertToJsData(csvData) {
    const data = [];

    csvData.forEach((row, index) => {
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
        }
    });

    return data;
}

function updateHtmlFile(newData) {
    const htmlFile = 'EMT Dashboard(30 June 2025).html';

    if (!fs.existsSync(htmlFile)) {
        console.error('HTML file not found:', htmlFile);
        return;
    }

    let htmlContent = fs.readFileSync(htmlFile, 'utf8');

    // Find the data array in the JavaScript section
    const dataStart = htmlContent.indexOf('const data = [');
    const dataEnd = htmlContent.indexOf('];', dataStart) + 2;

    if (dataStart === -1 || dataEnd === -1) {
        console.error('Could not find data array in HTML file');
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
}

// Main execution
console.log('🔄 Fetching data from Google Sheets...');

https.get(csvUrl, (res) => {
    let data = '';

    res.on('data', (chunk) => {
        data += chunk;
    });

    res.on('end', () => {
        try {
            console.log('📋 Converting CSV data...');
            const csvArray = csvToArray(data);
            const jsData = convertToJsData(csvArray);

            console.log(`📈 Found ${jsData.length} valid entries`);
            updateHtmlFile(jsData);

        } catch (error) {
            console.error('❌ Error processing data:', error);
            process.exit(1);
        }
    });

}).on('error', (error) => {
    console.error('❌ Error fetching CSV:', error);
    process.exit(1);
});