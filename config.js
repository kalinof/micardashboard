const path = require('path');

const DEFAULT_CSV_URL = 'https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&id=1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE&gid=0';
const DEFAULT_DATE_URL = 'https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&gid=353293525';
const DEFAULT_NON_COMPLIANT_PATH = path.join(__dirname, 'out', 'non_compliant.csv');
const DEFAULT_CASPS_PATH = path.join(__dirname, 'out', 'casps.csv');

module.exports = {
    csvUrl: process.env.CSV_URL || DEFAULT_CSV_URL,
    dateUrl: process.env.DATE_URL || DEFAULT_DATE_URL,
    nonCompliantPath: process.env.NON_COMPLIANT_PATH || DEFAULT_NON_COMPLIANT_PATH,
    caspsPath: process.env.CASPS_PATH || DEFAULT_CASPS_PATH
};

