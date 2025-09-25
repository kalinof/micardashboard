const DEFAULT_CSV_URL = 'https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&id=1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE&gid=0';
const DEFAULT_DATE_URL = 'https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&gid=353293525';

module.exports = {
    csvUrl: process.env.CSV_URL || DEFAULT_CSV_URL,
    dateUrl: process.env.DATE_URL || DEFAULT_DATE_URL
};

