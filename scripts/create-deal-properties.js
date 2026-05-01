/**
 * Create deal-level properties needed by the contract system
 *
 * Usage:
 *   HUBSPOT_ACCESS_TOKEN=pat-xxx node scripts/create-deal-properties.js
 */

const axios = require('axios');

const TOKEN = process.env.HUBSPOT_ACCESS_TOKEN;
if (!TOKEN) {
  console.error('Set HUBSPOT_ACCESS_TOKEN env var');
  process.exit(1);
}

const hs = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
});

const PROPERTIES = [
  {
    name: 'deal_category',
    label: 'Deal Category',
    type: 'enumeration',
    fieldType: 'select',
    groupName: 'dealinformation',
    description: 'Classifies the deal: new business, renewal, expansion, or contraction',
    options: [
      { label: 'New Business', value: 'new_business' },
      { label: 'Renewal', value: 'renewal' },
      { label: 'Expansion', value: 'expansion' },
      { label: 'Contraction', value: 'contraction' },
    ],
  },
  {
    name: 'contract_start_date',
    label: 'Contract Start Date',
    type: 'date',
    fieldType: 'date',
    groupName: 'dealinformation',
    description: 'Start date for the contract associated with this deal',
  },
  {
    name: 'contract_end_date',
    label: 'Contract End Date',
    type: 'date',
    fieldType: 'date',
    groupName: 'dealinformation',
    description: 'End date for the contract associated with this deal',
  },
  {
    name: 'total_tcv',
    label: 'Total TCV',
    type: 'number',
    fieldType: 'number',
    groupName: 'dealinformation',
    description:
      'Total Contract Value for the deal. On auto-spawned renewal deals this equals the deal amount (last-segment ARR × renewal term years).',
  },
  {
    name: 'year_1_arr',
    label: 'Year 1 ARR',
    type: 'number',
    fieldType: 'number',
    groupName: 'dealinformation',
    description:
      'Annualized recurring revenue for Year 1 of the deal. On auto-spawned renewal deals this is the source contract\'s last-segment ARR (i.e. the run rate carried into the renewal\'s Year 1).',
  },
];

async function main() {
  console.log('Creating deal properties for FinQuery contract system...\n');

  for (const prop of PROPERTIES) {
    try {
      await hs.post('/crm/v3/properties/deals', prop);
      console.log(`  ✓ Created: ${prop.name}`);
    } catch (err) {
      if (err.response?.status === 409) {
        console.log(`  — Already exists: ${prop.name}`);
      } else {
        console.error(`  ✗ Failed: ${prop.name} — ${err.response?.data?.message || err.message}`);
      }
    }
  }

  console.log('\nDone.');
}

main();
