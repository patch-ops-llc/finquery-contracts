const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

// ── Auth ─────────────────────────────────────────────────────────────────────
const TOKEN = process.env.HUBSPOT_ACCESS_TOKEN;
if (!TOKEN) {
  console.error('HUBSPOT_ACCESS_TOKEN env var is required');
  process.exit(1);
}

const hs = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
});

// ── Product Registry ─────────────────────────────────────────────────────────
const PRODUCT_REGISTRY = {
  LQ:  { code: 'LQ',  name: 'LeaseQuery',                category: 'core', arrField: 'lq_arr' },
  FCM: { code: 'FCM', name: 'Financial Close Management', category: 'core', arrField: 'fcm_arr' },
};

// ── Type ID Cache ────────────────────────────────────────────────────────────
let contractTypeId = null;
let subscriptionTypeId = null;

async function resolveTypeIds() {
  if (contractTypeId && subscriptionTypeId) return;
  const { data } = await hs.get('/crm/v3/schemas');
  for (const s of data.results) {
    if (s.name === 'fq_contract') contractTypeId = s.objectTypeId;
    if (s.name === 'fq_subscription') subscriptionTypeId = s.objectTypeId;
  }
}

// ── Schema Definitions ──────────────────────────────────────────────────────

const CONTRACT_SCHEMA = {
  name: 'fq_contract',
  labels: { singular: 'Contract', plural: 'Contracts' },
  primaryDisplayProperty: 'contract_name',
  requiredProperties: ['contract_name'],
  searchableProperties: ['contract_name', 'contract_number', 'status'],
  properties: [
    // ── Core identity ───────────────────────────────────────────────────
    { name: 'contract_name', label: 'Contract Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
    { name: 'contract_number', label: 'Contract Number', type: 'string', fieldType: 'text' },
    { name: 'sf_contract_id', label: 'Salesforce Contract ID', type: 'string', fieldType: 'text' },
    { name: 'description', label: 'Description', type: 'string', fieldType: 'textarea' },

    // ── Status ──────────────────────────────────────────────────────────
    {
      name: 'status', label: 'Status', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Draft', value: 'draft' },
        { label: 'In Approval Process', value: 'in_approval_process' },
        { label: 'Active', value: 'active' },
        { label: 'Future', value: 'future' },
        { label: 'Inactive', value: 'inactive' },
        { label: 'Expired', value: 'expired' },
        { label: 'Terminated', value: 'terminated' },
      ],
    },
    {
      name: 'termination_reason', label: 'Termination Reason', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Customer Cancellation', value: 'customer_cancellation' },
        { label: 'Non-Payment', value: 'non_payment' },
        { label: 'Contract Amendment', value: 'amendment' },
        { label: 'Replaced', value: 'replaced' },
        { label: 'Term Expired', value: 'term_expired' },
        { label: 'Manual', value: 'manual' },
        { label: 'Other', value: 'other' },
      ],
    },

    // ── Dates ────────────────────────────────────────────────────────────
    { name: 'start_date', label: 'Contract Start Date', type: 'date', fieldType: 'date' },
    { name: 'end_date', label: 'Contract End Date', type: 'date', fieldType: 'date' },
    { name: 'co_term_date', label: 'Co-Term Date', type: 'date', fieldType: 'date' },
    { name: 'activated_date', label: 'Activated Date', type: 'date', fieldType: 'date' },
    { name: 'terminated_date', label: 'Terminated Date', type: 'date', fieldType: 'date' },
    { name: 'company_signed_date', label: 'Company Signed Date', type: 'date', fieldType: 'date' },
    { name: 'customer_signed_date', label: 'Customer Signed Date', type: 'date', fieldType: 'date' },
    { name: 'customer_signed_title', label: 'Customer Signed Title', type: 'string', fieldType: 'text' },
    { name: 'amendment_start_date', label: 'Amendment Start Date', type: 'date', fieldType: 'date' },
    { name: 'contract_renewed_on', label: 'Contract Renewed On', type: 'date', fieldType: 'date' },

    // ── Term ─────────────────────────────────────────────────────────────
    { name: 'contract_term', label: 'Contract Term (months)', type: 'number', fieldType: 'number' },
    { name: 'previous_contract_term', label: 'Previous Contract Term', type: 'number', fieldType: 'number' },
    { name: 'renewal_term', label: 'Renewal Term', type: 'number', fieldType: 'number' },
    {
      name: 'evergreen', label: 'Evergreen', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },

    // ── Financial / ARR ──────────────────────────────────────────────────
    { name: 'total_arr', label: 'Total ARR', type: 'number', fieldType: 'number' },
    { name: 'lq_arr', label: 'LQ Year1 ARR', type: 'number', fieldType: 'number' },
    { name: 'fcm_arr', label: 'FCM Year1 ARR', type: 'number', fieldType: 'number' },
    { name: 'portfolio_management_arr', label: 'Portfolio Management Year1 ARR', type: 'number', fieldType: 'number' },
    { name: 'total_tcv', label: 'Total TCV', type: 'number', fieldType: 'number' },

    // ── Pricing / Renewal ────────────────────────────────────────────────
    { name: 'price_cap', label: 'Price Cap (%)', type: 'number', fieldType: 'number' },
    { name: 'max_uplift', label: 'Max Uplift (%)', type: 'number', fieldType: 'number' },
    { name: 'renewal_uplift_rate', label: 'Renewal Uplift (%)', type: 'number', fieldType: 'number' },
    {
      name: 'amendment_renewal_behavior', label: 'Amendment & Renewal Behavior', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Latest End Date', value: 'latest_end_date' },
        { label: 'Earliest End Date', value: 'earliest_end_date' },
      ],
    },
    {
      name: 'mdq_renewal_behavior', label: 'MDQ Renewal Behavior', type: 'enumeration', fieldType: 'select',
      options: [{ label: 'De-segmented', value: 'de_segmented' }],
    },
    {
      name: 'renewal_forecast', label: 'Renewal Forecast', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },
    {
      name: 'renewal_quoted', label: 'Renewal Quoted', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },

    // ── Subscription rollups (populated by API) ──────────────────────────
    { name: 'subscription_count', label: 'Subscription Count', type: 'number', fieldType: 'number' },
    { name: 'amendment_count', label: 'Amendment Count', type: 'number', fieldType: 'number' },
    { name: 'lq_active_subscriptions', label: 'LQ Current Active Record Subscriptions', type: 'number', fieldType: 'number' },
    { name: 'lq_archived_subscriptions', label: 'LQ Current Archived Record Subscriptions', type: 'number', fieldType: 'number' },
    { name: 'fcm_subscription_count', label: 'FCM Subscription Count', type: 'number', fieldType: 'number' },
    { name: 'lq_subscription_count', label: 'Total LQ Subscription Count', type: 'number', fieldType: 'number' },
    { name: 'fcm_core_product_count', label: 'FCM Core Product Count', type: 'number', fieldType: 'number' },
    { name: 'fcm_additional_count', label: 'FCM Additional Count', type: 'number', fieldType: 'number' },
    { name: 'portfolio_management_hours', label: 'Portfolio Management Hours Purchased', type: 'number', fieldType: 'number' },

    // ── People ───────────────────────────────────────────────────────────
    { name: 'activated_by', label: 'Activated By', type: 'string', fieldType: 'text' },
    { name: 'renewal_owner', label: 'Renewal Owner', type: 'string', fieldType: 'text' },
    { name: 'amendment_owner', label: 'Amendment Owner', type: 'string', fieldType: 'text' },

    // ── Billing address ──────────────────────────────────────────────────
    { name: 'billing_street', label: 'Billing Street', type: 'string', fieldType: 'text' },
    { name: 'billing_city', label: 'Billing City', type: 'string', fieldType: 'text' },
    { name: 'billing_state', label: 'Billing State/Province', type: 'string', fieldType: 'text' },
    { name: 'billing_postal_code', label: 'Billing Zip/Postal Code', type: 'string', fieldType: 'text' },
    { name: 'billing_country', label: 'Billing Country', type: 'string', fieldType: 'text' },

    // ── Special terms / notes ────────────────────────────────────────────
    { name: 'special_terms', label: 'Special Terms', type: 'string', fieldType: 'textarea' },

    // ── Integration ──────────────────────────────────────────────────────
    { name: 'netsuite_id', label: 'NetSuite ID', type: 'string', fieldType: 'text' },

    // ── JSON blob for UIE ────────────────────────────────────────────────
    { name: 'contract_data', label: 'Contract Data', type: 'string', fieldType: 'textarea' },
  ],
  associatedObjects: ['COMPANY', 'DEAL', 'CONTACT'],
};

const SUBSCRIPTION_SCHEMA = {
  name: 'fq_subscription',
  labels: { singular: 'Subscription Segment', plural: 'Subscription Segments' },
  primaryDisplayProperty: 'segment_name',
  requiredProperties: ['segment_name'],
  searchableProperties: ['segment_name', 'product_code', 'product_name', 'status'],
  properties: [
    // ── Core identity ───────────────────────────────────────────────────
    { name: 'segment_name', label: 'Segment Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
    { name: 'sf_subscription_id', label: 'Salesforce Subscription ID', type: 'string', fieldType: 'text' },
    { name: 'subscription_number', label: 'Subscription #', type: 'string', fieldType: 'text' },

    // ── Product ──────────────────────────────────────────────────────────
    { name: 'product_code', label: 'Product Code', type: 'string', fieldType: 'text' },
    { name: 'product_name', label: 'Product Name', type: 'string', fieldType: 'text' },
    {
      name: 'product_subscription_type', label: 'Product Subscription Type', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Evergreen', value: 'evergreen' },
        { label: 'One-time', value: 'one_time' },
        { label: 'Renewable', value: 'renewable' },
        { label: 'Renewable/Evergreen', value: 'renewable_evergreen' },
      ],
    },
    {
      name: 'subscription_type', label: 'Subscription Type', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Evergreen', value: 'evergreen' },
        { label: 'One-time', value: 'one_time' },
        { label: 'Renewable', value: 'renewable' },
        { label: 'Renewable/Evergreen', value: 'renewable_evergreen' },
      ],
    },
    {
      name: 'charge_type', label: 'Charge Type', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'One-Time', value: 'one_time' },
        { label: 'Recurring', value: 'recurring' },
        { label: 'Usage', value: 'usage' },
      ],
    },
    {
      name: 'billing_frequency', label: 'Billing Frequency', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Annual', value: 'annual' },
        { label: 'Monthly', value: 'monthly' },
        { label: 'Quarterly', value: 'quarterly' },
        { label: 'Semiannual', value: 'semiannual' },
        { label: 'Invoice Plan', value: 'invoice_plan' },
      ],
    },

    // ── Status ──────────────────────────────────────────────────────────
    {
      name: 'status', label: 'Status', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Active', value: 'active' },
        { label: 'Future', value: 'future' },
        { label: 'Inactive', value: 'inactive' },
        { label: 'Terminated', value: 'terminated' },
      ],
    },
    {
      name: 'proration_status', label: 'Proration Status', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'None', value: 'none' },
        { label: 'Prorated', value: 'prorated' },
        { label: 'Free Month', value: 'free_month' },
      ],
    },
    { name: 'amendment_indicator', label: 'Amendment Indicator', type: 'string', fieldType: 'text' },
    {
      name: 'bundled', label: 'Bundled', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },

    // ── Dates ────────────────────────────────────────────────────────────
    { name: 'start_date', label: 'Start Date', type: 'date', fieldType: 'date' },
    { name: 'end_date', label: 'End Date', type: 'date', fieldType: 'date' },
    { name: 'subscription_start_date', label: 'Subscription Start Date', type: 'date', fieldType: 'date' },
    { name: 'subscription_end_date', label: 'Subscription End Date', type: 'date', fieldType: 'date' },
    { name: 'arr_start_date', label: 'ARR Start Date', type: 'date', fieldType: 'date' },
    { name: 'arr_end_date', label: 'ARR End Date', type: 'date', fieldType: 'date' },
    { name: 'terminated_date', label: 'Terminated Date', type: 'date', fieldType: 'date' },
    { name: 'renewed_date', label: 'Renewed Date', type: 'date', fieldType: 'date' },

    // ── Segment (MDQ) ────────────────────────────────────────────────────
    { name: 'segment_year', label: 'Segment Year', type: 'number', fieldType: 'number' },
    { name: 'segment_label', label: 'Segment Label', type: 'string', fieldType: 'text' },
    { name: 'segment_index', label: 'Segment Index', type: 'number', fieldType: 'number' },
    { name: 'segment_key', label: 'Segment Key', type: 'string', fieldType: 'text' },
    { name: 'segment_start_date', label: 'Segment Start Date', type: 'date', fieldType: 'date' },
    { name: 'segment_end_date', label: 'Segment End Date', type: 'date', fieldType: 'date' },
    { name: 'segment_quantity', label: 'Segment Quantity', type: 'number', fieldType: 'number' },
    { name: 'segment_uplift', label: 'Segment Uplift (%)', type: 'number', fieldType: 'number' },
    { name: 'segment_uplift_amount', label: 'Segment Uplift (Amt)', type: 'number', fieldType: 'number' },

    // ── Quantity ──────────────────────────────────────────────────────────
    { name: 'quantity', label: 'Quantity', type: 'number', fieldType: 'number' },
    { name: 'original_quantity', label: 'Original Quantity', type: 'number', fieldType: 'number' },
    { name: 'renewal_quantity', label: 'Renewal Quantity', type: 'number', fieldType: 'number' },
    { name: 'number_position', label: 'Number', type: 'number', fieldType: 'number' },
    { name: 'option_level', label: 'Option Level', type: 'number', fieldType: 'number' },
    {
      name: 'option_type', label: 'Option Type', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Accessory', value: 'accessory' },
        { label: 'Component', value: 'component' },
        { label: 'Related Product', value: 'related_product' },
      ],
    },

    // ── Pricing ──────────────────────────────────────────────────────────
    { name: 'unit_price', label: 'Unit Price', type: 'number', fieldType: 'number' },
    { name: 'list_price', label: 'List Price', type: 'number', fieldType: 'number' },
    { name: 'net_price', label: 'Net Price', type: 'number', fieldType: 'number' },
    { name: 'regular_price', label: 'Regular Price', type: 'number', fieldType: 'number' },
    { name: 'special_price', label: 'Special Price', type: 'number', fieldType: 'number' },
    { name: 'customer_price', label: 'Customer Price', type: 'number', fieldType: 'number' },
    { name: 'discount_percent', label: 'Additional Disc. (%)', type: 'number', fieldType: 'number' },
    { name: 'discount_amount', label: 'Additional Disc. (Amt)', type: 'number', fieldType: 'number' },
    { name: 'prorate_multiplier', label: 'Prorate Multiplier', type: 'number', fieldType: 'number' },
    {
      name: 'pricing_method', label: 'Pricing Method', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'List', value: 'list' },
        { label: 'Cost', value: 'cost' },
        { label: 'Block', value: 'block' },
        { label: 'Custom', value: 'custom' },
        { label: 'Percent Of Total', value: 'percent_of_total' },
      ],
    },
    {
      name: 'subscription_pricing', label: 'Subscription Pricing', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Fixed Price', value: 'fixed_price' },
        { label: 'Percent Of Total', value: 'percent_of_total' },
      ],
    },

    // ── Revenue metrics ──────────────────────────────────────────────────
    { name: 'arr', label: 'ARR', type: 'number', fieldType: 'number' },
    { name: 'mrr', label: 'MRR', type: 'number', fieldType: 'number' },
    { name: 'tcv', label: 'TCV', type: 'number', fieldType: 'number' },

    // ── Renewal pricing ──────────────────────────────────────────────────
    { name: 'renewal_price', label: 'Renewal Price', type: 'number', fieldType: 'number' },
    { name: 'renewal_list_price_override', label: 'Renewal List Price Override', type: 'number', fieldType: 'number' },
    { name: 'renewal_uplift_rate', label: 'Renewal Uplift (%)', type: 'number', fieldType: 'number' },
    { name: 'price_cap_amount', label: 'Price Cap Amount', type: 'number', fieldType: 'number' },
    { name: 'price_cap_multiplier', label: 'Price Cap Multiplier', type: 'number', fieldType: 'number' },
  ],
  associatedObjects: ['COMPANY'],
};

const DEAL_PROPERTIES = [
  {
    name: 'deal_category', label: 'Deal Category', type: 'enumeration', fieldType: 'select',
    groupName: 'dealinformation',
    options: [
      { label: 'New Business', value: 'new_business' },
      { label: 'Renewal', value: 'renewal' },
      { label: 'Expansion', value: 'expansion' },
      { label: 'Contraction', value: 'contraction' },
    ],
  },
  { name: 'contract_start_date', label: 'Contract Start Date', type: 'date', fieldType: 'date', groupName: 'dealinformation' },
  { name: 'contract_end_date', label: 'Contract End Date', type: 'date', fieldType: 'date', groupName: 'dealinformation' },
];

// ── CRM helpers ──────────────────────────────────────────────────────────────

const CONTRACT_PROPS = [
  'contract_name', 'contract_number', 'sf_contract_id', 'description',
  'status', 'termination_reason',
  'start_date', 'end_date', 'co_term_date', 'activated_date', 'terminated_date',
  'company_signed_date', 'customer_signed_date', 'customer_signed_title',
  'amendment_start_date', 'contract_renewed_on',
  'contract_term', 'previous_contract_term', 'renewal_term', 'evergreen',
  'total_arr', 'lq_arr', 'fcm_arr', 'portfolio_management_arr', 'total_tcv',
  'price_cap', 'max_uplift', 'renewal_uplift_rate',
  'amendment_renewal_behavior', 'mdq_renewal_behavior',
  'renewal_forecast', 'renewal_quoted',
  'subscription_count', 'amendment_count',
  'lq_active_subscriptions', 'lq_archived_subscriptions',
  'fcm_subscription_count', 'lq_subscription_count',
  'fcm_core_product_count', 'fcm_additional_count',
  'portfolio_management_hours',
  'activated_by', 'renewal_owner', 'amendment_owner',
  'billing_street', 'billing_city', 'billing_state', 'billing_postal_code', 'billing_country',
  'special_terms', 'netsuite_id', 'contract_data',
];

const SUBSCRIPTION_PROPS = [
  'segment_name', 'sf_subscription_id', 'subscription_number',
  'product_code', 'product_name', 'product_subscription_type',
  'subscription_type', 'charge_type', 'billing_frequency',
  'status', 'proration_status', 'amendment_indicator', 'bundled',
  'start_date', 'end_date', 'subscription_start_date', 'subscription_end_date',
  'arr_start_date', 'arr_end_date', 'terminated_date', 'renewed_date',
  'segment_year', 'segment_label', 'segment_index', 'segment_key',
  'segment_start_date', 'segment_end_date', 'segment_quantity',
  'segment_uplift', 'segment_uplift_amount',
  'quantity', 'original_quantity', 'renewal_quantity',
  'number_position', 'option_level', 'option_type',
  'unit_price', 'list_price', 'net_price', 'regular_price',
  'special_price', 'customer_price',
  'discount_percent', 'discount_amount', 'prorate_multiplier',
  'pricing_method', 'subscription_pricing',
  'arr', 'mrr', 'tcv',
  'renewal_price', 'renewal_list_price_override',
  'renewal_uplift_rate', 'price_cap_amount', 'price_cap_multiplier',
];

async function getObject(typeId, objectId, properties) {
  const { data } = await hs.get(`/crm/v3/objects/${typeId}/${objectId}`, {
    params: { properties: properties.join(',') },
  });
  return data;
}

async function createObject(typeId, properties) {
  const { data } = await hs.post(`/crm/v3/objects/${typeId}`, { properties });
  return data;
}

async function updateObject(typeId, objectId, properties) {
  const { data } = await hs.patch(`/crm/v3/objects/${typeId}/${objectId}`, { properties });
  return data;
}

async function getAssociatedIds(fromType, fromId, toType) {
  try {
    const { data } = await hs.get(`/crm/v4/objects/${fromType}/${fromId}/associations/${toType}`);
    return (data.results || []).map((r) => r.toObjectId);
  } catch (e) {
    if (e.response?.status === 404) return [];
    throw e;
  }
}

async function createAssociation(fromType, fromId, toType, toId) {
  await hs.put(
    `/crm/v4/objects/${fromType}/${fromId}/associations/default/${toType}/${toId}`
  );
}

function fmtDateForHS(d) {
  if (!d) return null;
  const dt = d instanceof Date ? d : new Date(d);
  if (isNaN(dt.getTime())) return null;
  return dt.toISOString().split('T')[0];
}

function determineStatus(startDate, endDate) {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  const start = startDate ? new Date(startDate) : null;
  const end = endDate ? new Date(endDate) : null;
  if (start) start.setHours(0, 0, 0, 0);
  if (end) end.setHours(0, 0, 0, 0);

  if (start && start > now) return 'future';
  if (end && end < now) return 'inactive';
  if (start && start <= now && (!end || end >= now)) return 'active';
  return 'inactive';
}

function calcMetrics(subscriptions) {
  const metrics = { total_arr: 0, total_tcv: 0, lq_arr: 0, fcm_arr: 0, subscription_count: subscriptions.length };
  for (const sub of subscriptions) {
    const sp = sub.properties || {};
    if (sp.status === 'terminated') continue;
    const arr = parseFloat(sp.arr) || 0;
    const tcv = parseFloat(sp.tcv) || 0;
    metrics.total_arr += arr;
    metrics.total_tcv += tcv;
    const code = (sp.product_code || '').toUpperCase();
    if (code === 'LQ') metrics.lq_arr += arr;
    if (code === 'FCM') metrics.fcm_arr += arr;
  }
  return metrics;
}

// ── Self-healing setup ───────────────────────────────────────────────────────

async function ensureSetup() {
  console.log('[ensureSetup] Checking schemas...');
  const { data: schemas } = await hs.get('/crm/v3/schemas');
  let contractExists = false;
  let subscriptionExists = false;

  for (const s of schemas.results) {
    if (s.name === 'fq_contract') { contractTypeId = s.objectTypeId; contractExists = true; }
    if (s.name === 'fq_subscription') { subscriptionTypeId = s.objectTypeId; subscriptionExists = true; }
  }

  if (!contractExists) {
    console.log('[ensureSetup] Creating fq_contract schema...');
    try {
      const { data } = await hs.post('/crm/v3/schemas', CONTRACT_SCHEMA);
      contractTypeId = data.objectTypeId;
      console.log(`[ensureSetup] Created fq_contract: ${contractTypeId}`);
    } catch (e) {
      if (e.response?.status === 409) {
        console.log('[ensureSetup] fq_contract already exists (409)');
        await resolveTypeIds();
      } else throw e;
    }
  }

  if (!subscriptionExists) {
    console.log('[ensureSetup] Creating fq_subscription schema...');
    try {
      const { data } = await hs.post('/crm/v3/schemas', SUBSCRIPTION_SCHEMA);
      subscriptionTypeId = data.objectTypeId;
      console.log(`[ensureSetup] Created fq_subscription: ${subscriptionTypeId}`);
    } catch (e) {
      if (e.response?.status === 409) {
        console.log('[ensureSetup] fq_subscription already exists (409)');
        await resolveTypeIds();
      } else throw e;
    }
  }

  if (contractTypeId && subscriptionTypeId) {
    try {
      await hs.post(`/crm/v4/associations/${contractTypeId}/${subscriptionTypeId}/labels`, {
        label: 'Contract to Subscription',
        name: 'contract_to_subscription',
      });
      console.log('[ensureSetup] Created Contract ↔ Subscription association');
    } catch (e) {
      if (e.response?.status === 409 || e.response?.status === 400) {
        console.log('[ensureSetup] Association already exists');
      } else {
        console.warn('[ensureSetup] Association creation warning:', e.response?.data?.message || e.message);
      }
    }
  }

  for (const prop of DEAL_PROPERTIES) {
    try {
      await hs.post('/crm/v3/properties/deals', prop);
      console.log(`[ensureSetup] Created deal property: ${prop.name}`);
    } catch (e) {
      if (e.response?.status === 409) continue;
      console.warn(`[ensureSetup] Deal property ${prop.name}:`, e.response?.data?.message || e.message);
    }
  }

  console.log(`[ensureSetup] Ready — contract=${contractTypeId}, subscription=${subscriptionTypeId}`);
  return { contractTypeId, subscriptionTypeId, productRegistry: PRODUCT_REGISTRY };
}

// ── Route: Health ────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({ status: 'ok', service: 'finquery-contracts-api' }));

app.get('/api/health', (req, res) => res.json({ status: 'ok', contractTypeId, subscriptionTypeId }));

// ── Route: Ensure Setup ──────────────────────────────────────────────────────

app.get('/api/ensure-setup', async (req, res) => {
  try {
    const result = await ensureSetup();
    res.json({ success: true, ...result });
  } catch (e) {
    console.error('[ensure-setup] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Load Contract ─────────────────────────────────────────────────────

app.get('/api/load-contract', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();
    if (!contractTypeId) return res.status(500).json({ success: false, message: 'Contract schema not found' });

    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);

    let subscriptions = [];
    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      if (subIds.length > 0) {
        const fetches = subIds.map((id) => getObject(subscriptionTypeId, id, SUBSCRIPTION_PROPS));
        subscriptions = await Promise.all(fetches);
      }
    }

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    let company = null;
    if (companyIds.length > 0) {
      try {
        company = await getObject('0-2', companyIds[0], ['name', 'domain', 'city', 'state', 'country']);
      } catch (e) { /* non-critical */ }
    }

    const dealIds = await getAssociatedIds(contractTypeId, contractId, '0-3');
    const deals = [];
    for (const did of dealIds) {
      try {
        const d = await getObject('0-3', did, ['dealname', 'dealstage', 'amount', 'closedate', 'deal_category', 'pipeline']);
        deals.push({
          id: d.id,
          name: d.properties.dealname,
          stage: d.properties.dealstage,
          amount: d.properties.amount,
          closeDate: d.properties.closedate,
          category: d.properties.deal_category,
          pipeline: d.properties.pipeline,
        });
      } catch (e) { /* skip inaccessible deals */ }
    }

    const contactIds = await getAssociatedIds(contractTypeId, contractId, '0-1');
    const contacts = [];
    for (const cid of contactIds) {
      try {
        const c = await getObject('0-1', cid, ['firstname', 'lastname', 'email', 'jobtitle']);
        contacts.push({
          id: c.id,
          name: [c.properties.firstname, c.properties.lastname].filter(Boolean).join(' '),
          email: c.properties.email,
          title: c.properties.jobtitle,
        });
      } catch (e) { /* skip */ }
    }

    let portalId = null;
    try {
      const { data: acct } = await hs.get('/account-info/v3/details');
      portalId = acct.portalId;
    } catch (e) { /* non-critical */ }

    res.json({
      success: true,
      contract,
      subscriptions,
      company,
      deals,
      contacts,
      portalId,
      productRegistry: PRODUCT_REGISTRY,
    });
  } catch (e) {
    console.error('[load-contract] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Start Amendment ───────────────────────────────────────────────────

app.get('/api/start-amendment', async (req, res) => {
  try {
    const { contractId, amendmentType, startDate } = req.query;
    if (!contractId || !amendmentType) {
      return res.status(400).json({ success: false, message: 'contractId and amendmentType required' });
    }

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;
    const dealName = `${props.contract_name || 'Contract'} — ${amendmentType === 'expansion' ? 'Expansion' : 'Contraction'} Amendment`;

    const dealProps = {
      dealname: dealName,
      dealstage: 'appointmentscheduled',
      deal_category: amendmentType,
      contract_start_date: startDate || fmtDateForHS(new Date()),
      contract_end_date: props.end_date || null,
      pipeline: 'default',
    };

    const deal = await createObject('0-3', dealProps);

    const warnings = [];

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    if (companyIds.length > 0) {
      try {
        await createAssociation('0-3', deal.id, '0-2', companyIds[0]);
      } catch (e) {
        console.warn('[start-amendment] Company association failed (likely stale contact IDs on company):', e.response?.data?.message || e.message);
        warnings.push('Deal created but company association failed — some contacts on this company may be invalid');
      }
    }

    try {
      await createAssociation(contractTypeId, contractId, '0-3', deal.id);
    } catch (e) {
      console.warn('[start-amendment] Could not associate deal to contract:', e.message);
    }

    const amendCount = (parseInt(props.amendment_count) || 0) + 1;
    await updateObject(contractTypeId, contractId, {
      amendment_count: String(amendCount),
    });

    res.json({
      success: true,
      message: `${amendmentType === 'expansion' ? 'Expansion' : 'Contraction'} amendment deal created`,
      dealId: deal.id,
      dealName,
      warnings: warnings.length > 0 ? warnings : undefined,
    });
  } catch (e) {
    console.error('[start-amendment] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Terminate Contract ────────────────────────────────────────────────

app.get('/api/terminate-contract', async (req, res) => {
  try {
    const { contractId, reason } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();

    await updateObject(contractTypeId, contractId, {
      status: 'terminated',
      terminated_date: fmtDateForHS(new Date()),
      termination_reason: reason || 'manual',
    });

    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      for (const sid of subIds) {
        try {
          await updateObject(subscriptionTypeId, sid, { status: 'terminated' });
        } catch (e) { console.warn(`[terminate] Sub ${sid}:`, e.message); }
      }
    }

    res.json({ success: true, message: 'Contract terminated' });
  } catch (e) {
    console.error('[terminate-contract] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Reverse Termination ───────────────────────────────────────────────

app.get('/api/reverse-termination', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;

    if (props.status !== 'terminated') {
      return res.json({ success: false, message: 'Contract is not terminated' });
    }

    const newStatus = determineStatus(props.start_date, props.end_date);

    await updateObject(contractTypeId, contractId, {
      status: newStatus,
      terminated_date: '',
      termination_reason: '',
    });

    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      for (const sid of subIds) {
        try {
          const sub = await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS);
          if (sub.properties.status === 'terminated') {
            const subStatus = determineStatus(sub.properties.start_date, sub.properties.end_date);
            await updateObject(subscriptionTypeId, sid, { status: subStatus });
          }
        } catch (e) { /* skip */ }
      }
    }

    res.json({
      success: true,
      message: `Termination reversed — contract status set to ${newStatus}`,
      status: newStatus,
    });
  } catch (e) {
    console.error('[reverse-termination] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Create Renewal Deal ───────────────────────────────────────────────

app.get('/api/create-renewal-deal', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;

    const currentEnd = props.end_date ? new Date(props.end_date) : new Date();
    const renewalStart = new Date(currentEnd);
    renewalStart.setDate(renewalStart.getDate() + 1);
    const renewalEnd = new Date(renewalStart);
    renewalEnd.setFullYear(renewalEnd.getFullYear() + 1);

    const dealName = `${props.contract_name || 'Contract'} — Renewal`;

    const deal = await createObject('0-3', {
      dealname: dealName,
      dealstage: 'appointmentscheduled',
      deal_category: 'renewal',
      contract_start_date: fmtDateForHS(renewalStart),
      contract_end_date: fmtDateForHS(renewalEnd),
      amount: props.total_arr || '0',
      pipeline: 'default',
    });

    const warnings = [];

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    if (companyIds.length > 0) {
      try {
        await createAssociation('0-3', deal.id, '0-2', companyIds[0]);
      } catch (e) {
        console.warn('[create-renewal] Company association failed (likely stale contact IDs on company):', e.response?.data?.message || e.message);
        warnings.push('Deal created but company association failed — some contacts on this company may be invalid');
      }
    }

    try {
      await createAssociation(contractTypeId, contractId, '0-3', deal.id);
    } catch (e) {
      console.warn('[create-renewal] Could not associate deal to contract:', e.message);
    }

    res.json({
      success: true,
      message: `Renewal deal created: ${fmtDateForHS(renewalStart)} → ${fmtDateForHS(renewalEnd)}`,
      dealId: deal.id,
      dealName,
      warnings: warnings.length > 0 ? warnings : undefined,
    });
  } catch (e) {
    console.error('[create-renewal-deal] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Run Status Check ──────────────────────────────────────────────────

app.get('/api/run-status-check', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;

    if (props.status === 'terminated') {
      return res.json({ success: true, message: 'Contract is terminated — no status change', status: 'terminated' });
    }

    const newStatus = determineStatus(props.start_date, props.end_date);
    const updates = {};

    if (newStatus !== props.status) {
      updates.status = newStatus;
      if (newStatus === 'active' && !props.activated_date) {
        updates.activated_date = fmtDateForHS(new Date());
      }
    }

    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      for (const sid of subIds) {
        try {
          const sub = await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS);
          const sp = sub.properties;
          if (sp.status === 'terminated') continue;
          const subStatus = determineStatus(sp.start_date, sp.end_date);
          if (subStatus !== sp.status) {
            await updateObject(subscriptionTypeId, sid, { status: subStatus });
          }
        } catch (e) { /* skip */ }
      }
    }

    if (Object.keys(updates).length > 0) {
      await updateObject(contractTypeId, contractId, updates);
    }

    const subs = [];
    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      for (const sid of subIds) {
        try { subs.push(await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS)); } catch (e) { /* skip */ }
      }
    }
    const metrics = calcMetrics(subs);
    await updateObject(contractTypeId, contractId, {
      total_arr: String(metrics.total_arr),
      total_tcv: String(metrics.total_tcv),
      lq_arr: String(metrics.lq_arr),
      fcm_arr: String(metrics.fcm_arr),
      subscription_count: String(metrics.subscription_count),
    });

    res.json({
      success: true,
      message: newStatus !== props.status
        ? `Status updated: ${props.status} → ${newStatus}`
        : `Status confirmed: ${newStatus}. Metrics recalculated.`,
      status: newStatus || props.status,
    });
  } catch (e) {
    console.error('[run-status-check] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Load Account Rollups ──────────────────────────────────────────────

app.get('/api/load-account-rollups', async (req, res) => {
  try {
    const { companyId } = req.query;
    if (!companyId) return res.status(400).json({ success: false, message: 'companyId required' });

    await resolveTypeIds();
    if (!contractTypeId) return res.status(500).json({ success: false, message: 'Contract schema not found' });

    const contractIds = await getAssociatedIds('0-2', companyId, contractTypeId);

    let totalArr = 0;
    let activeContracts = 0;
    let totalContracts = contractIds.length;
    const contractSummaries = [];

    for (const cid of contractIds) {
      try {
        const c = await getObject(contractTypeId, cid, CONTRACT_PROPS);
        const cp = c.properties;
        contractSummaries.push({
          id: c.id,
          name: cp.contract_name,
          status: cp.status,
          arr: parseFloat(cp.total_arr) || 0,
          startDate: cp.start_date,
          endDate: cp.end_date,
        });
        if (cp.status === 'active') {
          activeContracts++;
          totalArr += parseFloat(cp.total_arr) || 0;
        }
      } catch (e) { /* skip inaccessible */ }
    }

    res.json({
      success: true,
      companyId,
      totalArr,
      activeContracts,
      totalContracts,
      contracts: contractSummaries,
    });
  } catch (e) {
    console.error('[load-account-rollups] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Load Deal CPQ ─────────────────────────────────────────────────────

app.get('/api/load-deal-cpq', async (req, res) => {
  try {
    const { dealId } = req.query;
    if (!dealId) return res.status(400).json({ success: false, message: 'dealId required' });

    await resolveTypeIds();

    const deal = await getObject('0-3', dealId, [
      'dealname', 'dealstage', 'amount', 'closedate', 'deal_category',
      'contract_start_date', 'contract_end_date', 'pipeline', 'hubspot_owner_id',
    ]);

    let sourceContract = null;
    let subscriptions = [];
    let contractId = null;

    if (contractTypeId) {
      const contractIds = await getAssociatedIds('0-3', dealId, contractTypeId);
      if (contractIds.length > 0) {
        contractId = contractIds[0];
        try {
          sourceContract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
        } catch (e) { /* non-critical */ }

        if (sourceContract && subscriptionTypeId) {
          const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
          if (subIds.length > 0) {
            const fetches = subIds.map((id) => getObject(subscriptionTypeId, id, SUBSCRIPTION_PROPS));
            subscriptions = await Promise.all(fetches);
          }
        }
      }
    }

    const companyIds = await getAssociatedIds('0-3', dealId, '0-2');
    let company = null;
    if (companyIds.length > 0) {
      try {
        company = await getObject('0-2', companyIds[0], ['name', 'domain', 'city', 'state', 'country']);
      } catch (e) { /* non-critical */ }
    }

    const contactIds = await getAssociatedIds('0-3', dealId, '0-1');
    const contacts = [];
    for (const cid of contactIds) {
      try {
        const c = await getObject('0-1', cid, ['firstname', 'lastname', 'email', 'jobtitle']);
        contacts.push({
          id: c.id,
          name: [c.properties.firstname, c.properties.lastname].filter(Boolean).join(' '),
          email: c.properties.email,
          title: c.properties.jobtitle,
        });
      } catch (e) { /* skip */ }
    }

    let portalId = null;
    try {
      const { data: acct } = await hs.get('/account-info/v3/details');
      portalId = acct.portalId;
    } catch (e) { /* non-critical */ }

    const isClosedWon = deal.properties.dealstage === 'closedwon';
    const category = deal.properties.deal_category || 'new_business';

    res.json({
      success: true,
      deal: {
        id: deal.id,
        name: deal.properties.dealname,
        stage: deal.properties.dealstage,
        amount: deal.properties.amount,
        closeDate: deal.properties.closedate,
        category,
        contractStartDate: deal.properties.contract_start_date,
        contractEndDate: deal.properties.contract_end_date,
        pipeline: deal.properties.pipeline,
        ownerId: deal.properties.hubspot_owner_id,
      },
      sourceContract,
      contractId,
      subscriptions,
      company,
      contacts,
      portalId,
      productRegistry: PRODUCT_REGISTRY,
      isClosedWon,
    });
  } catch (e) {
    console.error('[load-deal-cpq] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Update Contract from Deal (webhook) ──────────────────────────────

app.post('/api/update-contract-from-deal', async (req, res) => {
  try {
    const dealId = req.body?.dealId || req.query?.dealId;
    if (!dealId) return res.status(400).json({ success: false, message: 'dealId required' });

    await resolveTypeIds();
    if (!contractTypeId) return res.status(500).json({ success: false, message: 'Contract schema not found' });

    const deal = await getObject('0-3', dealId, [
      'dealname', 'dealstage', 'amount', 'closedate', 'deal_category',
      'contract_start_date', 'contract_end_date',
    ]);

    const dp = deal.properties;
    if (dp.dealstage !== 'closedwon') {
      return res.json({ success: true, message: 'Deal not closed-won — no action taken' });
    }

    const contractIds = await getAssociatedIds('0-3', dealId, contractTypeId);

    if (contractIds.length === 0 && dp.deal_category === 'new_business') {
      const companyIds = await getAssociatedIds('0-3', dealId, '0-2');
      const startDate = dp.contract_start_date || fmtDateForHS(new Date());
      let endDate = dp.contract_end_date;
      if (!endDate) {
        const ed = new Date(startDate);
        ed.setFullYear(ed.getFullYear() + 1);
        endDate = fmtDateForHS(ed);
      }

      const contract = await createObject(contractTypeId, {
        contract_name: dp.dealname.replace(' — New Business', '').replace(' - New Business', ''),
        status: determineStatus(startDate, endDate),
        start_date: startDate,
        end_date: endDate,
        co_term_date: endDate,
        total_arr: dp.amount || '0',
        total_tcv: dp.amount || '0',
        subscription_count: '0',
        amendment_count: '0',
      });

      try { await createAssociation(contractTypeId, contract.id, '0-3', dealId); } catch (e) { /* ok */ }
      if (companyIds.length > 0) {
        try { await createAssociation(contractTypeId, contract.id, '0-2', companyIds[0]); } catch (e) { /* ok */ }
      }

      return res.json({ success: true, message: 'New contract created from deal', contractId: contract.id });
    }

    if (contractIds.length > 0) {
      const cid = contractIds[0];
      const updates = {};

      if (dp.deal_category === 'renewal') {
        if (dp.contract_start_date) updates.start_date = dp.contract_start_date;
        if (dp.contract_end_date) {
          updates.end_date = dp.contract_end_date;
          updates.co_term_date = dp.contract_end_date;
        }
      }

      const contract = await getObject(contractTypeId, cid, CONTRACT_PROPS);
      const newStatus = determineStatus(
        updates.start_date || contract.properties.start_date,
        updates.end_date || contract.properties.end_date
      );
      updates.status = newStatus;

      if (Object.keys(updates).length > 0) {
        await updateObject(contractTypeId, cid, updates);
      }

      return res.json({ success: true, message: `Contract ${cid} updated from deal`, contractId: cid });
    }

    res.json({ success: true, message: 'No contract associated and not new business — no action' });
  } catch (e) {
    console.error('[update-contract-from-deal] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Seed Subscription Segments ─────────────────────────────────────────

app.get('/api/seed-subscriptions', async (req, res) => {
  try {
    await resolveTypeIds();
    if (!contractTypeId || !subscriptionTypeId) {
      return res.status(500).json({ success: false, message: 'Schemas not ready — hit /api/ensure-setup first' });
    }

    const { data: searchResult } = await hs.post(`/crm/v3/objects/${contractTypeId}/search`, {
      filterGroups: [],
      properties: ['contract_name', 'contract_number', 'status', 'start_date', 'end_date', 'total_arr'],
      limit: 20,
    });

    const contracts = searchResult.results || [];
    if (contracts.length === 0) {
      return res.json({ success: false, message: 'No contracts found in portal' });
    }

    const results = [];

    for (const contract of contracts) {
      const cp = contract.properties;
      const cId = contract.id;

      const existingSubIds = await getAssociatedIds(contractTypeId, cId, subscriptionTypeId);
      if (existingSubIds.length > 0) {
        results.push({ contractId: cId, name: cp.contract_name, skipped: true, reason: `Already has ${existingSubIds.length} subscriptions` });
        continue;
      }

      const startDate = cp.start_date || '2025-01-01';
      const startYear = new Date(startDate).getFullYear();
      const startMonth = new Date(startDate).getMonth();
      const startDay = new Date(startDate).getDate();

      function segDate(yearOffset, month, day) {
        const y = startYear + yearOffset;
        const m = month !== undefined ? month : startMonth;
        const d = day !== undefined ? day : startDay;
        return `${y}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
      }

      function segEndDate(yearOffset, month, day) {
        const y = startYear + yearOffset;
        const m = month !== undefined ? month : startMonth;
        const d = day !== undefined ? day : startDay;
        const dt = new Date(y, m, d);
        dt.setDate(dt.getDate() - 1);
        return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')}`;
      }

      const contractName = cp.contract_name || 'Contract';

      const subs = [
        {
          segment_name: `${contractName} — LQ Year 1`,
          subscription_number: `SUB-${cId}-LQ-01`,
          product_code: 'LQ',
          product_name: 'LeaseQuery',
          product_subscription_type: 'renewable',
          subscription_type: 'renewable',
          charge_type: 'recurring',
          billing_frequency: 'annual',
          status: 'inactive',
          segment_year: '1',
          segment_label: 'Year 1',
          segment_index: '1',
          start_date: segDate(0),
          end_date: segEndDate(1),
          segment_start_date: segDate(0),
          segment_end_date: segEndDate(1),
          arr_start_date: segDate(0),
          arr_end_date: segEndDate(1),
          subscription_start_date: segDate(0),
          subscription_end_date: cp.end_date || segEndDate(3),
          quantity: '200',
          unit_price: '500',
          list_price: '600',
          net_price: '100000',
          regular_price: '500',
          customer_price: '500',
          discount_percent: '16.67',
          prorate_multiplier: '1',
          pricing_method: 'list',
          arr: '100000',
          mrr: '8333.33',
          tcv: '100000',
        },
        {
          segment_name: `${contractName} — LQ Year 2`,
          subscription_number: `SUB-${cId}-LQ-02`,
          product_code: 'LQ',
          product_name: 'LeaseQuery',
          product_subscription_type: 'renewable',
          subscription_type: 'renewable',
          charge_type: 'recurring',
          billing_frequency: 'annual',
          status: 'active',
          segment_year: '2',
          segment_label: 'Year 2',
          segment_index: '2',
          segment_uplift: '3',
          start_date: segDate(1),
          end_date: segEndDate(2),
          segment_start_date: segDate(1),
          segment_end_date: segEndDate(2),
          arr_start_date: segDate(1),
          arr_end_date: segEndDate(2),
          subscription_start_date: segDate(0),
          subscription_end_date: cp.end_date || segEndDate(3),
          quantity: '200',
          unit_price: '515',
          list_price: '600',
          net_price: '103000',
          regular_price: '515',
          customer_price: '515',
          discount_percent: '14.17',
          prorate_multiplier: '1',
          pricing_method: 'list',
          arr: '103000',
          mrr: '8583.33',
          tcv: '103000',
        },
        {
          segment_name: `${contractName} — LQ Year 3`,
          subscription_number: `SUB-${cId}-LQ-03`,
          product_code: 'LQ',
          product_name: 'LeaseQuery',
          product_subscription_type: 'renewable',
          subscription_type: 'renewable',
          charge_type: 'recurring',
          billing_frequency: 'annual',
          status: 'future',
          segment_year: '3',
          segment_label: 'Year 3',
          segment_index: '3',
          segment_uplift: '3',
          start_date: segDate(2),
          end_date: segEndDate(3),
          segment_start_date: segDate(2),
          segment_end_date: segEndDate(3),
          arr_start_date: segDate(2),
          arr_end_date: segEndDate(3),
          subscription_start_date: segDate(0),
          subscription_end_date: cp.end_date || segEndDate(3),
          quantity: '200',
          unit_price: '530.45',
          list_price: '600',
          net_price: '106090',
          regular_price: '530.45',
          customer_price: '530.45',
          discount_percent: '11.59',
          prorate_multiplier: '1',
          pricing_method: 'list',
          arr: '106090',
          mrr: '8840.83',
          tcv: '106090',
        },
        {
          segment_name: `${contractName} — FCM Year 1`,
          subscription_number: `SUB-${cId}-FCM-01`,
          product_code: 'FCM',
          product_name: 'Financial Close Management',
          product_subscription_type: 'renewable',
          subscription_type: 'renewable',
          charge_type: 'recurring',
          billing_frequency: 'annual',
          status: 'active',
          amendment_indicator: 'Expansion',
          segment_year: '1',
          segment_label: 'Year 1',
          segment_index: '1',
          start_date: segDate(0, startMonth + 6 > 11 ? startMonth + 6 - 12 : startMonth + 6, 1),
          end_date: segEndDate(2),
          segment_start_date: segDate(0, startMonth + 6 > 11 ? startMonth + 6 - 12 : startMonth + 6, 1),
          segment_end_date: segEndDate(2),
          arr_start_date: segDate(0, startMonth + 6 > 11 ? startMonth + 6 - 12 : startMonth + 6, 1),
          arr_end_date: segEndDate(2),
          subscription_start_date: segDate(0, startMonth + 6 > 11 ? startMonth + 6 - 12 : startMonth + 6, 1),
          subscription_end_date: cp.end_date || segEndDate(3),
          quantity: '1',
          unit_price: '55000',
          list_price: '65000',
          net_price: '55000',
          regular_price: '55000',
          customer_price: '55000',
          discount_percent: '15.38',
          prorate_multiplier: '1',
          pricing_method: 'list',
          arr: '55000',
          mrr: '4583.33',
          tcv: '55000',
        },
        {
          segment_name: `${contractName} — FCM Year 2`,
          subscription_number: `SUB-${cId}-FCM-02`,
          product_code: 'FCM',
          product_name: 'Financial Close Management',
          product_subscription_type: 'renewable',
          subscription_type: 'renewable',
          charge_type: 'recurring',
          billing_frequency: 'annual',
          status: 'future',
          amendment_indicator: 'Expansion',
          segment_year: '2',
          segment_label: 'Year 2',
          segment_index: '2',
          segment_uplift: '3',
          start_date: segDate(2),
          end_date: segEndDate(3),
          segment_start_date: segDate(2),
          segment_end_date: segEndDate(3),
          arr_start_date: segDate(2),
          arr_end_date: segEndDate(3),
          subscription_start_date: segDate(0, startMonth + 6 > 11 ? startMonth + 6 - 12 : startMonth + 6, 1),
          subscription_end_date: cp.end_date || segEndDate(3),
          quantity: '1',
          unit_price: '56650',
          list_price: '65000',
          net_price: '56650',
          regular_price: '56650',
          customer_price: '56650',
          discount_percent: '12.85',
          prorate_multiplier: '1',
          pricing_method: 'list',
          arr: '56650',
          mrr: '4720.83',
          tcv: '56650',
        },
      ];

      const createdIds = [];
      for (const sub of subs) {
        const record = await createObject(subscriptionTypeId, sub);
        createdIds.push(record.id);
        try { await createAssociation(contractTypeId, cId, subscriptionTypeId, record.id); } catch (e) { /* ok */ }
        const companyIds = await getAssociatedIds(contractTypeId, cId, '0-2');
        if (companyIds.length > 0) {
          try { await createAssociation(subscriptionTypeId, record.id, '0-2', companyIds[0]); } catch (e) { /* ok */ }
        }
      }

      await updateObject(contractTypeId, cId, {
        total_arr: '158000',
        lq_arr: '103000',
        fcm_arr: '55000',
        total_tcv: '523740',
        subscription_count: String(subs.length),
      });

      results.push({ contractId: cId, name: cp.contract_name, created: createdIds.length, subIds: createdIds });
    }

    res.json({ success: true, message: `Processed ${contracts.length} contracts`, results });
  } catch (e) {
    console.error('[seed-subscriptions] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.response?.data?.message || e.message });
  }
});

// ── Startup ──────────────────────────────────────────────────────────────────

app.listen(PORT, async () => {
  console.log(`FinQuery Contracts API running on port ${PORT}`);
  try {
    await resolveTypeIds();
    console.log(`  Contract type: ${contractTypeId || 'not found (will create on first setup)'}`);
    console.log(`  Subscription type: ${subscriptionTypeId || 'not found (will create on first setup)'}`);
  } catch (e) {
    console.warn('Could not resolve type IDs on startup:', e.message);
  }
});
