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

    // ── Flags ─────────────────────────────────────────────────────────────
    {
      name: 'has_legacy_products', label: 'Has Legacy Products', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },

    // ── Special terms / notes ────────────────────────────────────────────
    { name: 'special_terms', label: 'Special Terms', type: 'string', fieldType: 'textarea' },

    // ── Auto-Renewal ─────────────────────────────────────────────────────
    {
      name: 'auto_renewal_enabled', label: 'Auto-Renewal Enabled', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },
    { name: 'auto_renewal_date', label: 'Auto-Renewal Date', type: 'date', fieldType: 'date' },
    {
      name: 'auto_renewal_released', label: 'Auto-Renewal Released', type: 'enumeration', fieldType: 'booleancheckbox',
      options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }],
    },

    // ── Contract lineage ────────────────────────────────────────────────
    { name: 'replaced_by_contract', label: 'Replaced By Contract ID', type: 'string', fieldType: 'text' },
    { name: 'replaces_contract', label: 'Replaces Contract ID', type: 'string', fieldType: 'text' },

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
        { label: 'Expired', value: 'expired' },
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
      name: 'revenue_type', label: 'Revenue Type', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'New', value: 'new' },
        { label: 'Renewal', value: 'renewal' },
        { label: 'Expansion', value: 'expansion' },
        { label: 'Contraction', value: 'contraction' },
        { label: 'Cross-Sell', value: 'cross_sell' },
      ],
    },
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
      { label: 'Amendment', value: 'amendment' },
      { label: 'Expansion', value: 'expansion' },
      { label: 'Contraction', value: 'contraction' },
    ],
  },
  { name: 'contract_start_date', label: 'Contract Start Date', type: 'date', fieldType: 'date', groupName: 'dealinformation' },
  { name: 'contract_end_date', label: 'Contract End Date', type: 'date', fieldType: 'date', groupName: 'dealinformation' },
  {
    name: 'revenue_type', label: 'Revenue Type', type: 'enumeration', fieldType: 'select',
    groupName: 'dealinformation',
    options: [
      { label: 'New', value: 'new' },
      { label: 'Renewal', value: 'renewal' },
      { label: 'Expansion', value: 'expansion' },
      { label: 'Contraction', value: 'contraction' },
      { label: 'Cross-Sell', value: 'cross_sell' },
    ],
  },
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
  'has_legacy_products',
  'replaced_by_contract', 'replaces_contract',
  'special_terms', 'netsuite_id', 'contract_data',
  'auto_renewal_enabled', 'auto_renewal_date', 'auto_renewal_released',
];

const SUBSCRIPTION_PROPS = [
  'segment_name', 'sf_subscription_id', 'subscription_number',
  'product_code', 'product_name', 'product_subscription_type',
  'subscription_type', 'charge_type', 'billing_frequency',
  'status', 'proration_status', 'amendment_indicator', 'revenue_type', 'bundled',
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

async function createObject(typeId, properties, associations) {
  const body = { properties };
  if (associations) body.associations = associations;
  const { data } = await hs.post(`/crm/v3/objects/${typeId}`, body);
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
  if (end && end < now) return 'expired';
  if (start && start <= now && (!end || end >= now)) return 'active';
  return 'expired';
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

function mapBillingFrequencyToRecurringPeriod(subscription) {
  const sp = subscription.properties || {};
  const billingFrequency = (sp.billing_frequency || '').toLowerCase();
  if (billingFrequency === 'monthly') return 'P1M';
  if (billingFrequency === 'quarterly') return 'P3M';
  if (billingFrequency === 'semiannual') return 'P6M';
  if (billingFrequency === 'annual') return 'P12M';
  return 'P12M';
}

function normalizeLineRevenueType(rawRevenueType, fallback = 'renewal') {
  const value = (rawRevenueType || '').toLowerCase();
  if (['new', 'renewal', 'expansion', 'contraction', 'cross_sell'].includes(value)) {
    return value;
  }
  return fallback;
}

const INHERITED_LINE_MARKER = 'FQ_INHERITED_SOURCE_LINE:';
const INHERITED_PRODUCT_MARKER = 'FQ_INHERITED_PRODUCT_KEY:';

function parseInheritedSourceLineId(description) {
  const text = String(description || '');
  const match = text.match(/FQ_INHERITED_SOURCE_LINE:(\d+)/i);
  if (!match) return null;
  return match[1];
}

function parseInheritedProductKey(description) {
  const text = String(description || '');
  const match = text.match(/FQ_INHERITED_PRODUCT_KEY:([a-z0-9-]+)/i);
  if (!match) return null;
  return String(match[1]).toLowerCase();
}

function sanitizeProductKey(raw, fallback = 'product') {
  return String(raw || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || fallback;
}

function buildInheritedProductDescription(baseDescription, productKey) {
  const base = String(baseDescription || '')
    .replace(/\s*\|\s*FQ_INHERITED_SOURCE_LINE:\d+\s*$/i, '')
    .replace(/\s*\|\s*FQ_INHERITED_PRODUCT_KEY:[a-z0-9-]+\s*$/i, '')
    .trim();
  return `${base ? `${base} | ` : ''}${INHERITED_PRODUCT_MARKER}${productKey}`;
}

function normalizeRecurringPeriod(periodRaw) {
  const period = String(periodRaw || '').trim();
  if (!period) return 'P12M';
  const lower = period.toLowerCase();
  if (lower === 'one_time' || lower === 'onetime') return null;
  if (lower === 'annual') return 'P12M';
  return period;
}

function stripUnsupportedLineItemProps(err, properties) {
  const message = JSON.stringify(err?.response?.data || err?.message || '').toLowerCase();
  const nextProps = { ...properties };
  let changed = false;

  if (message.includes('revenue_type') && Object.prototype.hasOwnProperty.call(nextProps, 'revenue_type')) {
    delete nextProps.revenue_type;
    changed = true;
  }

  if (
    message.includes('hs_recurring_billing_start_date') &&
    Object.prototype.hasOwnProperty.call(nextProps, 'hs_recurring_billing_start_date')
  ) {
    delete nextProps.hs_recurring_billing_start_date;
    changed = true;
  }

  return changed ? nextProps : null;
}

async function createDealLineItemWithFallback(dealId, properties) {
  try {
    return await createObject('line_items', properties, [{
      to: { id: dealId },
      types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 20 }],
    }]);
  } catch (e) {
    const fallbackProps = stripUnsupportedLineItemProps(e, properties);
    if (!fallbackProps) throw e;
    return createObject('line_items', fallbackProps, [{
      to: { id: dealId },
      types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 20 }],
    }]);
  }
}

async function updateLineItemWithFallback(lineItemId, properties) {
  try {
    return await updateObject('line_items', lineItemId, properties);
  } catch (e) {
    const fallbackProps = stripUnsupportedLineItemProps(e, properties);
    if (!fallbackProps) throw e;
    return updateObject('line_items', lineItemId, fallbackProps);
  }
}

async function syncContractRecurringLineItemsToDeal(contractId, dealId, options = {}) {
  const { fallbackRevenueType = 'renewal' } = options;

  const sourceLineItemIds = await getAssociatedIds(contractTypeId, contractId, 'line_items');
  const sourceItems = sourceLineItemIds.length === 0
    ? []
    : await Promise.all(
      sourceLineItemIds.map((id) =>
        getObject('line_items', id, [
          'name', 'quantity', 'price', 'hs_sku', 'description',
          'hs_recurring_billing_period', 'revenue_type',
        ])
      )
    );

  const recurringSourceItems = sourceItems.filter((item) => {
    const period = normalizeRecurringPeriod(item?.properties?.hs_recurring_billing_period);
    return !!period;
  });

  const groupedByProduct = {};
  recurringSourceItems.forEach((item, index) => {
    const props = item.properties || {};
    const sku = String(props.hs_sku || '').trim();
    const name = String(props.name || 'Product').trim() || 'Product';
    const key = sanitizeProductKey(sku || name, `product-${index + 1}`);
    const qty = Math.max(0, parseFloat(props.quantity) || 0);
    const quantity = qty > 0 ? qty : 1;
    const unitPrice = Math.max(0, parseFloat(props.price) || 0);
    const lineAmount = quantity * unitPrice;
    const period = normalizeRecurringPeriod(props.hs_recurring_billing_period) || 'P12M';

    if (!groupedByProduct[key]) {
      groupedByProduct[key] = {
        key,
        name,
        sku,
        quantity: 0,
        amount: 0,
        period,
        hasExpansion: false,
      };
    }

    groupedByProduct[key].quantity += quantity;
    groupedByProduct[key].amount += lineAmount;
    if (!groupedByProduct[key].sku && sku) groupedByProduct[key].sku = sku;
    if (period) groupedByProduct[key].period = period;

    const revenueType = normalizeLineRevenueType(props.revenue_type, fallbackRevenueType);
    if (revenueType === 'expansion' || revenueType === 'cross_sell' || revenueType === 'new') {
      groupedByProduct[key].hasExpansion = true;
    }
  });

  const groupedItems = Object.values(groupedByProduct);

  const dealLineItemIds = await getAssociatedIds('0-3', dealId, 'line_items');
  const dealItems = dealLineItemIds.length === 0
    ? []
    : await Promise.all(
      dealLineItemIds.map((id) =>
        getObject('line_items', id, ['description'])
      )
    );

  const existingByManagedKey = {};
  for (const item of dealItems) {
    const desc = item?.properties?.description;
    const productKey = parseInheritedProductKey(desc);
    const sourceLineId = parseInheritedSourceLineId(desc);
    const key = productKey || (sourceLineId ? `legacy-source-${sourceLineId}` : null);
    if (!key) continue;
    existingByManagedKey[key] = item;
  }

  let created = 0;
  let updated = 0;
  let removed = 0;
  const warnings = [];

  const incomingKeySet = new Set();

  groupedItems.forEach((item) => incomingKeySet.add(item.key));

  const toPriceString = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return '0';
    return (Math.round(n * 100) / 100).toFixed(2);
  };

  for (const item of groupedItems) {
    const quantity = Math.max(1, Math.round(item.quantity || 0));
    const unitPrice = quantity > 0 ? (item.amount / quantity) : item.amount;
    const lineProps = {
      name: item.name || 'Product',
      quantity: String(quantity),
      price: toPriceString(unitPrice),
      hs_sku: item.sku || '',
      description: buildInheritedProductDescription(item.name, item.key),
      hs_recurring_billing_period: item.period || 'P12M',
      revenue_type: item.hasExpansion ? 'expansion' : normalizeLineRevenueType('', fallbackRevenueType),
    };

    try {
      const existing = existingByManagedKey[item.key];
      if (existing?.id) {
        await updateLineItemWithFallback(existing.id, lineProps);
        updated++;
      } else {
        await createDealLineItemWithFallback(dealId, lineProps);
        created++;
      }
    } catch (e) {
      const lineName = item.name || item.key;
      warnings.push(`Failed syncing ${lineName}`);
      console.warn('[sync-contract-lines] Failed line sync:', lineName, e.response?.data?.message || e.message);
    }
  }

  for (const [key, existing] of Object.entries(existingByManagedKey)) {
    if (incomingKeySet.has(key)) continue;
    try {
      await hs.delete(`/crm/v3/objects/line_items/${existing.id}`);
      removed++;
    } catch (e) {
      warnings.push(`Failed deleting stale inherited line ${existing.id}`);
      console.warn('[sync-contract-lines] Failed deleting stale line item:', existing.id, e.response?.data?.message || e.message);
    }
  }

  return {
    sourceRecurringCount: recurringSourceItems.length,
    lineItemsCreated: created,
    lineItemsUpdated: updated,
    lineItemsRemoved: removed,
    warnings,
  };
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

  // Ensure auto-renewal properties exist on contract schema (for existing deployments)
  if (contractTypeId) {
    const autoRenewalProps = [
      { name: 'auto_renewal_enabled', label: 'Auto-Renewal Enabled', type: 'enumeration', fieldType: 'booleancheckbox',
        options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }] },
      { name: 'auto_renewal_date', label: 'Auto-Renewal Date', type: 'date', fieldType: 'date' },
      { name: 'auto_renewal_released', label: 'Auto-Renewal Released', type: 'enumeration', fieldType: 'booleancheckbox',
        options: [{ label: 'Yes', value: 'true' }, { label: 'No', value: 'false' }] },
    ];
    for (const prop of autoRenewalProps) {
      try {
        await hs.post(`/crm/v3/properties/${contractTypeId}`, prop);
        console.log(`[ensureSetup] Created auto-renewal property: ${prop.name}`);
      } catch (e) {
        if (e.response?.status === 409 || e.response?.status === 400) continue;
      }
    }
  }

  console.log(`[ensureSetup] Ready — contract=${contractTypeId}, subscription=${subscriptionTypeId}`);
  return { contractTypeId, subscriptionTypeId, productRegistry: PRODUCT_REGISTRY };
}

// ── Route: Health ────────────────────────────────────────────────────────────

const API_VERSION = '2026-04-08a';

app.get('/', (req, res) => res.json({ status: 'ok', service: 'finquery-contracts-api', version: API_VERSION }));

app.get('/api/health', (req, res) => res.json({ status: 'ok', version: API_VERSION, contractTypeId, subscriptionTypeId }));

// ── Route: Test Line Item Creation ───────────────────────────────────────────

app.get('/api/test-line-item', async (req, res) => {
  try {
    const { dealId, contractId } = req.query;
    if (!dealId && !contractId) {
      return res.status(400).json({ success: false, message: 'dealId or contractId required' });
    }

    await resolveTypeIds();

    if (contractId && subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      const subs = [];
      for (const sid of subIds) {
        try {
          const sub = await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS);
          subs.push({
            id: sub.id,
            name: sub.properties?.segment_name,
            product: sub.properties?.product_name,
            productCode: sub.properties?.product_code,
            status: sub.properties?.status,
            quantity: sub.properties?.quantity,
            unitPrice: sub.properties?.unit_price,
            arr: sub.properties?.arr,
          });
        } catch (e) {
          subs.push({ id: sid, error: e.message });
        }
      }
      const activeSubs = subs.filter((s) => s.status === 'active');
      return res.json({
        success: true,
        contractId,
        totalSegments: subs.length,
        activeSegments: activeSubs.length,
        segments: subs,
        note: dealId
          ? 'Pass dealId to also test creating a line item on that deal'
          : 'Add &dealId=X to test creating a line item on a specific deal',
      });
    }

    if (dealId) {
      try {
        const lineItem = await createObject('line_items', {
          name: 'TEST — Delete Me',
          quantity: '1',
          price: '0',
        }, [{
          to: { id: dealId },
          types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 20 }],
        }]);
        return res.json({ success: true, message: 'Test line item created', lineItemId: lineItem.id, dealId });
      } catch (e) {
        return res.json({
          success: false,
          message: 'Line item creation failed',
          error: e.response?.data || e.message,
        });
      }
    }

    res.json({ success: false, message: 'No action taken' });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message, detail: e.response?.data });
  }
});

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
    const { contractId, startDate } = req.query;
    if (!contractId) {
      return res.status(400).json({ success: false, message: 'contractId required' });
    }

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;
    const dealName = `${props.contract_name || 'Contract'} — Amendment`;

    const dealProps = {
      dealname: dealName,
      dealstage: 'appointmentscheduled',
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
        console.warn('[start-amendment] Company association failed:', e.response?.data?.message || e.message);
        warnings.push('Company association failed — some contacts on this company may be invalid');
      }
    }

    try {
      await createAssociation(contractTypeId, contractId, '0-3', deal.id);
    } catch (e) {
      console.warn('[start-amendment] Could not associate deal to contract:', e.message);
    }

    const contactIds = await getAssociatedIds(contractTypeId, contractId, '0-1');
    let contactsLinked = 0;
    for (const cid of contactIds) {
      try {
        await createAssociation('0-3', deal.id, '0-1', cid);
        contactsLinked++;
      } catch (e) {
        console.warn(`[start-amendment] Skipping invalid contact ${cid}:`, e.response?.data?.message || e.message);
      }
    }
    if (contactIds.length > 0 && contactsLinked < contactIds.length) {
      warnings.push(`${contactIds.length - contactsLinked} of ${contactIds.length} contact associations failed (likely deleted contacts)`);
    }

    // Always seed from current contract recurring line items for contract-origin deals.
    const seeded = await syncContractRecurringLineItemsToDeal(contractId, deal.id, {
      fallbackRevenueType: 'renewal',
    });
    const lineItemsInherited = seeded.lineItemsCreated + seeded.lineItemsUpdated;
    if (seeded.warnings?.length) warnings.push(...seeded.warnings);
    console.log(
      `[start-amendment] Synced recurring lines from contract ${contractId}: ` +
      `${seeded.lineItemsCreated} created, ${seeded.lineItemsUpdated} updated, ${seeded.lineItemsRemoved} removed ` +
      `(source recurring: ${seeded.sourceRecurringCount})`
    );

    const amendCount = (parseInt(props.amendment_count) || 0) + 1;
    await updateObject(contractTypeId, contractId, {
      amendment_count: String(amendCount),
    });

    res.json({
      success: true,
      message: `Amendment deal created${lineItemsInherited > 0 ? ` with ${lineItemsInherited} inherited line items` : ''}`,
      dealId: deal.id,
      dealName,
      contactsLinked,
      lineItemsInherited,
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
        console.warn('[create-renewal] Company association failed:', e.response?.data?.message || e.message);
        warnings.push('Company association failed — some contacts on this company may be invalid');
      }
    }

    try {
      await createAssociation(contractTypeId, contractId, '0-3', deal.id);
    } catch (e) {
      console.warn('[create-renewal] Could not associate deal to contract:', e.message);
    }

    const contactIds = await getAssociatedIds(contractTypeId, contractId, '0-1');
    let contactsLinked = 0;
    for (const cid of contactIds) {
      try {
        await createAssociation('0-3', deal.id, '0-1', cid);
        contactsLinked++;
      } catch (e) {
        console.warn(`[create-renewal] Skipping invalid contact ${cid}:`, e.response?.data?.message || e.message);
      }
    }
    if (contactIds.length > 0 && contactsLinked < contactIds.length) {
      warnings.push(`${contactIds.length - contactsLinked} of ${contactIds.length} contact associations failed (likely deleted contacts)`);
    }

    // Always seed from current contract recurring line items for contract-origin deals.
    const seeded = await syncContractRecurringLineItemsToDeal(contractId, deal.id, {
      fallbackRevenueType: 'renewal',
    });
    const lineItemsCreated = seeded.lineItemsCreated + seeded.lineItemsUpdated;
    if (seeded.warnings?.length) warnings.push(...seeded.warnings);
    console.log(
      `[create-renewal] Synced recurring lines from contract ${contractId}: ` +
      `${seeded.lineItemsCreated} created, ${seeded.lineItemsUpdated} updated, ${seeded.lineItemsRemoved} removed ` +
      `(source recurring: ${seeded.sourceRecurringCount})`
    );

    res.json({
      success: true,
      message: `Renewal deal created: ${fmtDateForHS(renewalStart)} → ${fmtDateForHS(renewalEnd)}`,
      dealId: deal.id,
      dealName,
      contactsLinked,
      lineItemsCreated,
      warnings: warnings.length > 0 ? warnings : undefined,
    });
  } catch (e) {
    console.error('[create-renewal-deal] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Set Auto-Renewal ──────────────────────────────────────────────────

app.get('/api/set-auto-renewal', async (req, res) => {
  try {
    const { contractId, enabled, date } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();

    const updates = {};
    if (enabled !== undefined) {
      updates.auto_renewal_enabled = enabled === 'true' ? 'true' : 'false';
    }
    if (date) {
      updates.auto_renewal_date = date;
    }
    if (enabled === 'true') {
      updates.auto_renewal_released = 'false';
    }

    await updateObject(contractTypeId, contractId, updates);

    res.json({
      success: true,
      message: enabled === 'true'
        ? `Auto-renewal armed for ${date || 'contract end date'}`
        : 'Auto-renewal disabled',
    });
  } catch (e) {
    console.error('[set-auto-renewal] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Trigger Auto-Renewal ──────────────────────────────────────────────

app.get('/api/trigger-auto-renewal', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    await resolveTypeIds();
    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const props = contract.properties;

    if (props.auto_renewal_released === 'true') {
      return res.json({ success: true, message: 'Auto-renewal already released', alreadyReleased: true });
    }

    const currentEnd = props.end_date ? new Date(props.end_date) : new Date();
    const renewalStart = new Date(currentEnd);
    renewalStart.setDate(renewalStart.getDate() + 1);
    const renewalEnd = new Date(renewalStart);
    renewalEnd.setFullYear(renewalEnd.getFullYear() + 1);

    const dealName = `${props.contract_name || 'Contract'} — Auto-Renewal`;

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
      try { await createAssociation('0-3', deal.id, '0-2', companyIds[0]); }
      catch (e) { warnings.push('Company association failed'); }
    }

    try { await createAssociation(contractTypeId, contractId, '0-3', deal.id); }
    catch (e) { /* ok */ }

    const contactIds = await getAssociatedIds(contractTypeId, contractId, '0-1');
    let contactsLinked = 0;
    for (const cid of contactIds) {
      try { await createAssociation('0-3', deal.id, '0-1', cid); contactsLinked++; }
      catch (e) { /* skip invalid */ }
    }

    const seeded = await syncContractRecurringLineItemsToDeal(contractId, deal.id, {
      fallbackRevenueType: 'renewal',
    });
    const lineItemsCreated = seeded.lineItemsCreated + seeded.lineItemsUpdated;
    if (seeded.warnings?.length) warnings.push(...seeded.warnings);

    await updateObject(contractTypeId, contractId, {
      auto_renewal_released: 'true',
    });

    console.log(
      `[trigger-auto-renewal] Released for contract ${contractId}: deal ${deal.id}, ` +
      `${seeded.lineItemsCreated} created, ${seeded.lineItemsUpdated} updated, ${seeded.lineItemsRemoved} removed`
    );

    res.json({
      success: true,
      message: `Auto-renewal deal created: ${fmtDateForHS(renewalStart)} → ${fmtDateForHS(renewalEnd)}`,
      dealId: deal.id,
      dealName,
      contactsLinked,
      lineItemsCreated,
      warnings: warnings.length > 0 ? warnings : undefined,
    });
  } catch (e) {
    console.error('[trigger-auto-renewal] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Check Auto-Renewals (batch) ───────────────────────────────────────

app.get('/api/check-auto-renewals', async (req, res) => {
  try {
    await resolveTypeIds();
    if (!contractTypeId) return res.status(500).json({ success: false, message: 'Contract schema not found' });

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const { data } = await hs.post(`/crm/v3/objects/${contractTypeId}/search`, {
      filterGroups: [{
        filters: [
          { propertyName: 'auto_renewal_enabled', operator: 'EQ', value: 'true' },
          { propertyName: 'auto_renewal_date', operator: 'LTE', value: String(today.valueOf()) },
          { propertyName: 'status', operator: 'IN', values: ['active', 'future'] },
        ],
      }],
      properties: ['contract_name', 'auto_renewal_date', 'auto_renewal_released', 'status', 'end_date', 'total_arr'],
      limit: 100,
    });

    const candidates = (data.results || []).filter(
      (c) => c.properties.auto_renewal_released !== 'true'
    );

    const results = [];
    for (const c of candidates) {
      try {
        const cp = c.properties;
        const currentEnd = cp.end_date ? new Date(cp.end_date) : new Date();
        const renewalStart = new Date(currentEnd);
        renewalStart.setDate(renewalStart.getDate() + 1);
        const renewalEnd = new Date(renewalStart);
        renewalEnd.setFullYear(renewalEnd.getFullYear() + 1);

        const dealName = `${cp.contract_name || 'Contract'} — Auto-Renewal`;
        const deal = await createObject('0-3', {
          dealname: dealName,
          dealstage: 'appointmentscheduled',
          deal_category: 'renewal',
          contract_start_date: fmtDateForHS(renewalStart),
          contract_end_date: fmtDateForHS(renewalEnd),
          amount: cp.total_arr || '0',
          pipeline: 'default',
        });

        try { await createAssociation(contractTypeId, c.id, '0-3', deal.id); } catch (e) { /* ok */ }
        const companyIds = await getAssociatedIds(contractTypeId, c.id, '0-2');
        if (companyIds.length > 0) {
          try { await createAssociation('0-3', deal.id, '0-2', companyIds[0]); } catch (e) { /* ok */ }
        }

        const seeded = await syncContractRecurringLineItemsToDeal(c.id, deal.id, {
          fallbackRevenueType: 'renewal',
        });

        await updateObject(contractTypeId, c.id, { auto_renewal_released: 'true' });

        results.push({
          contractId: c.id,
          name: cp.contract_name,
          dealId: deal.id,
          dealName,
          lineItemsSynced: seeded.lineItemsCreated + seeded.lineItemsUpdated,
          status: 'released',
        });
        console.log(`[check-auto-renewals] Released: ${cp.contract_name} → deal ${deal.id}`);
      } catch (e) {
        results.push({ contractId: c.id, name: c.properties.contract_name, error: e.message });
      }
    }

    res.json({
      success: true,
      message: `Processed ${results.length} auto-renewal(s)`,
      processed: results.length,
      results,
    });
  } catch (e) {
    console.error('[check-auto-renewals] Error:', e.response?.data || e.message);
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

// ── Route: Load Company Contracts ────────────────────────────────────────────

app.get('/api/load-company-contracts', async (req, res) => {
  try {
    const { companyId } = req.query;
    if (!companyId) return res.status(400).json({ success: false, message: 'companyId required' });

    await resolveTypeIds();
    if (!contractTypeId) return res.status(500).json({ success: false, message: 'Contract schema not found' });

    let companyName = '';
    try {
      const company = await getObject('0-2', companyId, ['name']);
      companyName = company.properties?.name || '';
    } catch (e) { /* non-critical */ }

    const contractIds = await getAssociatedIds('0-2', companyId, contractTypeId);

    let totalArr = 0;
    let totalMrr = 0;
    let totalTcv = 0;
    let ltv = 0;
    let activeContracts = 0;
    let lqArr = 0;
    let fcmArr = 0;
    const contracts = [];

    for (const cid of contractIds) {
      try {
        const c = await getObject(contractTypeId, cid, CONTRACT_PROPS);
        const cp = c.properties;
        const arr = parseFloat(cp.total_arr) || 0;
        const tcv = parseFloat(cp.total_tcv) || 0;
        const mrr = arr / 12;

        ltv += tcv;

        if (cp.status === 'active') {
          activeContracts++;
          totalArr += arr;
          totalMrr += mrr;
          totalTcv += tcv;
          lqArr += parseFloat(cp.lq_arr) || 0;
          fcmArr += parseFloat(cp.fcm_arr) || 0;
        }

        const products = [];
        const lqA = parseFloat(cp.lq_arr) || 0;
        const fcmA = parseFloat(cp.fcm_arr) || 0;
        if (lqA > 0) products.push({ name: 'LeaseQuery', code: 'LQ', arr: lqA });
        if (fcmA > 0) products.push({ name: 'FCM', code: 'FCM', arr: fcmA });

        contracts.push({
          id: c.id,
          objectTypeId: contractTypeId,
          name: cp.contract_name,
          number: cp.contract_number,
          status: cp.status,
          arr,
          mrr,
          tcv,
          startDate: cp.start_date,
          endDate: cp.end_date,
          coTermDate: cp.co_term_date,
          term: parseInt(cp.contract_term) || null,
          renewalTerm: parseInt(cp.renewal_term) || null,
          evergreen: cp.evergreen,
          renewalUplift: parseFloat(cp.renewal_uplift_rate) || 0,
          subscriptionCount: parseInt(cp.subscription_count) || 0,
          amendmentCount: parseInt(cp.amendment_count) || 0,
          activatedDate: cp.activated_date,
          terminatedDate: cp.terminated_date,
          terminationReason: cp.termination_reason,
          products,
        });
      } catch (e) {
        console.warn(`[load-company-contracts] Skipping contract ${cid}:`, e.message);
      }
    }

    let portalId = null;
    try {
      const { data: acct } = await hs.get('/account-info/v3/details');
      portalId = acct.portalId;
    } catch (e) { /* non-critical */ }

    res.json({
      success: true,
      companyId,
      companyName,
      totalArr,
      totalMrr,
      totalTcv,
      ltv,
      lqArr,
      fcmArr,
      activeContracts,
      totalContracts: contracts.length,
      contracts,
      portalId,
    });
  } catch (e) {
    console.error('[load-company-contracts] Error:', e.response?.data || e.message);
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

// ── Route: Upsert Demo Deal Line Items by Product ────────────────────────────

app.post('/api/upsert-demo-line-items', async (req, res) => {
  try {
    const dealId = req.body?.dealId || req.query?.dealId;
    const productItems = Array.isArray(req.body?.productItems) ? req.body.productItems : [];
    const yearItems = Array.isArray(req.body?.yearItems) ? req.body.yearItems : [];

    if (!dealId) {
      return res.status(400).json({ success: false, message: 'dealId required' });
    }

    const normalizeRevenueType = (raw) => {
      const value = String(raw || '').toLowerCase();
      return value === 'expansion' ? 'expansion' : 'renewal';
    };

    const parseYearToken = (description) => {
      const raw = String(description || '');
      const match = raw.match(/FQ_DEMO_YEAR:(\d+)/i);
      if (!match) return null;
      const year = parseInt(match[1], 10);
      return Number.isFinite(year) && year > 0 ? year : null;
    };

    const parseProductKeyToken = (description) => {
      const raw = String(description || '');
      const match = raw.match(/FQ_DEMO_PRODUCT_KEY:([a-z0-9-]+)/i);
      if (!match) return null;
      return String(match[1]).toLowerCase();
    };

    const sanitizeKey = (rawValue, fallback = 'product') =>
      String(rawValue || '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '') || fallback;

    const toPriceString = (value) => {
      const n = Number(value);
      if (!Number.isFinite(n) || n < 0) return '0';
      return (Math.round(n * 100) / 100).toFixed(2);
    };

    const normalizedProducts = productItems
      .map((item, index) => {
        const key = sanitizeKey(
          item?.key || item?.productCode || item?.productName,
          `product-${index + 1}`
        );
        const quantity = Math.max(0, parseFloat(item?.quantity) || 0);
        const netArr = Math.max(0, parseFloat(item?.netArr) || 0);
        const unitPrice =
          Math.max(0, parseFloat(item?.unitPrice)) ||
          (quantity > 0 ? netArr / quantity : netArr);

        return {
          key,
          productCode: String(item?.productCode || '').trim(),
          productName: String(item?.productName || '').trim() || 'Product',
          quantity,
          unitPrice,
          netArr,
          revenueType: normalizeRevenueType(item?.revenueType),
          startDate: fmtDateForHS(item?.startDate),
        };
      })
      .filter(Boolean);

    const normalizedYearsAsProducts = yearItems
      .map((item) => {
        const year = parseInt(item?.year, 10);
        if (!Number.isFinite(year) || year <= 0) return null;
        const netArr = Math.max(0, parseFloat(item?.netArr) || 0);
        return {
          key: `year-${year}`,
          productCode: `DEMO-Y${year}`,
          productName: `Year ${year}`,
          quantity: 1,
          unitPrice: netArr,
          netArr,
          revenueType: normalizeRevenueType(item?.revenueType),
          startDate: fmtDateForHS(item?.startDate),
        };
      })
      .filter(Boolean);

    const normalizedItems = normalizedProducts.length > 0 ? normalizedProducts : normalizedYearsAsProducts;

    const incomingKeySet = new Set(normalizedItems.map((item) => item.key));

    const associatedIds = await getAssociatedIds('0-3', dealId, 'line_items');
    const existingManagedByKey = {};

    for (const lineItemId of associatedIds) {
      try {
        const lineItem = await getObject('line_items', lineItemId, [
          'description',
          'name',
          'price',
          'quantity',
          'hs_sku',
          'hs_recurring_billing_period',
          'hs_recurring_billing_start_date',
          'revenue_type',
        ]);
        const productKey = parseProductKeyToken(lineItem?.properties?.description);
        const year = parseYearToken(lineItem?.properties?.description);
        const key = productKey || (year ? `year-${year}` : null);
        if (!key) continue;
        existingManagedByKey[key] = lineItem;
      } catch (e) {
        console.warn('[upsert-demo-line-items] Failed to read line item', lineItemId, e.message);
      }
    }

    let created = 0;
    let updated = 0;
    let removed = 0;
    const lineItems = [];

    for (const item of normalizedItems) {
      const quantity = Math.max(0, parseFloat(item.quantity) || 0);
      const unitPrice = Math.max(0, parseFloat(item.unitPrice) || 0);

      const props = {
        name: `Demo CPQ - ${item.productName || item.productCode || item.key}`,
        quantity: String(quantity > 0 ? quantity : 1),
        price: toPriceString(unitPrice),
        hs_sku: item.productCode || `DEMO-${item.key.toUpperCase()}`,
        hs_recurring_billing_period: 'P12M',
        description: `FinQuery CPQ demo line item | FQ_DEMO_PRODUCT_KEY:${item.key}`,
        revenue_type: item.revenueType,
      };

      if (item.startDate) {
        props.hs_recurring_billing_start_date = item.startDate;
      }

      const existing = existingManagedByKey[item.key];
      if (existing?.id) {
        await updateLineItemWithFallback(existing.id, props);
        updated++;
        lineItems.push({ id: existing.id, key: item.key, ...props });
      } else {
        const createdItem = await createDealLineItemWithFallback(dealId, props);
        created++;
        lineItems.push({ id: createdItem.id, key: item.key, ...props });
      }
    }

    for (const [key, existing] of Object.entries(existingManagedByKey)) {
      if (incomingKeySet.has(key)) continue;
      if (!existing?.id) continue;
      try {
        await hs.delete(`/crm/v3/objects/line_items/${existing.id}`);
        removed++;
      } catch (e) {
        console.warn('[upsert-demo-line-items] Failed to delete stale demo line item', existing.id, e.message);
      }
    }

    res.json({
      success: true,
      message: `Demo product line items synced (${created} created, ${updated} updated, ${removed} removed)`,
      created,
      updated,
      removed,
      lineItems,
    });
  } catch (e) {
    console.error('[upsert-demo-line-items] Error:', e.response?.data || e.message);
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

    const category = dp.deal_category || 'new_business';
    const contractIds = await getAssociatedIds('0-3', dealId, contractTypeId);

    // ── Helper: read deal line items ─────────────────────────────────
    async function getDealLineItems() {
      const lineItemIds = await getAssociatedIds('0-3', dealId, 'line_items');
      if (lineItemIds.length === 0) return [];
      const items = await Promise.all(
        lineItemIds.map((id) =>
          getObject('line_items', id, [
            'name', 'quantity', 'price', 'amount', 'hs_sku', 'description',
            'hs_line_item_currency_code', 'hs_recurring_billing_period',
            'hs_recurring_billing_start_date', 'revenue_type',
          ])
        )
      );
      return items;
    }

    function isRecurringLineItem(li) {
      const lp = li.properties || {};
      const period = (lp.hs_recurring_billing_period || '').toLowerCase();
      return period && period !== 'one_time' && period !== 'onetime';
    }

    // ── Helper: create subscription segments from recurring line items ─
    async function createSegmentsFromLineItems(cId, lineItems, opts = {}) {
      const {
        startDate,
        endDate,
        revenueType,
        amendmentIndicator,
        dealCategory,
        companyId,
      } = opts;

      const recurringItems = lineItems.filter(isRecurringLineItem);
      let created = 0;
      let totalArr = 0;

      for (const li of recurringItems) {
        const lp = li.properties || {};
        const qty = parseInt(lp.quantity) || 1;
        const unitPrice = parseFloat(lp.price) || 0;
        const lineArr = unitPrice * qty;
        totalArr += lineArr;

        const segProps = {
          segment_name: lp.name || 'Product',
          product_name: lp.name || 'Product',
          product_code: lp.hs_sku || '',
          quantity: String(qty),
          unit_price: String(unitPrice),
          arr: String(lineArr),
          mrr: String(lineArr / 12),
          tcv: String(lineArr),
          status: determineStatus(startDate, endDate),
          start_date: startDate,
          end_date: endDate,
          segment_year: '1',
          segment_index: String(created),
          segment_label: `Year 1`,
          revenue_type: normalizeLineRevenueType(lp.revenue_type, revenueType || ''),
        };

        if (amendmentIndicator) {
          let indicator = amendmentIndicator;
          if (dealCategory === 'amendment') {
            const lineRevType = normalizeLineRevenueType(lp.revenue_type, 'renewal');
            if (lineRevType === 'expansion' || lineRevType === 'cross_sell' || lineRevType === 'new') {
              indicator = 'Expansion';
            } else if (lineRevType === 'contraction') {
              indicator = 'Contraction';
            } else {
              indicator = 'Renewal';
            }
          }
          // Per-line-item amendment indicator: on contraction deals, items tagged
          // as expansion or cross-sell get "Expansion" instead of "Contraction"
          if (dealCategory === 'contraction') {
            const lineRevType = (lp.revenue_type || '').toLowerCase();
            if (lineRevType === 'expansion' || lineRevType === 'cross_sell') {
              indicator = 'Expansion';
            }
          }
          segProps.amendment_indicator = indicator;
        }

        const seg = await createObject(subscriptionTypeId, segProps);
        try { await createAssociation(subscriptionTypeId, seg.id, contractTypeId, cId); } catch (e) { /* ok */ }
        if (companyId) {
          try { await createAssociation(subscriptionTypeId, seg.id, '0-2', companyId); } catch (e) { /* ok */ }
        }
        created++;
        console.log(`[update-contract] Created segment ${seg.id}: ${lp.name} x${qty} @ ${unitPrice}`);
      }

      const skippedOneTime = lineItems.length - recurringItems.length;
      if (skippedOneTime > 0) {
        console.log(`[update-contract] Skipped ${skippedOneTime} one-time line items (not subscription segments)`);
      }

      return { created, totalArr };
    }

    // ── Helper: copy ALL line items to contract ─────────────────────
    async function copyLineItemsToContract(cId, lineItems) {
      let created = 0;
      for (const li of lineItems) {
        const lp = li.properties || {};
        try {
          const contractLineItem = await createObject('line_items', {
            name: lp.name || 'Product',
            quantity: lp.quantity || '1',
            price: lp.price || '0',
            amount: lp.amount || '',
            hs_sku: lp.hs_sku || '',
            description: lp.description || '',
            hs_recurring_billing_period: lp.hs_recurring_billing_period || '',
            revenue_type: lp.revenue_type || '',
          });
          await createAssociation('line_items', contractLineItem.id, contractTypeId, cId);
          created++;
          console.log(`[update-contract] Copied line item to contract: ${lp.name} (${isRecurringLineItem(li) ? 'recurring' : 'one-time'})`);
        } catch (e) {
          console.warn(`[update-contract] Failed to copy line item ${lp.name} to contract:`, e.response?.data?.message || e.message);
        }
      }
      return created;
    }

    // ═══ NEW BUSINESS ═══════════════════════════════════════════════════
    if (contractIds.length === 0 && category === 'new_business') {
      const companyIds = await getAssociatedIds('0-3', dealId, '0-2');
      const startDate = dp.contract_start_date || fmtDateForHS(new Date());
      let endDate = dp.contract_end_date;
      if (!endDate) {
        const ed = new Date(startDate);
        ed.setFullYear(ed.getFullYear() + 1);
        endDate = fmtDateForHS(ed);
      }

      const lineItems = await getDealLineItems();

      const contract = await createObject(contractTypeId, {
        contract_name: dp.dealname.replace(' — New Business', '').replace(' - New Business', ''),
        status: determineStatus(startDate, endDate),
        start_date: startDate,
        end_date: endDate,
        co_term_date: endDate,
        total_arr: dp.amount || '0',
        total_tcv: dp.amount || '0',
        subscription_count: String(lineItems.length),
        amendment_count: '0',
      });

      try { await createAssociation(contractTypeId, contract.id, '0-3', dealId); } catch (e) { /* ok */ }
      const companyId = companyIds.length > 0 ? companyIds[0] : null;
      if (companyId) {
        try { await createAssociation(contractTypeId, contract.id, '0-2', companyId); } catch (e) { /* ok */ }
      }

      let segmentsCreated = 0;
      let contractLineItems = 0;
      if (lineItems.length > 0) {
        contractLineItems = await copyLineItemsToContract(contract.id, lineItems);

        if (subscriptionTypeId) {
          const result = await createSegmentsFromLineItems(contract.id, lineItems, {
            startDate,
            endDate,
            revenueType: 'new',
            companyId,
          });
          segmentsCreated = result.created;

          if (result.totalArr > 0) {
            await updateObject(contractTypeId, contract.id, {
              total_arr: String(result.totalArr),
              total_tcv: String(result.totalArr),
              subscription_count: String(segmentsCreated),
            });
          }
        }
      }

      return res.json({
        success: true,
        message: `New contract created with ${segmentsCreated} subscription segments and ${contractLineItems} line items`,
        contractId: contract.id,
        segmentsCreated,
        contractLineItems,
      });
    }

    // ═══ EXISTING CONTRACT ═══════════════════════════════════════════════
    if (contractIds.length > 0) {
      const cid = contractIds[0];
      const contract = await getObject(contractTypeId, cid, CONTRACT_PROPS);
      const cp = contract.properties;
      const updates = {};

      // ── RENEWAL — creates NEW contract, expires old one ───────────
      if (category === 'renewal') {
        const renewalStart = dp.contract_start_date || fmtDateForHS(new Date());
        let renewalEnd = dp.contract_end_date;
        if (!renewalEnd) {
          const ed = new Date(renewalStart);
          ed.setFullYear(ed.getFullYear() + 1);
          renewalEnd = fmtDateForHS(ed);
        }

        const companyIds = await getAssociatedIds(contractTypeId, cid, '0-2');
        const companyId = companyIds.length > 0 ? companyIds[0] : null;
        const lineItems = await getDealLineItems();

        // Create new contract for the renewal term
        const contractName = (cp.contract_name || 'Contract').replace(/ — Renewal.*$/, '');
        const newContract = await createObject(contractTypeId, {
          contract_name: contractName,
          status: determineStatus(renewalStart, renewalEnd),
          start_date: renewalStart,
          end_date: renewalEnd,
          co_term_date: renewalEnd,
          total_arr: dp.amount || cp.total_arr || '0',
          total_tcv: dp.amount || cp.total_tcv || '0',
          subscription_count: '0',
          amendment_count: '0',
          replaces_contract: cid,
          contract_renewed_on: fmtDateForHS(new Date()),
          has_legacy_products: cp.has_legacy_products || 'false',
        });

        // Associate new contract to deal, company, contacts
        try { await createAssociation(contractTypeId, newContract.id, '0-3', dealId); } catch (e) { /* ok */ }
        if (companyId) {
          try { await createAssociation(contractTypeId, newContract.id, '0-2', companyId); } catch (e) { /* ok */ }
        }
        const contactIds = await getAssociatedIds(contractTypeId, cid, '0-1');
        for (const contactId of contactIds) {
          try { await createAssociation(contractTypeId, newContract.id, '0-1', contactId); } catch (e) { /* ok */ }
        }

        // Copy line items to new contract + create subscription segments
        let contractLineItemsCopied = 0;
        let segmentsCreated = 0;
        if (lineItems.length > 0) {
          contractLineItemsCopied = await copyLineItemsToContract(newContract.id, lineItems);

          if (subscriptionTypeId) {
            const result = await createSegmentsFromLineItems(newContract.id, lineItems, {
              startDate: renewalStart,
              endDate: renewalEnd,
              revenueType: 'renewal',
              companyId,
            });
            segmentsCreated = result.created;

            if (result.totalArr > 0) {
              await updateObject(contractTypeId, newContract.id, {
                total_arr: String(result.totalArr),
                total_tcv: String(result.totalArr),
                subscription_count: String(segmentsCreated),
              });
            }
          }
        }

        // Expire old contract and link to new
        await updateObject(contractTypeId, cid, {
          status: 'expired',
          replaced_by_contract: newContract.id,
          contract_renewed_on: fmtDateForHS(new Date()),
        });

        return res.json({
          success: true,
          message: `Renewal contract created: ${renewalStart} → ${renewalEnd} (${segmentsCreated} segments, ${contractLineItemsCopied} line items). Old contract ${cid} expired.`,
          contractId: newContract.id,
          oldContractId: cid,
          segmentsCreated,
          contractLineItems: contractLineItemsCopied,
        });
      }

      // ── AMENDMENT / EXPANSION / CONTRACTION ──────────────────────────
      if (category === 'amendment' || category === 'expansion' || category === 'contraction') {
        const lineItems = await getDealLineItems();
        const companyIds = await getAssociatedIds(contractTypeId, cid, '0-2');
        const companyId = companyIds.length > 0 ? companyIds[0] : null;

        const amendStartDate = dp.contract_start_date || cp.amendment_start_date || fmtDateForHS(new Date());
        const amendEndDate = dp.contract_end_date || cp.end_date;

        let segmentsCreated = 0;
        let contractLineItemsCopied = 0;
        let newTotalArr = 0;

        if (lineItems.length > 0) {
          // Amendment processing replaces contract line items with the deal's full post-amendment set
          const existingLineItemIds = await getAssociatedIds(contractTypeId, cid, 'line_items');
          for (const liId of existingLineItemIds) {
            try {
              await hs.delete(`/crm/v3/objects/line_items/${liId}`);
            } catch (e) {
              console.warn(`[update-contract] Failed to remove old line item ${liId}:`, e.response?.data?.message || e.message);
            }
          }
          console.log(`[update-contract] Removed ${existingLineItemIds.length} existing line items from contract ${cid} before ${category} replacement`);

          contractLineItemsCopied = await copyLineItemsToContract(cid, lineItems);

          if (subscriptionTypeId) {
            const result = await createSegmentsFromLineItems(cid, lineItems, {
              startDate: amendStartDate,
              endDate: amendEndDate,
              revenueType: category === 'amendment' ? 'renewal' : category,
              amendmentIndicator: category === 'expansion' ? 'Expansion' : (category === 'contraction' ? 'Contraction' : 'Renewal'),
              dealCategory: category,
              companyId,
            });
            segmentsCreated = result.created;
            newTotalArr = result.totalArr;
          }
        }

        const amendCount = parseInt(cp.amendment_count) || 0;
        updates.amendment_count = String(amendCount + 1);
        updates.amendment_start_date = amendStartDate;

        // Recalculate metrics from all segments
        const allSubIds = await getAssociatedIds(contractTypeId, cid, subscriptionTypeId);
        if (allSubIds.length > 0) {
          const allSubs = await Promise.all(allSubIds.map((id) => getObject(subscriptionTypeId, id, SUBSCRIPTION_PROPS)));
          const metrics = calcMetrics(allSubs);
          updates.total_arr = String(metrics.total_arr);
          updates.total_tcv = String(metrics.total_tcv);
          updates.lq_arr = String(metrics.lq_arr);
          updates.fcm_arr = String(metrics.fcm_arr);
          updates.subscription_count = String(metrics.subscription_count);
          newTotalArr = metrics.total_arr;
        }

        updates.status = determineStatus(cp.start_date, cp.end_date);

        // Auto-terminate if amendment results in zero ARR
        if (newTotalArr <= 0 && (category === 'contraction' || category === 'amendment')) {
          console.log(`[update-contract] Zero-ARR detected after ${category} — auto-terminating contract ${cid}`);
          updates.status = 'terminated';
          updates.terminated_date = fmtDateForHS(new Date());
          updates.termination_reason = 'amendment';

          // Terminate all segments
          if (allSubIds && allSubIds.length > 0) {
            for (const subId of allSubIds) {
              try {
                await updateObject(subscriptionTypeId, subId, {
                  status: 'terminated',
                  terminated_date: fmtDateForHS(new Date()),
                });
              } catch (e) { /* best-effort */ }
            }
          }
        }

        await updateObject(contractTypeId, cid, updates);

        const terminated = updates.status === 'terminated';
        return res.json({
          success: true,
          message: terminated
            ? `Contract ${cid} auto-terminated (zero ARR after ${category})`
            : `Contract ${cid} updated: ${segmentsCreated} segments added from ${category}`,
          contractId: cid,
          segmentsCreated,
          autoTerminated: terminated,
        });
      }

      // ── FALLBACK for other categories ────────────────────────────────
      const newStatus = determineStatus(cp.start_date, cp.end_date);
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
