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
  timeout: 30000,
});

// ── HubSpot Rate-Limit Handling ──────────────────────────────────────────────
// HubSpot Private Apps cap at 100 req / 10 sec (150 on Pro/Enterprise). Without
// retry/backoff a single burst (e.g. closing a multi-year deal that creates
// many subscription segments + associations) trips a 429 and the whole request
// fails. This interceptor retries 429 / 502 / 503 / 504 with exponential
// backoff + jitter, honoring the `Retry-After` header when present.
const HS_MAX_RETRIES = 5;
const HS_BASE_DELAY_MS = 500;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryHubSpot(error) {
  if (!error || !error.config) return false;
  if (error.config.__hsNoRetry) return false;
  const status = error.response?.status;
  if (status === 429) return true;
  if (status === 502 || status === 503 || status === 504) return true;
  // Retry transient network failures (ECONNRESET, ETIMEDOUT, etc.) once or twice.
  if (!error.response && error.code && ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN'].includes(error.code)) {
    return true;
  }
  return false;
}

function computeRetryDelay(error, attempt) {
  const retryAfterHeader = error.response?.headers?.['retry-after'];
  if (retryAfterHeader) {
    const seconds = Number(retryAfterHeader);
    if (Number.isFinite(seconds) && seconds > 0) {
      return Math.min(seconds * 1000, 15000);
    }
  }
  const exp = HS_BASE_DELAY_MS * Math.pow(2, attempt);
  const jitter = Math.floor(Math.random() * 250);
  return Math.min(exp + jitter, 10000);
}

hs.interceptors.response.use(
  (response) => response,
  async (error) => {
    const config = error.config;
    if (!config) return Promise.reject(error);
    config.__hsRetryCount = config.__hsRetryCount || 0;

    if (config.__hsRetryCount >= HS_MAX_RETRIES || !shouldRetryHubSpot(error)) {
      if (error.response?.status === 429) {
        console.error(
          `[hubspot] 429 after ${config.__hsRetryCount} retries on ${config.method?.toUpperCase()} ${config.url}`
        );
      }
      return Promise.reject(error);
    }

    const delayMs = computeRetryDelay(error, config.__hsRetryCount);
    config.__hsRetryCount += 1;
    console.warn(
      `[hubspot] ${error.response?.status || error.code || 'network'} on ${config.method?.toUpperCase()} ${config.url} ` +
      `— retry ${config.__hsRetryCount}/${HS_MAX_RETRIES} in ${delayMs}ms`
    );
    await sleep(delayMs);
    return hs.request(config);
  }
);

// ── Product Registry ─────────────────────────────────────────────────────────
const PRODUCT_REGISTRY = {
  LQ:  { code: 'LQ',  name: 'LeaseQuery',                category: 'core', arrField: 'lq_arr' },
  FCM: { code: 'FCM', name: 'FinQuery Contract Management', category: 'core', arrField: 'fcm_arr' },
};

// Prevent slow card loads on contracts with very large deal histories.
const CARD_DEAL_LOAD_LIMIT = 10;

// ── Super Admin Allowlist ────────────────────────────────────────────────────
// Test-data buttons (Load Test Data, Load NetSuite Test Data, etc.) and other
// privileged actions are only shown in the UI for users in this allowlist. The
// list is sourced from the SUPER_ADMIN_EMAILS env var (comma-separated) and is
// surfaced to the cards via /api/ensure-setup so the front-end can compare
// against context.user.email. Defaults cover Zach + Betty for the FinQuery
// engagement so the buttons stay usable in sandbox even if the env var is not
// set; production should override this via Railway env config.
const SUPER_ADMIN_EMAILS = (process.env.SUPER_ADMIN_EMAILS || 'zach@patchops.io,betty@patchops.io')
  .split(',')
  .map((e) => e.trim().toLowerCase())
  .filter(Boolean);

function isSuperAdminEmail(email) {
  if (!email) return false;
  return SUPER_ADMIN_EMAILS.includes(String(email).trim().toLowerCase());
}

// ── Type ID Cache ────────────────────────────────────────────────────────────
let contractTypeId = null;
let subscriptionTypeId = null;
let companyArrPropertyMap = null;

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

function normalizePropertyLabel(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

async function resolveCompanyArrPropertyMap() {
  if (companyArrPropertyMap) return companyArrPropertyMap;

  const fallback = {
    currentArr: null,
    activeArr: null,
  };

  const fallbackCandidates = {
    currentArr: ['current_arr', 'current_arr__c', 'currentannualrecurringrevenue'],
    activeArr: ['active_arr', 'active_arr__c', 'activeannualrecurringrevenue'],
  };

  try {
    const { data } = await hs.get('/crm/v3/properties/0-2', {
      params: { archived: false },
    });
    const props = data.results || [];
    const availableNames = new Set(props.map((p) => p.name));

    const findByLabel = (label) => {
      const normalizedLabel = normalizePropertyLabel(label);
      const found = props.find((p) => normalizePropertyLabel(p.label) === normalizedLabel);
      return found?.name || null;
    };

    const resolveName = (label, candidates) =>
      findByLabel(label) || candidates.find((name) => availableNames.has(name)) || null;

    companyArrPropertyMap = {
      currentArr: resolveName('Current ARR', fallbackCandidates.currentArr),
      activeArr: resolveName('Active ARR', fallbackCandidates.activeArr),
    };
    return companyArrPropertyMap;
  } catch (e) {
    console.warn('[company-arr] Could not resolve company ARR property names:', e.response?.data?.message || e.message);
    companyArrPropertyMap = fallback;
    return companyArrPropertyMap;
  }
}

async function syncCompanyArrRollups(companyId) {
  if (!companyId) {
    return {
      companyId,
      totalArr: 0,
      activeContracts: 0,
      updatedProperties: [],
    };
  }

  await resolveTypeIds();
  if (!contractTypeId) {
    return {
      companyId,
      totalArr: 0,
      activeContracts: 0,
      updatedProperties: [],
      warning: 'Contract schema not found',
    };
  }

  const contractIds = await getAssociatedIds('0-2', companyId, contractTypeId);
  let totalArr = 0;
  let activeContracts = 0;

  for (const cid of contractIds) {
    try {
      const contract = await getObject(contractTypeId, cid, ['status', 'total_arr']);
      const cp = contract.properties || {};
      if (cp.status === 'active') {
        activeContracts++;
        totalArr += parseFloat(cp.total_arr) || 0;
      }
    } catch (e) {
      console.warn(`[company-arr] Skipping contract ${cid} while syncing company ${companyId}:`, e.message);
    }
  }

  const propertyMap = await resolveCompanyArrPropertyMap();
  const updates = {};
  if (propertyMap.currentArr) updates[propertyMap.currentArr] = String(totalArr);
  if (propertyMap.activeArr) updates[propertyMap.activeArr] = String(totalArr);

  const updatedProperties = Object.keys(updates);
  if (updatedProperties.length > 0) {
    try {
      await updateObject('0-2', companyId, updates);
    } catch (e) {
      console.warn(`[company-arr] Failed updating company ${companyId}:`, e.response?.data?.message || e.message);
      return {
        companyId,
        totalArr,
        activeContracts,
        updatedProperties: [],
        warning: 'Company ARR property update failed',
      };
    }
  } else {
    console.warn(`[company-arr] No company ARR properties found for company ${companyId} (wanted Current ARR / Active ARR).`);
  }

  return {
    companyId,
    totalArr,
    activeContracts,
    updatedProperties,
  };
}

function fmtDateForHS(d) {
  if (!d) return null;
  const dt = d instanceof Date ? d : new Date(d);
  if (isNaN(dt.getTime())) return null;
  return dt.toISOString().split('T')[0];
}

function parseDateValue(value) {
  if (!value) return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value);
  }
  if (typeof value === 'number') {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;

    // HubSpot date-only fields are "YYYY-MM-DD"; parse in local time to avoid
    // timezone-driven day shifts (e.g., 3/16 rendering as 3/15).
    const ymd = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (ymd) {
      const y = Number(ymd[1]);
      const m = Number(ymd[2]) - 1;
      const d = Number(ymd[3]);
      const parsed = new Date(y, m, d);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    if (/^\d+$/.test(trimmed)) {
      const d = new Date(Number(trimmed));
      return Number.isNaN(d.getTime()) ? null : d;
    }

    const parsed = new Date(trimmed);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  return null;
}

function getContractEndDate(props = {}) {
  return props.end_date || props.co_term_date || null;
}

function determineStatus(startDate, endDate) {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  const start = parseDateValue(startDate);
  const end = parseDateValue(endDate);
  if (start) start.setHours(0, 0, 0, 0);
  if (end) end.setHours(0, 0, 0, 0);

  if (start && start > now) return 'future';
  if (end && end < now) return 'expired';
  if (start && start <= now && (!end || end >= now)) return 'active';
  return 'expired';
}

function addDaysToDate(dateObj, days) {
  const d = new Date(dateObj);
  d.setDate(d.getDate() + days);
  return d;
}

// Splits a [start, end] contract span into 12-month segment buckets:
// Year 1 = [start, start+1y-1d], Year 2 = [start+1y, start+2y-1d], ... with the
// last segment truncated to `end`. Returns [{ year, start_date, end_date }].
function buildYearSegments(startDateStr, endDateStr) {
  const start = parseDateValue(startDateStr);
  const end = parseDateValue(endDateStr);
  if (!start || !end || start > end) {
    return [{ year: 1, start_date: startDateStr, end_date: endDateStr }];
  }

  const segments = [];
  let currentStart = new Date(start);
  let segmentYear = 1;

  while (currentStart <= end) {
    const nextYearStart = new Date(currentStart);
    nextYearStart.setFullYear(nextYearStart.getFullYear() + 1);
    const candidateEnd = addDaysToDate(nextYearStart, -1);
    const currentEnd = candidateEnd > end ? new Date(end) : candidateEnd;

    segments.push({
      year: segmentYear,
      start_date: fmtDateForHS(currentStart),
      end_date: fmtDateForHS(currentEnd),
    });

    if (currentEnd >= end) break;
    currentStart = addDaysToDate(currentEnd, 1);
    segmentYear += 1;
  }

  return segments;
}

// Derive a short product code for segment_name / product_code from a line item.
// Prefers the head of hs_sku (e.g. "LQ-CORE" -> "LQ"); falls back to initials of
// the product name.
function deriveProductCode(lineItemProps = {}) {
  const sku = String(lineItemProps.hs_sku || '').trim();
  if (sku) {
    const head = sku.toUpperCase().split(/[-_\s]/)[0];
    if (head) return head;
    return sku.toUpperCase();
  }
  const name = String(lineItemProps.name || '').trim();
  if (!name) return 'PRODUCT';
  const initials = name
    .split(/\s+/)
    .map((w) => w[0])
    .filter(Boolean)
    .join('')
    .toUpperCase();
  return (initials || name).slice(0, 16);
}

function toMidnight(value) {
  const d = parseDateValue(value);
  if (!d) return null;
  d.setHours(0, 0, 0, 0);
  return d;
}

function chooseCurrentSegmentYear(subscriptions) {
  const today = toMidnight(new Date());
  const byYear = new Map();

  for (const sub of subscriptions) {
    const sp = sub.properties || {};
    if (sp.status === 'terminated') continue;
    const year = parseInt(sp.segment_year, 10) || 1;
    if (!byYear.has(year)) {
      byYear.set(year, {
        year,
        arr: 0,
        tcv: 0,
        lqArr: 0,
        fcmArr: 0,
        earliestStart: null,
        latestEnd: null,
      });
    }
    const bucket = byYear.get(year);
    const arr = parseFloat(sp.arr) || 0;
    const tcv = parseFloat(sp.tcv) || 0;
    const code = (sp.product_code || '').toUpperCase();
    const start = toMidnight(sp.start_date || sp.segment_start_date);
    const end = toMidnight(sp.end_date || sp.segment_end_date);

    bucket.arr += arr;
    bucket.tcv += tcv;
    if (code === 'LQ') bucket.lqArr += arr;
    if (code === 'FCM') bucket.fcmArr += arr;

    if (start && (!bucket.earliestStart || start < bucket.earliestStart)) {
      bucket.earliestStart = start;
    }
    if (end && (!bucket.latestEnd || end > bucket.latestEnd)) {
      bucket.latestEnd = end;
    }
  }

  if (byYear.size === 0) return null;

  const years = Array.from(byYear.values()).sort((a, b) => a.year - b.year);
  const active = years.find((y) => {
    const hasStarted = !y.earliestStart || y.earliestStart <= today;
    const notEnded = !y.latestEnd || y.latestEnd >= today;
    return hasStarted && notEnded;
  });
  if (active) return active;

  const past = years
    .filter((y) => y.latestEnd && y.latestEnd < today)
    .sort((a, b) => b.latestEnd - a.latestEnd);
  if (past.length > 0) return past[0];

  const future = years
    .filter((y) => y.earliestStart && y.earliestStart > today)
    .sort((a, b) => a.earliestStart - b.earliestStart);
  if (future.length > 0) return future[0];

  return years[0];
}

function calcMetrics(subscriptions) {
  const metrics = { total_arr: 0, total_tcv: 0, lq_arr: 0, fcm_arr: 0, subscription_count: subscriptions.length };
  const currentSegment = chooseCurrentSegmentYear(subscriptions);

  for (const sub of subscriptions) {
    const sp = sub.properties || {};
    if (sp.status === 'terminated') continue;
    const tcv = parseFloat(sp.tcv) || 0;
    metrics.total_tcv += tcv;
  }

  if (currentSegment) {
    metrics.total_arr = currentSegment.arr;
    metrics.lq_arr = currentSegment.lqArr;
    metrics.fcm_arr = currentSegment.fcmArr;
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
  if (!period) return null;
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
  const warnings = [];
  let recurringSourceItems = [];
  let inheritanceSource = 'none';

  // Primary source: subscription segments (renewal/amendment model of record).
  if (subscriptionTypeId) {
    const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
    console.log(
      `[sync-contract-lines] Contract ${contractId}: found ${subIds.length} associated subscription segment(s)`
    );
    if (subIds.length > 0) {
      const subRecords = await Promise.all(
        subIds.map((id) =>
          getObject(subscriptionTypeId, id, [
            'status',
            'product_name',
            'product_code',
            'quantity',
            'original_quantity',
            'renewal_quantity',
            'unit_price',
            'arr',
            'mrr',
            'billing_frequency',
            'revenue_type',
            'segment_year',
            'arr_end_date',
            'end_date',
          ])
        )
      );

      const today = new Date();
      today.setHours(0, 0, 0, 0);

      const isStillRunning = (sub) => {
        const p = sub?.properties || {};
        const endRaw = p.arr_end_date || p.end_date;
        const endDate = parseDateValue(endRaw);
        if (!endDate) return true; // unknown end date -> assume still active
        return endDate >= today;
      };

      const statusCounts = {};
      subRecords.forEach((sub) => {
        const s = String(sub?.properties?.status || '').toLowerCase().trim() || '(empty)';
        statusCounts[s] = (statusCounts[s] || 0) + 1;
      });
      console.log(
        `[sync-contract-lines] Contract ${contractId}: subscription status breakdown: ${JSON.stringify(statusCounts)}`
      );

      const isInheritableStatus = (sub) => {
        const status = String(sub?.properties?.status || '').toLowerCase().trim();
        if (status === 'active' || status === 'future') return true;
        // Treat blank status as inheritable when the segment is still running. This
        // catches imported/legacy segments that never had the status field populated.
        if (!status && isStillRunning(sub)) return true;
        return false;
      };

      let inheritableSubs = subRecords.filter(isInheritableStatus);

      // Defensive fallback: if no segments matched the status filter but there are
      // still segments whose end date is today or later, inherit those instead of
      // returning an empty deal. Renewals on imported contracts often have legacy
      // status values and we'd rather seed line items than ship an empty deal.
      if (inheritableSubs.length === 0) {
        const stillRunning = subRecords.filter(isStillRunning);
        if (stillRunning.length > 0) {
          warnings.push(
            `No subscription segments had an active/future status; inheriting ${stillRunning.length} still-running segment(s) by date instead.`
          );
          console.warn(
            `[sync-contract-lines] Contract ${contractId}: status filter matched 0 segments. ` +
            `Falling back to ${stillRunning.length} still-running segment(s) based on end date.`
          );
          inheritableSubs = stillRunning;
        }
      }

      if (inheritableSubs.length > 0) {
        const billingFrequencyToPeriod = (billingFrequencyRaw) => {
          const value = String(billingFrequencyRaw || '').trim().toLowerCase();
          if (value === 'monthly') return 'P1M';
          if (value === 'quarterly') return 'P3M';
          if (value === 'semi-annual' || value === 'semi_annual') return 'P6M';
          if (value === 'annual' || value === 'yearly') return 'P12M';
          return 'P12M';
        };

        recurringSourceItems = inheritableSubs.map((sub, index) => {
          const p = sub.properties || {};
          const quantity = Number(p.renewal_quantity || p.quantity || p.original_quantity || 1) || 1;
          const unitPriceFromField = Number(p.unit_price || 0) || 0;
          const arr = Number(p.arr || 0) || 0;
          const mrr = Number(p.mrr || 0) || 0;
          const annualAmount = arr > 0 ? arr : (mrr > 0 ? mrr * 12 : 0);
          const computedUnitPrice = unitPriceFromField > 0
            ? unitPriceFromField
            : (quantity > 0 ? annualAmount / quantity : annualAmount);

          return {
            id: sub.id || `fallback-sub-${index + 1}`,
            properties: {
              name: p.product_name || p.product_code || `Subscription ${index + 1}`,
              quantity: String(quantity > 0 ? quantity : 1),
              price: String(Math.max(0, computedUnitPrice || 0)),
              hs_sku: p.product_code || '',
              description: '',
              hs_recurring_billing_period: billingFrequencyToPeriod(p.billing_frequency),
              revenue_type: p.revenue_type || fallbackRevenueType,
            },
          };
        });
        inheritanceSource = 'subscription_segments';
      } else {
        warnings.push('No active/future/still-running subscription segments found; falling back to contract line items');
      }
    } else {
      warnings.push('No subscription segments associated; falling back to contract line items');
    }
  } else {
    console.warn(
      `[sync-contract-lines] subscriptionTypeId is not resolved; falling back to contract line items for contract ${contractId}`
    );
    warnings.push('Subscription schema not resolved; falling back to contract line items');
  }

  // Legacy fallback: use recurring contract line items only when no subscription source is available.
  if (recurringSourceItems.length === 0) {
    const sourceLineItemIds = await getAssociatedIds(contractTypeId, contractId, 'line_items');
    console.log(
      `[sync-contract-lines] Contract ${contractId}: legacy fallback -> ${sourceLineItemIds.length} contract line item(s) found`
    );
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

    recurringSourceItems = sourceItems.filter((item) => {
      const period = normalizeRecurringPeriod(item?.properties?.hs_recurring_billing_period);
      return !!period;
    });

    if (recurringSourceItems.length > 0) {
      warnings.push(`Used ${recurringSourceItems.length} recurring contract line items as fallback`);
      inheritanceSource = 'contract_line_items';
    }
  }

  if (recurringSourceItems.length === 0) {
    warnings.push('No inheritable recurring source items were found on subscriptions or contract line items');
    console.warn(
      `[sync-contract-lines] Contract ${contractId}: NOTHING to inherit (no subscriptions matched and no recurring line items found). Renewal deal will have zero line items.`
    );
    return {
      sourceRecurringCount: 0,
      lineItemsCreated: 0,
      lineItemsUpdated: 0,
      lineItemsRemoved: 0,
      inheritanceSource,
      warnings,
    };
  }

  console.log(
    `[sync-contract-lines] Contract ${contractId}: inheriting ${recurringSourceItems.length} recurring item(s) from ${inheritanceSource}`
  );

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
  let failed = 0;
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
      failed++;
      const lineName = item.name || item.key;
      const detail = e.response?.data?.message || e.message;
      warnings.push(`Failed syncing line item "${lineName}": ${detail}`);
      console.error('[sync-contract-lines] Failed line sync:', lineName, e.response?.data || e.message);
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
    lineItemsFailed: failed,
    inheritanceSource,
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
  return {
    contractTypeId,
    subscriptionTypeId,
    productRegistry: PRODUCT_REGISTRY,
    superAdminEmails: SUPER_ADMIN_EMAILS,
  };
}

// ── Route: Health ────────────────────────────────────────────────────────────

const API_VERSION = '2026-04-08a';

app.get('/', (req, res) => res.json({ status: 'ok', service: 'finquery-contracts-api', version: API_VERSION }));

app.get('/api/health', (req, res) => res.json({
  status: 'ok',
  version: API_VERSION,
  contractTypeId,
  subscriptionTypeId,
  superAdminEmails: SUPER_ADMIN_EMAILS,
}));

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

    const metrics = calcMetrics(subscriptions);
    const contractProps = contract.properties || {};
    const shouldUpdateRollups =
      (parseFloat(contractProps.total_arr) || 0) !== metrics.total_arr ||
      (parseFloat(contractProps.total_tcv) || 0) !== metrics.total_tcv ||
      (parseFloat(contractProps.lq_arr) || 0) !== metrics.lq_arr ||
      (parseFloat(contractProps.fcm_arr) || 0) !== metrics.fcm_arr ||
      (parseInt(contractProps.subscription_count, 10) || 0) !== metrics.subscription_count;

    if (shouldUpdateRollups) {
      await updateObject(contractTypeId, contractId, {
        total_arr: String(metrics.total_arr),
        total_tcv: String(metrics.total_tcv),
        lq_arr: String(metrics.lq_arr),
        fcm_arr: String(metrics.fcm_arr),
        subscription_count: String(metrics.subscription_count),
      });
      contract.properties = {
        ...contractProps,
        total_arr: String(metrics.total_arr),
        total_tcv: String(metrics.total_tcv),
        lq_arr: String(metrics.lq_arr),
        fcm_arr: String(metrics.fcm_arr),
        subscription_count: String(metrics.subscription_count),
      };
    }

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    let company = null;
    if (companyIds.length > 0) {
      try {
        company = await getObject('0-2', companyIds[0], ['name', 'domain', 'city', 'state', 'country']);
      } catch (e) { /* non-critical */ }
    }

    const dealIds = await getAssociatedIds(contractTypeId, contractId, '0-3');
    const dealsMeta = {
      totalAssociatedDeals: dealIds.length,
      loadedDealsCount: 0,
      loadSkipped: false,
      loadLimit: CARD_DEAL_LOAD_LIMIT,
    };
    const deals = [];
    if (dealIds.length > CARD_DEAL_LOAD_LIMIT) {
      dealsMeta.loadSkipped = true;
      console.log(`[load-contract] Skipping deal payload for contract ${contractId}. Associated deals: ${dealIds.length}, limit: ${CARD_DEAL_LOAD_LIMIT}`);
    } else if (dealIds.length > 0) {
      const dealFetches = dealIds.map((did) => getObject('0-3', did, ['dealname', 'dealstage', 'amount', 'closedate', 'deal_category', 'pipeline']));
      const dealResults = await Promise.allSettled(dealFetches);
      for (const result of dealResults) {
        if (result.status !== 'fulfilled') continue;
        const d = result.value;
        deals.push({
          id: d.id,
          name: d.properties.dealname,
          stage: d.properties.dealstage,
          amount: d.properties.amount,
          arr: d.properties.amount,
          closeDate: d.properties.closedate,
          category: d.properties.deal_category,
          pipeline: d.properties.pipeline,
        });
      }
      dealsMeta.loadedDealsCount = deals.length;
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
      dealsMeta,
      contacts,
      portalId,
      productRegistry: PRODUCT_REGISTRY,
      superAdminEmails: SUPER_ADMIN_EMAILS,
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
      contract_end_date: getContractEndDate(props),
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
      total_arr: '0',
      lq_arr: '0',
      fcm_arr: '0',
    });

    if (subscriptionTypeId) {
      const subIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
      for (const sid of subIds) {
        try {
          await updateObject(subscriptionTypeId, sid, { status: 'terminated' });
        } catch (e) { console.warn(`[terminate] Sub ${sid}:`, e.message); }
      }
    }

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    const companyRollups = [];
    for (const companyId of companyIds) {
      try {
        companyRollups.push(await syncCompanyArrRollups(companyId));
      } catch (e) {
        console.warn(`[terminate] Company ARR sync failed for company ${companyId}:`, e.message);
      }
    }

    res.json({
      success: true,
      message: 'Contract terminated',
      companyRollups,
    });
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

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    for (const companyId of companyIds) {
      try {
        await syncCompanyArrRollups(companyId);
      } catch (e) {
        console.warn(`[reverse-termination] Company ARR sync failed for company ${companyId}:`, e.message);
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

// ── Helper: Create a renewal deal for a given contract ───────────────────────
// Shared by:
//   - /api/create-renewal-deal (manual click from the contract card)
//   - /api/update-contract-from-deal (auto-spawn on Closed Won for new business
//     and renewal deals; per Apr 28 training: renewals are now generated
//     immediately on Closed Won so the next-cycle deal shows in the forecast,
//     not at end-of-term + 1)
// Returns { success, dealId, dealName, lineItemsCreated, warnings, existing? }.
async function createRenewalDealForContract(contractId, options = {}) {
  const {
    nameSuffix = 'Renewal',           // e.g. 'Renewal', 'Auto-Renewal'
    closeDateOverride = null,         // close date should = current contract end date for forecasting
    contractPropsOverride = null,     // optional pre-fetched props to avoid an extra GET
    skipIfOpenRenewalExists = true,
    pipeline = 'default',
    dealStage = 'appointmentscheduled',
  } = options;

  await resolveTypeIds();

  const props = contractPropsOverride
    || (await getObject(contractTypeId, contractId, CONTRACT_PROPS)).properties;

  const currentEndDate = parseDateValue(getContractEndDate(props)) || new Date();
  const renewalStart = new Date(currentEndDate);
  renewalStart.setDate(renewalStart.getDate() + 1);
  const renewalEnd = new Date(renewalStart);
  renewalEnd.setFullYear(renewalEnd.getFullYear() + 1);

  const dealName = `${props.contract_name || 'Contract'} — ${nameSuffix}`;
  const warnings = [];

  if (skipIfOpenRenewalExists) {
    const existingDealIds = await getAssociatedIds(contractTypeId, contractId, '0-3');
    if (existingDealIds.length > 0) {
      const existingDeals = await Promise.allSettled(
        existingDealIds.map((id) =>
          getObject('0-3', id, ['dealname', 'dealstage', 'deal_category', 'amount', 'closedate'])
        )
      );
      const openRenewal = existingDeals
        .filter((r) => r.status === 'fulfilled')
        .map((r) => r.value)
        .find((d) => {
          const category = String(d?.properties?.deal_category || '').toLowerCase();
          const stage = String(d?.properties?.dealstage || '').toLowerCase();
          return category === 'renewal' && stage !== 'closedwon' && stage !== 'closedlost';
        });
      if (openRenewal) {
        console.log(
          `[renewal-helper] Open renewal deal already exists for contract ${contractId}: ${openRenewal.id}. Reusing.`
        );
        return {
          success: true,
          existing: true,
          dealId: openRenewal.id,
          dealName: openRenewal.properties?.dealname || dealName,
          lineItemsCreated: 0,
          warnings: ['An open renewal deal already existed; no new deal was created.'],
        };
      }
    }
  }

  // Per Apr 28 training: close date = current contract end date so the
  // renewal deal lands in the right forecast quarter even though it was
  // created at Closed Won of the prior term.
  const closeDate = closeDateOverride
    || fmtDateForHS(parseDateValue(getContractEndDate(props)) || renewalStart);

  const deal = await createObject('0-3', {
    dealname: dealName,
    dealstage: dealStage,
    deal_category: 'renewal',
    contract_start_date: fmtDateForHS(renewalStart),
    contract_end_date: fmtDateForHS(renewalEnd),
    closedate: closeDate,
    amount: props.total_arr || '0',
    pipeline,
  });

  const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
  if (companyIds.length > 0) {
    try {
      await createAssociation('0-3', deal.id, '0-2', companyIds[0]);
    } catch (e) {
      console.warn('[renewal-helper] Company association failed:', e.response?.data?.message || e.message);
      warnings.push('Company association failed — some contacts on this company may be invalid');
    }
  }

  try {
    await createAssociation(contractTypeId, contractId, '0-3', deal.id);
  } catch (e) {
    console.warn('[renewal-helper] Could not associate deal to contract:', e.message);
  }

  const contactIds = await getAssociatedIds(contractTypeId, contractId, '0-1');
  let contactsLinked = 0;
  for (const cid of contactIds) {
    try {
      await createAssociation('0-3', deal.id, '0-1', cid);
      contactsLinked++;
    } catch (e) {
      console.warn(`[renewal-helper] Skipping invalid contact ${cid}:`, e.response?.data?.message || e.message);
    }
  }
  if (contactIds.length > 0 && contactsLinked < contactIds.length) {
    warnings.push(`${contactIds.length - contactsLinked} of ${contactIds.length} contact associations failed (likely deleted contacts)`);
  }

  const seeded = await syncContractRecurringLineItemsToDeal(contractId, deal.id, {
    fallbackRevenueType: 'renewal',
  });
  const lineItemsCreated = seeded.lineItemsCreated + seeded.lineItemsUpdated;
  if (seeded.warnings?.length) warnings.push(...seeded.warnings);

  if (lineItemsCreated === 0) {
    warnings.unshift(
      `No line items were seeded on the renewal deal (source: ${seeded.inheritanceSource}). ` +
      `Check that the contract has subscription segments with status active/future or recurring contract line items.`
    );
  }

  console.log(
    `[renewal-helper] Created renewal deal ${deal.id} for contract ${contractId}: ` +
    `${seeded.lineItemsCreated} created, ${seeded.lineItemsUpdated} updated, ` +
    `${seeded.lineItemsRemoved} removed, closeDate=${closeDate} ` +
    `(${fmtDateForHS(renewalStart)} → ${fmtDateForHS(renewalEnd)})`
  );

  return {
    success: true,
    existing: false,
    dealId: deal.id,
    dealName,
    contactsLinked,
    lineItemsCreated,
    lineItemsFailed: seeded.lineItemsFailed || 0,
    inheritanceSource: seeded.inheritanceSource,
    renewalStart: fmtDateForHS(renewalStart),
    renewalEnd: fmtDateForHS(renewalEnd),
    closeDate,
    warnings,
  };
}

// ── Route: Create Renewal Deal ───────────────────────────────────────────────

app.get('/api/create-renewal-deal', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) return res.status(400).json({ success: false, message: 'contractId required' });

    const result = await createRenewalDealForContract(contractId, {
      nameSuffix: 'Renewal',
    });

    if (result.existing) {
      return res.json({
        success: true,
        message: `An open renewal deal already exists for this contract: ${result.dealName}. Reusing it instead of creating a duplicate.`,
        dealId: result.dealId,
        dealName: result.dealName,
        existing: true,
        warnings: result.warnings,
      });
    }

    res.json({
      success: true,
      message: result.lineItemsCreated > 0
        ? `Renewal deal created with ${result.lineItemsCreated} line item(s): ${result.renewalStart} → ${result.renewalEnd}`
        : `Renewal deal created (no line items seeded): ${result.renewalStart} → ${result.renewalEnd}`,
      dealId: result.dealId,
      dealName: result.dealName,
      contactsLinked: result.contactsLinked,
      lineItemsCreated: result.lineItemsCreated,
      lineItemsFailed: result.lineItemsFailed,
      inheritanceSource: result.inheritanceSource,
      closeDate: result.closeDate,
      warnings: result.warnings && result.warnings.length > 0 ? result.warnings : undefined,
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

    const currentEnd = parseDateValue(getContractEndDate(props)) || new Date();
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
      properties: ['contract_name', 'auto_renewal_date', 'auto_renewal_released', 'status', 'end_date', 'co_term_date', 'total_arr'],
      limit: 100,
    });

    const candidates = (data.results || []).filter(
      (c) => c.properties.auto_renewal_released !== 'true'
    );

    const results = [];
    for (const c of candidates) {
      try {
        const cp = c.properties;
        const currentEnd = parseDateValue(getContractEndDate(cp)) || new Date();
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
        const rawStatus = cp.status;
        const derivedStatus = determineStatus(cp.start_date, cp.end_date);
        const status =
          rawStatus === 'terminated' || rawStatus === 'draft' || rawStatus === 'in_approval_process'
            ? rawStatus
            : derivedStatus;

        let arr = parseFloat(cp.total_arr) || 0;
        let tcv = parseFloat(cp.total_tcv) || 0;
        let lqA = parseFloat(cp.lq_arr) || 0;
        let fcmA = parseFloat(cp.fcm_arr) || 0;

        if (subscriptionTypeId) {
          const subIds = await getAssociatedIds(contractTypeId, cid, subscriptionTypeId);
          if (subIds.length > 0) {
            const subs = [];
            for (const sid of subIds) {
              try {
                subs.push(await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS));
              } catch (e) { /* skip inaccessible */ }
            }
            if (subs.length > 0) {
              const metrics = calcMetrics(subs);
              arr = metrics.total_arr;
              tcv = metrics.total_tcv;
              lqA = metrics.lq_arr;
              fcmA = metrics.fcm_arr;
            }
          }
        }

        const mrr = arr / 12;

        ltv += tcv;

        if (status === 'active') {
          activeContracts++;
          totalArr += arr;
          totalMrr += mrr;
          totalTcv += tcv;
          lqArr += lqA;
          fcmArr += fcmA;
        }

        const products = [];
        if (lqA > 0) products.push({ name: 'LeaseQuery', code: 'LQ', arr: lqA });
        if (fcmA > 0) products.push({ name: 'FCM', code: 'FCM', arr: fcmA });

        contracts.push({
          id: c.id,
          objectTypeId: contractTypeId,
          name: cp.contract_name,
          number: cp.contract_number,
          status,
          rawStatus,
          arr,
          mrr,
          tcv,
          startDate: cp.start_date || null,
          endDate: cp.end_date || cp.co_term_date || null,
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
      superAdminEmails: SUPER_ADMIN_EMAILS,
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
        const year = parseInt(item?.year, 10);
        const safeYear = Number.isFinite(year) && year > 0 ? year : null;
        const baseKey = sanitizeKey(
          item?.key || item?.productCode || item?.productName,
          `product-${index + 1}`
        );
        const key = safeYear ? `${baseKey}-y${safeYear}` : baseKey;
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
          year: safeYear,
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
        description: `FinQuery CPQ demo line item | FQ_DEMO_PRODUCT_KEY:${item.key}${item.year ? ` | FQ_DEMO_YEAR:${item.year}` : ''}`,
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
      message: `Demo product/year line items synced (${created} created, ${updated} updated, ${removed} removed)`,
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
    const syncCompanyArrForIds = async (companyIds = []) => {
      const uniqueIds = [...new Set((companyIds || []).filter(Boolean))];
      const results = [];
      for (const companyId of uniqueIds) {
        try {
          results.push(await syncCompanyArrRollups(companyId));
        } catch (e) {
          console.warn(`[update-contract] Company ARR sync failed for company ${companyId}:`, e.message);
        }
      }
      return results;
    };

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
    // Creates ONE segment per recurring line item per contract year. One-time
    // line items live on the contract as line items only -- they are NOT
    // segments. Multi-year contracts get N segments per recurring product (one
    // per year), matching the seed pattern (e.g. "LQ Year 1", "LQ Year 2").
    async function createSegmentsFromLineItems(cId, lineItems, opts = {}) {
      const {
        startDate,
        endDate,
        revenueType,
        amendmentIndicator,
        dealCategory,
        companyId,
        contractName,
      } = opts;

      const recurringItems = lineItems.filter(isRecurringLineItem);
      const yearSegments = buildYearSegments(startDate, endDate);
      const cleanContractName = String(contractName || 'Contract').trim() || 'Contract';
      let created = 0;
      let totalArr = 0;

      for (const [liIndex, li] of recurringItems.entries()) {
        const lp = li.properties || {};
        const qty = parseInt(lp.quantity) || 1;
        const unitPrice = parseFloat(lp.price) || 0;
        const annualArr = unitPrice * qty;
        const productName = String(lp.name || 'Product').trim() || 'Product';
        const productCode = deriveProductCode(lp);
        const lineRevType = normalizeLineRevenueType(lp.revenue_type, revenueType || '');

        // Resolve amendment_indicator once per line item (constant across years).
        let perLineIndicator = null;
        if (amendmentIndicator) {
          let indicator = amendmentIndicator;
          if (dealCategory === 'amendment') {
            const liRev = normalizeLineRevenueType(lp.revenue_type, 'renewal');
            if (liRev === 'expansion' || liRev === 'cross_sell' || liRev === 'new') {
              indicator = 'Expansion';
            } else if (liRev === 'contraction') {
              indicator = 'Contraction';
            } else {
              indicator = 'Renewal';
            }
          }
          if (dealCategory === 'contraction') {
            const liRev = (lp.revenue_type || '').toLowerCase();
            if (liRev === 'expansion' || liRev === 'cross_sell') {
              indicator = 'Expansion';
            }
          }
          perLineIndicator = indicator;
        }

        for (const [yearIdx, segYear] of yearSegments.entries()) {
          totalArr += annualArr;

          const segProps = {
            segment_name: `${cleanContractName} — ${productCode} Year ${segYear.year}`,
            product_name: productName,
            product_code: productCode,
            quantity: String(qty),
            original_quantity: String(qty),
            unit_price: String(unitPrice),
            arr: String(annualArr),
            mrr: String(annualArr / 12),
            tcv: String(annualArr),
            status: determineStatus(segYear.start_date, segYear.end_date),
            start_date: segYear.start_date,
            end_date: segYear.end_date,
            subscription_start_date: startDate,
            subscription_end_date: endDate,
            arr_start_date: segYear.start_date,
            arr_end_date: segYear.end_date,
            segment_year: String(segYear.year),
            segment_index: String(yearIdx + 1),
            segment_label: `Year ${segYear.year}`,
            segment_key: `${cId}-${liIndex + 1}-${yearIdx + 1}`,
            billing_frequency: 'annual',
            charge_type: 'recurring',
            revenue_type: lineRevType,
          };

          if (perLineIndicator) segProps.amendment_indicator = perLineIndicator;

          const seg = await createObject(subscriptionTypeId, segProps);
          try { await createAssociation(subscriptionTypeId, seg.id, contractTypeId, cId); } catch (e) { /* ok */ }
          if (companyId) {
            try { await createAssociation(subscriptionTypeId, seg.id, '0-2', companyId); } catch (e) { /* ok */ }
          }
          created++;
          console.log(`[update-contract] Created segment ${seg.id}: ${productName} Year ${segYear.year} x${qty} @ ${unitPrice}`);
        }
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

      const newBusinessContractName = (dp.dealname || 'Contract')
        .replace(' — New Business', '')
        .replace(' - New Business', '');

      const contract = await createObject(contractTypeId, {
        contract_name: newBusinessContractName,
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
            contractName: newBusinessContractName,
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

      const companyRollups = await syncCompanyArrForIds(companyId ? [companyId] : []);

      // Per Apr 28 training: spawn the next-cycle renewal deal IMMEDIATELY on
      // Closed Won so it lands in the forecast for the contract's end-date
      // quarter. Only fires for new_business + renewal — amendments do NOT
      // generate renewal deals on close.
      let renewalDeal = null;
      try {
        renewalDeal = await createRenewalDealForContract(contract.id, {
          nameSuffix: 'Renewal',
        });
      } catch (renewalErr) {
        console.warn(
          `[update-contract] Auto-renewal deal creation failed for new contract ${contract.id}:`,
          renewalErr.response?.data?.message || renewalErr.message
        );
      }

      return res.json({
        success: true,
        message: `New contract created with ${segmentsCreated} subscription segments and ${contractLineItems} line items`
          + (renewalDeal?.dealId ? `; renewal deal ${renewalDeal.dealId} auto-created` : ''),
        contractId: contract.id,
        segmentsCreated,
        contractLineItems,
        companyRollups,
        renewalDeal,
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
              contractName,
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

        const companyRollups = await syncCompanyArrForIds(companyId ? [companyId] : []);

        // Per Apr 28 training: when a renewal closes won, immediately spawn the
        // NEXT-cycle renewal deal so the next-term forecast is populated. Close
        // date = the new contract's end date for accurate quarterly forecasting.
        let nextRenewalDeal = null;
        try {
          nextRenewalDeal = await createRenewalDealForContract(newContract.id, {
            nameSuffix: 'Renewal',
          });
        } catch (renewalErr) {
          console.warn(
            `[update-contract] Auto-renewal deal creation failed for renewed contract ${newContract.id}:`,
            renewalErr.response?.data?.message || renewalErr.message
          );
        }

        return res.json({
          success: true,
          message: `Renewal contract created: ${renewalStart} → ${renewalEnd} (${segmentsCreated} segments, ${contractLineItemsCopied} line items). Old contract ${cid} expired.`
            + (nextRenewalDeal?.dealId ? ` Next-cycle renewal deal ${nextRenewalDeal.dealId} auto-created.` : ''),
          contractId: newContract.id,
          oldContractId: cid,
          segmentsCreated,
          contractLineItems: contractLineItemsCopied,
          companyRollups,
          renewalDeal: nextRenewalDeal,
        });
      }

      // ── AMENDMENT / EXPANSION / CONTRACTION ──────────────────────────
      if (category === 'amendment' || category === 'expansion' || category === 'contraction') {
        const lineItems = await getDealLineItems();
        const companyIds = await getAssociatedIds(contractTypeId, cid, '0-2');
        const companyId = companyIds.length > 0 ? companyIds[0] : null;

        const amendStartDate = dp.contract_start_date || cp.amendment_start_date || fmtDateForHS(new Date());
        const amendEndDate = dp.contract_end_date || getContractEndDate(cp);

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
              contractName: cp.contract_name || 'Contract',
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
        const companyRollups = await syncCompanyArrForIds(companyId ? [companyId] : []);

        const terminated = updates.status === 'terminated';
        return res.json({
          success: true,
          message: terminated
            ? `Contract ${cid} auto-terminated (zero ARR after ${category})`
            : `Contract ${cid} updated: ${segmentsCreated} segments added from ${category}`,
          contractId: cid,
          segmentsCreated,
          autoTerminated: terminated,
          companyRollups,
        });
      }

      // ── FALLBACK for other categories ────────────────────────────────
      const newStatus = determineStatus(cp.start_date, cp.end_date);
      updates.status = newStatus;

      if (Object.keys(updates).length > 0) {
        await updateObject(contractTypeId, cid, updates);
      }

      const fallbackCompanyIds = await getAssociatedIds(contractTypeId, cid, '0-2');
      const companyRollups = await syncCompanyArrForIds(fallbackCompanyIds);

      return res.json({
        success: true,
        message: `Contract ${cid} updated from deal`,
        contractId: cid,
        companyRollups,
      });
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
          product_name: 'FinQuery Contract Management',
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
          product_name: 'FinQuery Contract Management',
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

// ── Route: Sweep Contract Statuses (date-based, batch) ──────────────────────
// Per Apr 28 training:
//   - Migrated/historical contracts pull status directly from Salesforce
//     `activated` field, which never re-evaluates as time passes.
//   - This sweep walks every non-terminal contract and applies the date-based
//     status rule (future / active / expired) so future-dated migrated contracts
//     correctly land as `future` instead of stuck `active`.
// Manual statuses (terminated, draft, in_approval_process) are NEVER touched.
//
// Optional query params:
//   - limit (number) default 200, max 500
//   - dryRun=true  to preview changes without writing

app.get('/api/sweep-contract-statuses', async (req, res) => {
  try {
    await resolveTypeIds();
    if (!contractTypeId) {
      return res.status(500).json({ success: false, message: 'Contract schema not found' });
    }

    const requestedLimit = parseInt(req.query.limit, 10);
    const limit = Number.isFinite(requestedLimit) && requestedLimit > 0
      ? Math.min(requestedLimit, 500)
      : 200;
    const dryRun = String(req.query.dryRun || '').toLowerCase() === 'true';

    const results = [];
    let after = undefined;
    let scanned = 0;
    let updated = 0;
    let skippedManual = 0;
    let unchanged = 0;
    let pageCount = 0;
    const MAX_PAGES = 25;

    while (pageCount < MAX_PAGES && scanned < limit) {
      pageCount += 1;
      const pageSize = Math.min(100, limit - scanned);
      const body = {
        filterGroups: [],
        properties: ['contract_name', 'status', 'start_date', 'end_date', 'co_term_date'],
        limit: pageSize,
      };
      if (after) body.after = after;

      const { data } = await hs.post(`/crm/v3/objects/${contractTypeId}/search`, body);
      const page = data.results || [];
      if (page.length === 0) break;

      for (const c of page) {
        scanned++;
        const cp = c.properties || {};
        const currentStatus = cp.status || '';

        // Manual statuses are not touched by date sweeps.
        if (
          currentStatus === 'terminated' ||
          currentStatus === 'draft' ||
          currentStatus === 'in_approval_process'
        ) {
          skippedManual++;
          continue;
        }

        const newStatus = determineStatus(cp.start_date, getContractEndDate(cp));
        if (!newStatus || newStatus === currentStatus) {
          unchanged++;
          continue;
        }

        if (!dryRun) {
          try {
            const updates = { status: newStatus };
            if (newStatus === 'active' && !cp.activated_date) {
              updates.activated_date = fmtDateForHS(new Date());
            }
            await updateObject(contractTypeId, c.id, updates);
            updated++;
          } catch (e) {
            console.warn(`[sweep-statuses] Failed to update contract ${c.id}:`, e.response?.data?.message || e.message);
            results.push({
              contractId: c.id,
              name: cp.contract_name,
              from: currentStatus,
              to: newStatus,
              error: e.response?.data?.message || e.message,
            });
            continue;
          }
        } else {
          updated++;
        }

        results.push({
          contractId: c.id,
          name: cp.contract_name,
          from: currentStatus,
          to: newStatus,
          startDate: cp.start_date,
          endDate: cp.end_date,
        });
      }

      after = data.paging?.next?.after;
      if (!after) break;
    }

    res.json({
      success: true,
      message: dryRun
        ? `[dry run] Would update ${updated} contract(s) (scanned ${scanned}; ${skippedManual} skipped manual statuses; ${unchanged} unchanged)`
        : `Updated ${updated} contract(s) (scanned ${scanned}; ${skippedManual} skipped manual statuses; ${unchanged} unchanged)`,
      dryRun,
      scanned,
      updated,
      skippedManual,
      unchanged,
      changes: results,
    });
  } catch (e) {
    console.error('[sweep-contract-statuses] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Load Test Data (single contract) ──────────────────────────────────
// Admin-only convenience endpoint used by the "Load Test Data" button on the
// contract card. Wraps the seeded subscription generator so demo + sandbox
// contracts can get realistic year-by-year segments without running a CLI.

app.get('/api/load-test-data', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) {
      return res.status(400).json({ success: false, message: 'contractId required' });
    }

    await resolveTypeIds();
    if (!contractTypeId || !subscriptionTypeId) {
      return res.status(500).json({
        success: false,
        message: 'Schemas not ready — hit /api/ensure-setup first',
      });
    }

    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const cp = contract.properties || {};

    const existingSubIds = await getAssociatedIds(contractTypeId, contractId, subscriptionTypeId);
    if (existingSubIds.length > 0) {
      return res.json({
        success: false,
        message: `Contract already has ${existingSubIds.length} subscription segment(s). Remove them first to re-seed.`,
        skipped: true,
        existingCount: existingSubIds.length,
      });
    }

    const startDate = cp.start_date || fmtDateForHS(new Date());
    const endDate = getContractEndDate(cp)
      || fmtDateForHS((() => { const d = parseDateValue(startDate) || new Date(); d.setFullYear(d.getFullYear() + 3); return d; })());

    // 3-year LeaseQuery + 2-year FCM (matches the seed-subscriptions pattern
    // used for the Meridian demo dataset). Year 1 LQ is intentionally
    // `inactive` so the card has at least one historical segment to show.
    const yearSegments = buildYearSegments(startDate, endDate);
    if (yearSegments.length === 0) {
      return res.status(400).json({ success: false, message: 'Could not derive year segments from contract dates' });
    }

    const cleanContractName = (cp.contract_name || 'Contract').trim() || 'Contract';
    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    const companyId = companyIds[0] || null;

    const seedTemplates = [
      { code: 'LQ',  name: 'LeaseQuery',                quantity: 200, unitPrice: 500, listPrice: 600, uplift: 3 },
      { code: 'FCM', name: 'FinQuery Contract Management', quantity: 1, unitPrice: 55000, listPrice: 65000, uplift: 3 },
    ];

    const created = [];
    for (const template of seedTemplates) {
      for (const [yearIdx, segYear] of yearSegments.entries()) {
        const upliftMultiplier = Math.pow(1 + (template.uplift / 100), yearIdx);
        const unitPrice = +(template.unitPrice * upliftMultiplier).toFixed(2);
        const annualArr = +(unitPrice * template.quantity).toFixed(2);
        const today = fmtDateForHS(new Date());
        let status = determineStatus(segYear.start_date, segYear.end_date);
        if (template.code === 'LQ' && yearIdx === 0 && segYear.end_date < today) {
          status = 'inactive';
        }

        try {
          const seg = await createObject(subscriptionTypeId, {
            segment_name: `${cleanContractName} — ${template.code} Year ${segYear.year}`,
            subscription_number: `SUB-${contractId}-${template.code}-${String(segYear.year).padStart(2, '0')}`,
            product_code: template.code,
            product_name: template.name,
            product_subscription_type: 'renewable',
            subscription_type: 'renewable',
            charge_type: 'recurring',
            billing_frequency: 'annual',
            status,
            segment_year: String(segYear.year),
            segment_label: `Year ${segYear.year}`,
            segment_index: String(yearIdx + 1),
            segment_uplift: yearIdx > 0 ? String(template.uplift) : undefined,
            start_date: segYear.start_date,
            end_date: segYear.end_date,
            segment_start_date: segYear.start_date,
            segment_end_date: segYear.end_date,
            arr_start_date: segYear.start_date,
            arr_end_date: segYear.end_date,
            subscription_start_date: startDate,
            subscription_end_date: endDate,
            quantity: String(template.quantity),
            original_quantity: String(template.quantity),
            unit_price: String(unitPrice),
            list_price: String(template.listPrice),
            net_price: String(annualArr),
            regular_price: String(unitPrice),
            customer_price: String(unitPrice),
            prorate_multiplier: '1',
            pricing_method: 'list',
            arr: String(annualArr),
            mrr: String(+(annualArr / 12).toFixed(2)),
            tcv: String(annualArr),
            revenue_type: yearIdx === 0 ? 'new' : 'renewal',
          });
          try { await createAssociation(subscriptionTypeId, seg.id, contractTypeId, contractId); } catch (e) { /* ok */ }
          if (companyId) {
            try { await createAssociation(subscriptionTypeId, seg.id, '0-2', companyId); } catch (e) { /* ok */ }
          }
          created.push(seg.id);
        } catch (e) {
          console.warn(`[load-test-data] Segment create failed for ${template.code} Year ${segYear.year}:`, e.response?.data?.message || e.message);
        }
      }
    }

    // Refresh contract rollups so the card immediately reflects the new ARR.
    const subs = [];
    for (const sid of created) {
      try { subs.push(await getObject(subscriptionTypeId, sid, SUBSCRIPTION_PROPS)); } catch (e) { /* skip */ }
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
      message: `Loaded ${created.length} test subscription segment(s) across ${yearSegments.length} year(s)`,
      contractId,
      segmentsCreated: created.length,
      yearsSeeded: yearSegments.length,
      totalArr: metrics.total_arr,
    });
  } catch (e) {
    console.error('[load-test-data] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Load NetSuite Test Data ───────────────────────────────────────────
// Admin-only convenience endpoint that simulates a NetSuite sync by stamping a
// fake NetSuite ID + billing address onto the contract. Returns synchronously
// AFTER all writes complete so the card can refresh and immediately show the
// new fields without a 10s polling delay (per Apr 28 bug report).

app.get('/api/load-netsuite-test-data', async (req, res) => {
  try {
    const { contractId } = req.query;
    if (!contractId) {
      return res.status(400).json({ success: false, message: 'contractId required' });
    }

    await resolveTypeIds();
    if (!contractTypeId) {
      return res.status(500).json({ success: false, message: 'Contract schema not found' });
    }

    const contract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);
    const cp = contract.properties || {};

    if (cp.netsuite_id) {
      return res.json({
        success: true,
        message: `NetSuite ID already set: ${cp.netsuite_id}. Clear it first to reseed.`,
        skipped: true,
        netsuiteId: cp.netsuite_id,
      });
    }

    // Deterministic fake ID so re-runs on the same contract produce the same
    // value. NetSuite IDs are typically 6-7 digit numbers.
    const numericContractId = String(contractId).replace(/\D/g, '') || '0';
    const fakeNsId = String((parseInt(numericContractId.slice(-6), 10) || 100000) + 4500000);

    const updates = {
      netsuite_id: fakeNsId,
      billing_street: cp.billing_street || '1180 Peachtree St NE',
      billing_city: cp.billing_city || 'Atlanta',
      billing_state: cp.billing_state || 'Georgia',
      billing_postal_code: cp.billing_postal_code || '30309',
      billing_country: cp.billing_country || 'United States',
    };

    // Single PATCH so the card sees all updates after the response resolves —
    // no client-side polling needed.
    await updateObject(contractTypeId, contractId, updates);

    // Re-fetch so the response carries the post-update state. Card can use
    // `updatedContract.properties` directly to populate UI without a follow-up
    // load-contract call.
    const updatedContract = await getObject(contractTypeId, contractId, CONTRACT_PROPS);

    res.json({
      success: true,
      message: `NetSuite test data loaded: ID ${fakeNsId}`,
      contractId,
      netsuiteId: fakeNsId,
      billingAddress: {
        street: updates.billing_street,
        city: updates.billing_city,
        state: updates.billing_state,
        postalCode: updates.billing_postal_code,
        country: updates.billing_country,
      },
      updatedContract,
    });
  } catch (e) {
    console.error('[load-netsuite-test-data] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
  }
});

// ── Route: Aggregate Contract Reports ────────────────────────────────────────
// Cohort views over the contract object + child subscription segments. Powers
// the Betty / Scott / Amy demo dashboards. Returns JSON ready to feed a
// HubSpot Custom Report or a Snowflake-style export.
//
// Cohort buckets:
//   - startedThisQuarter: contracts whose start_date falls in the current quarter
//   - endingThisQuarter: contracts whose end_date falls in the current quarter
//   - byArrBand: contracts grouped by Year 1 ARR band (<10K, 10–50K, 50–100K, 100K–250K, 250K+)
//   - yearByYear: contracts joined to their subscription segments, summarised by segment_year
//
// Optional query params:
//   - quarterStart, quarterEnd (YYYY-MM-DD) to override the current quarter window
//   - limit (number) default 500, max 2000 — total contracts scanned

app.get('/api/reports/contract-cohorts', async (req, res) => {
  try {
    await resolveTypeIds();
    if (!contractTypeId) {
      return res.status(500).json({ success: false, message: 'Contract schema not found' });
    }

    const requestedLimit = parseInt(req.query.limit, 10);
    const totalLimit = Number.isFinite(requestedLimit) && requestedLimit > 0
      ? Math.min(requestedLimit, 2000)
      : 500;

    // Compute the active quarter window unless overridden.
    const today = new Date();
    const currentQuarterStart = req.query.quarterStart
      ? parseDateValue(req.query.quarterStart)
      : new Date(today.getFullYear(), Math.floor(today.getMonth() / 3) * 3, 1);
    const currentQuarterEnd = req.query.quarterEnd
      ? parseDateValue(req.query.quarterEnd)
      : new Date(currentQuarterStart.getFullYear(), currentQuarterStart.getMonth() + 3, 0);

    const ARR_BANDS = [
      { label: '< $10K',          min: 0,       max: 10000 },
      { label: '$10K – $50K',     min: 10000,   max: 50000 },
      { label: '$50K – $100K',    min: 50000,   max: 100000 },
      { label: '$100K – $250K',   min: 100000,  max: 250000 },
      { label: '$250K +',         min: 250000,  max: Number.POSITIVE_INFINITY },
    ];

    const arrBandSummary = ARR_BANDS.map((band) => ({
      label: band.label,
      contractCount: 0,
      totalArr: 0,
      contractIds: [],
    }));

    const startedThisQuarter = [];
    const endingThisQuarter = [];
    const yearByYearTotals = {}; // { [year]: { contractCount, totalArr, totalTcv, lqArr, fcmArr } }

    let after = undefined;
    let scanned = 0;
    let pageCount = 0;
    const MAX_PAGES = 30;

    const inWindow = (dateStr, windowStart, windowEnd) => {
      const d = parseDateValue(dateStr);
      if (!d || !windowStart || !windowEnd) return false;
      return d >= windowStart && d <= windowEnd;
    };

    while (pageCount < MAX_PAGES && scanned < totalLimit) {
      pageCount += 1;
      const pageSize = Math.min(100, totalLimit - scanned);
      const body = {
        filterGroups: [],
        properties: [
          'contract_name', 'contract_number', 'status',
          'start_date', 'end_date', 'co_term_date',
          'total_arr', 'total_tcv', 'lq_arr', 'fcm_arr',
          'subscription_count',
        ],
        limit: pageSize,
      };
      if (after) body.after = after;

      const { data } = await hs.post(`/crm/v3/objects/${contractTypeId}/search`, body);
      const page = data.results || [];
      if (page.length === 0) break;

      for (const c of page) {
        scanned++;
        const cp = c.properties || {};
        const totalArr = parseFloat(cp.total_arr) || 0;
        const endDate = getContractEndDate(cp);

        const summary = {
          id: c.id,
          name: cp.contract_name,
          number: cp.contract_number,
          status: cp.status,
          startDate: cp.start_date,
          endDate,
          totalArr,
          totalTcv: parseFloat(cp.total_tcv) || 0,
          lqArr: parseFloat(cp.lq_arr) || 0,
          fcmArr: parseFloat(cp.fcm_arr) || 0,
        };

        if (inWindow(cp.start_date, currentQuarterStart, currentQuarterEnd)) {
          startedThisQuarter.push(summary);
        }
        if (inWindow(endDate, currentQuarterStart, currentQuarterEnd)) {
          endingThisQuarter.push(summary);
        }

        const band = ARR_BANDS.findIndex((b) => totalArr >= b.min && totalArr < b.max);
        if (band >= 0) {
          arrBandSummary[band].contractCount += 1;
          arrBandSummary[band].totalArr += totalArr;
          if (arrBandSummary[band].contractIds.length < 25) {
            arrBandSummary[band].contractIds.push(c.id);
          }
        }
      }

      after = data.paging?.next?.after;
      if (!after) break;
    }

    // Year-by-year summary requires walking subscription segments. Cap the
    // contracts we drill into so a portal with thousands of contracts does
    // not blow the 30s Railway request budget. Prefer the contracts we have
    // already loaded via the cohort scans.
    const drillContractIds = Array.from(new Set([
      ...startedThisQuarter.map((c) => c.id),
      ...endingThisQuarter.map((c) => c.id),
    ])).slice(0, 100);

    if (subscriptionTypeId && drillContractIds.length > 0) {
      for (const cid of drillContractIds) {
        try {
          const subIds = await getAssociatedIds(contractTypeId, cid, subscriptionTypeId);
          if (subIds.length === 0) continue;
          const subs = await Promise.all(
            subIds.map((id) =>
              getObject(subscriptionTypeId, id, [
                'segment_year', 'arr', 'tcv', 'product_code', 'status',
              ])
            )
          );
          for (const sub of subs) {
            const sp = sub.properties || {};
            if (sp.status === 'terminated') continue;
            const year = parseInt(sp.segment_year, 10) || 1;
            const arr = parseFloat(sp.arr) || 0;
            const tcv = parseFloat(sp.tcv) || 0;
            const code = (sp.product_code || '').toUpperCase();

            if (!yearByYearTotals[year]) {
              yearByYearTotals[year] = {
                year,
                segmentCount: 0,
                totalArr: 0,
                totalTcv: 0,
                lqArr: 0,
                fcmArr: 0,
              };
            }
            yearByYearTotals[year].segmentCount += 1;
            yearByYearTotals[year].totalArr += arr;
            yearByYearTotals[year].totalTcv += tcv;
            if (code === 'LQ') yearByYearTotals[year].lqArr += arr;
            if (code === 'FCM') yearByYearTotals[year].fcmArr += arr;
          }
        } catch (e) {
          console.warn(`[reports] Year-by-year drill failed for contract ${cid}:`, e.message);
        }
      }
    }

    res.json({
      success: true,
      message: `Aggregated ${scanned} contract(s) (drilled ${drillContractIds.length} for year-by-year)`,
      generatedAt: new Date().toISOString(),
      window: {
        quarterStart: fmtDateForHS(currentQuarterStart),
        quarterEnd: fmtDateForHS(currentQuarterEnd),
      },
      cohorts: {
        startedThisQuarter: {
          count: startedThisQuarter.length,
          totalArr: startedThisQuarter.reduce((s, c) => s + c.totalArr, 0),
          contracts: startedThisQuarter,
        },
        endingThisQuarter: {
          count: endingThisQuarter.length,
          totalArr: endingThisQuarter.reduce((s, c) => s + c.totalArr, 0),
          contracts: endingThisQuarter,
        },
        byArrBand: arrBandSummary,
        yearByYear: Object.values(yearByYearTotals).sort((a, b) => a.year - b.year),
      },
      scanned,
    });
  } catch (e) {
    console.error('[reports/contract-cohorts] Error:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.message });
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
