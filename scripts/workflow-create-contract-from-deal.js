/**
 * HubSpot Workflow Custom Code Action
 *
 * Purpose:
 * - Input: deal ID
 * - Reads deal + associated line items
 * - Creates one Contract record
 * - Creates supporting Subscription Segment records by engagement year
 * - For new_business / renewal deals ONLY, immediately spawns the next-cycle
 *   renewal deal with closedate = the new contract's end date so the renewal
 *   lands in the correct forecast quarter. Amendment / expansion / contraction
 *   deals never spawn a renewal here (per Apr 28 training session).
 *
 * Renewal deal shape (per May 1 correction):
 * - Term: inherits the FULL term of the source contract (e.g. a 3-year source
 *   spawns a 3-year renewal), not a fixed 1-year window.
 * - Line items: NONE. Renewals are placeholders for DealHub to quote into;
 *   we used to seed final-year recurring lines but that was wrong — the
 *   renewal deal stays empty until DealHub configures it.
 * - Amount: LAST-SEGMENT ARR (single-year run rate at the END of the source
 *   contract). For a source priced Y1=30K, Y2=35K, Y3=40K the renewal
 *   amount is 40K — NOT 40K × renewal years and NOT total TCV. DealHub
 *   replaces this with their own forecast when they quote into the deal.
 *   Products whose final segment ends well before contract end (terminated
 *   mid-contract) are excluded so they don't inflate the figure.
 * - total_tcv on the renewal deal == amount (single-year placeholder until
 *   DealHub's quote rebuilds it from line items).
 * - year_1_arr on the renewal deal == amount (the run rate carried into
 *   renewal Year 1).
 *
 * Workflow trigger requirements (configure in HubSpot UI):
 * - Trigger on Deal stage = Closed Won
 * - Filter OUT amendment / expansion / contraction deals — this action only
 *   runs for new_business + renewal categories. (You can also rely on the
 *   built-in category check below; the filter is for performance / clarity.)
 * - The 120-day "renewal quote" generation logic stays in DealHub and is NOT
 *   what this action does. This action creates the renewal DEAL only.
 *
 * Workflow setup:
 * 1) Add an input field named `dealId` (or pass `hs_object_id` from deal-based workflow)
 * 2) Add secret `PRIVATE_APP_TOKEN` (Private App token)
 * 3) Add output fields:
 *    - success (string)
 *    - contractId (string)
 *    - segmentsCreated (number)
 *    - recurringLineItems (number)
 *    - oneTimeLineItems (number)
 *    - totalArr (number)                  — annualized ARR (one year of recurring revenue)
 *    - totalTcv (number)                  — total contract value (annual ARR × term years)
 *    - status (string)
 *    - renewalDealId (string)             — populated for new_business / renewal closes
 *    - renewalDealClosedate (string)      — YYYY-MM-DD; matches the new contract's end date
 *    - renewalLineItemsSeeded (number)    — always 0; renewals no longer seed line items (left for output-field stability)
 *    - renewalDealAmount (number)         — amount on the auto-spawned renewal deal (last-segment ARR × renewal term years; one-time fees excluded)
 *    - renewalDealTermMonths (number)     — term of the spawned renewal deal in months (matches source contract term)
 *    - contractLineItemsCopied (number)   — # of source deal line items mirrored onto the contract
 *    - errorMessage (string)
 */
const axios = require('axios');

// Renewal deals must land in the FinQuery renewals pipeline, NOT the default
// sales pipeline. IDs match railway-api/server.js so the workflow and the
// shared helper agree on placement. Override via secrets on the workflow
// action if a sandbox / non-prod portal needs different IDs.
const RENEWAL_PIPELINE_ID =
  process.env.RENEWAL_PIPELINE_ID || '860641302';
const RENEWAL_DEAL_STAGE =
  process.env.RENEWAL_DEAL_STAGE || '1331037727';

const DEAL_PROPS = [
  'dealname',
  'amount',
  'closedate',
  'dealstage',
  'deal_category',
  'contract_start_date',
  'contract_end_date',
  'hubspot_owner_id',
];

const LINE_ITEM_PROPS = [
  'name',
  'description',
  'dh_quantity',
  'price',
  'amount',
  'hs_sku',
  'hs_line_item_currency_code',
  'dh_duration',
  'product_tag',
  'hs_recurring_billing_start_date',
  'hs_recurring_billing_end_date',
  'hs_recurring_billing_number_of_payments',
  'hs_recurring_billing_terms',
  'hs_acv',
  'hs_arr',
  'hs_mrr',
  'hs_term_in_months',
  'revenue_type',
  // DealHub's UI exposes simple `start_date` / `end_date` columns alongside
  // (or instead of) the HubSpot-native hs_recurring_billing_* dates. Read
  // both so we pick up whichever DealHub actually wrote.
  'start_date',
  'end_date',
];

// Fields we mirror from the source deal line item onto each contract line
// item. Keep this in sync with railway-api/server.js copyLineItemsToContract.
const CONTRACT_LINE_ITEM_PROPS = [
  'name',
  'description',
  'dh_quantity',
  'price',
  'amount',
  'hs_sku',
  'hs_line_item_currency_code',
  'dh_duration',
  'product_tag',
  'hs_recurring_billing_start_date',
  'hs_recurring_billing_end_date',
  'hs_recurring_billing_number_of_payments',
  'hs_recurring_billing_terms',
  'hs_acv',
  'hs_arr',
  'hs_mrr',
  'hs_term_in_months',
  'revenue_type',
];

// Common one-time charges. DealHub-managed line items use the `product_tag`
// enum ("Recurring" / "One-time") as the primary signal, with `dh_duration`
// (months — 0 / blank means one-time) as the secondary signal, and a name-
// based heuristic as a final fallback for legacy / manually-created lines.
const ONE_TIME_NAME_PATTERN =
  /\b(setup|set[\s-]?up|onboarding|on[\s-]?boarding|implementation|installation|kick[\s-]?off|training|provisioning|one[\s-]?time|ad[\s-]?hoc|professional services)\b/i;

// Returns 'recurring' | 'one_time' | null. See server.js parseProductTag.
function parseProductTag(value) {
  if (value === null || value === undefined) return null;
  const v = String(value).trim().toLowerCase().replace(/[\s_]+/g, '-');
  if (!v) return null;
  if (v === 'recurring' || v === 'subscription') return 'recurring';
  if (v === 'one-time' || v === 'onetime' || v === 'one' || v === 'ad-hoc' || v === 'adhoc') return 'one_time';
  return null;
}

// Minimum contract properties this workflow needs to write. We self-heal any
// missing ones on the contract schema before creating the record so the
// workflow never fails with PROPERTY_DOESNT_EXIST in portals where the schema
// was created with an older / leaner definition.
// IMPORTANT: contract start/end dates use the internal property names
// `startdate` / `enddate` on this portal (NOT `contract_start_date` /
// `contract_end_date` — those long-form names are the deal-level custom
// properties). Keep this list in sync with railway-api/server.js
// CONTRACT_SCHEMA + CONTRACT_PROPS.
const CONTRACT_REQUIRED_PROPERTIES = [
  { name: 'contract_name', label: 'Contract Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
  { name: 'contract_number', label: 'Contract Number', type: 'string', fieldType: 'text' },
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
  { name: 'startdate', label: 'Contract Start Date', type: 'date', fieldType: 'date' },
  { name: 'enddate', label: 'Contract End Date', type: 'date', fieldType: 'date' },
  { name: 'co_term_date', label: 'Co-Term Date', type: 'date', fieldType: 'date' },
  { name: 'activated_date', label: 'Activated Date', type: 'date', fieldType: 'date' },
  { name: 'contract_term', label: 'Contract Term (months)', type: 'number', fieldType: 'number' },
  { name: 'previous_contract_term', label: 'Previous Contract Term (months)', type: 'number', fieldType: 'number' },
  { name: 'renewal_term', label: 'Renewal Term (months)', type: 'number', fieldType: 'number' },
  { name: 'contract_renewed_on', label: 'Contract Renewed On', type: 'date', fieldType: 'date' },
  { name: 'total_arr', label: 'Total ARR', type: 'number', fieldType: 'number' },
  { name: 'total_tcv', label: 'Total TCV', type: 'number', fieldType: 'number' },
  { name: 'subscription_count', label: 'Subscription Count', type: 'number', fieldType: 'number' },
  { name: 'amendment_count', label: 'Amendment Count', type: 'number', fieldType: 'number' },
  { name: 'contract_data', label: 'Contract Data', type: 'string', fieldType: 'textarea' },
];

function fmtDateForHS(value) {
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function parseDateValue(value) {
  if (!value) return null;
  if (/^\d+$/.test(String(value))) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      const d = new Date(asNumber);
      if (!Number.isNaN(d.getTime())) return d;
    }
  }
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function addYearsToDateString(yyyyMmDd, years) {
  const d = parseDateValue(yyyyMmDd);
  if (!d) return null;
  d.setUTCFullYear(d.getUTCFullYear() + years);
  return fmtDateForHS(d);
}

function determineStatus(startDate, endDate) {
  const today = fmtDateForHS(new Date());
  if (startDate && startDate > today) return 'future';
  if (endDate && endDate < today) return 'expired';
  return 'active';
}

// Whole months between start and end (inclusive). Used to populate
// contract_term / renewal_term so the contract record reflects the term
// without requiring DealHub to write the value separately.
function monthsBetween(startDateStr, endDateStr) {
  const start = parseDateValue(startDateStr);
  const end = parseDateValue(endDateStr);
  if (!start || !end || start > end) return null;
  const months =
    (end.getUTCFullYear() - start.getUTCFullYear()) * 12 +
    (end.getUTCMonth() - start.getUTCMonth()) +
    (end.getUTCDate() >= start.getUTCDate() ? 1 : 0);
  return months > 0 ? months : null;
}

function cleanContractName(dealName) {
  const name = String(dealName || 'Contract').trim();
  return name
    .replace(/\s+[-—]\s+New Business$/i, '')
    .replace(/\s+[-—]\s+Renewal$/i, '')
    .trim();
}

function normalizeRevenueType(raw, fallback) {
  const value = String(raw || '').trim().toLowerCase();
  if (['new', 'renewal', 'expansion', 'contraction', 'cross_sell'].includes(value)) return value;
  return fallback || 'new';
}

function addDays(dateObj, days) {
  const d = new Date(dateObj);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function addMonths(dateObj, months) {
  const d = new Date(dateObj);
  d.setUTCMonth(d.getUTCMonth() + months);
  return d;
}

// `dh_duration` is the DealHub-managed line item duration field stored as a
// plain number of months. Returns months as a number, or null when the input
// is missing/zero/non-recurring.
function parseDhDuration(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n);
}

// Resolves the [start, end] span for a single line item, preferring its own
// recurring billing dates / term over the deal-level contract dates so a
// 3-year line item generates 3 segments even when the deal-level dates only
// cover 2. We accept multiple date-field conventions because DealHub may
// populate the HubSpot-native fields, the simple `start_date`/`end_date`
// custom fields, or a mix.
//   1. start = hs_recurring_billing_start_date, else start_date, else fallback
//   2. end   = hs_recurring_billing_end_date, else end_date, else
//              start + (number_of_payments × dh_duration months) - 1 day, else
//              start + dh_duration - 1 day (when DealHub sends a single
//              dh_duration without a payments count — typical MDQ-style
//              one-line-per-year payload), else
//              start + hs_term_in_months - 1 day, else
//              fallback
function resolveLineItemSpan(lineItem, fallbackStart, fallbackEnd) {
  const lp = (lineItem && lineItem.properties) || {};
  const start =
    parseDateValue(lp.hs_recurring_billing_start_date) ||
    parseDateValue(lp.start_date) ||
    parseDateValue(fallbackStart);

  let end =
    parseDateValue(lp.hs_recurring_billing_end_date) ||
    parseDateValue(lp.end_date);

  if (!end && start) {
    const periodMonths = parseDhDuration(lp.dh_duration);
    const numPayments = Math.max(0, Number(lp.hs_recurring_billing_number_of_payments) || 0);
    if (periodMonths && numPayments > 0) {
      end = addDays(addMonths(start, periodMonths * numPayments), -1);
    } else if (periodMonths) {
      // DealHub's MDQ-style payload sends ONE line item per product per year
      // with `dh_duration = 12` and no payments count. Treat the duration as
      // the line item's full span so it produces a single year-aligned
      // segment instead of falling back to the deal-level contract dates.
      end = addDays(addMonths(start, periodMonths), -1);
    } else {
      const termMonths = Math.max(0, Number(lp.hs_term_in_months) || 0);
      if (termMonths > 0) {
        end = addDays(addMonths(start, termMonths), -1);
      }
    }
  }

  if (!end) end = parseDateValue(fallbackEnd);

  return {
    startDate: start ? fmtDateForHS(start) : (fallbackStart || null),
    endDate: end ? fmtDateForHS(end) : (fallbackEnd || null),
  };
}

// Tolerance for off-by-one line item end dates: HubSpot recomputes
// hs_recurring_billing_end_date as start + period × number_of_payments when both
// are set, which can leave the persisted end date 1 day past the intended term
// boundary. Without tolerance, a 5/16/2026 → 5/16/2027 line item produces a
// proper Year 1 segment AND a single-day "Year 2" segment on 5/16/2027.
// Anything shorter than MIN_TAIL_DAYS at the tail of the loop is folded into
// the previous segment instead of emitting a bogus mini-year.
const MIN_TAIL_DAYS = 14;
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
    nextYearStart.setUTCFullYear(nextYearStart.getUTCFullYear() + 1);
    const currentEndCandidate = addDays(nextYearStart, -1);
    const currentEnd = currentEndCandidate > end ? new Date(end) : currentEndCandidate;

    const daysInSegment = Math.round((currentEnd - currentStart) / 86400000) + 1;
    if (segments.length > 0 && daysInSegment > 0 && daysInSegment < MIN_TAIL_DAYS) {
      segments[segments.length - 1].end_date = fmtDateForHS(currentEnd);
      break;
    }

    segments.push({
      year: segmentYear,
      start_date: fmtDateForHS(currentStart),
      end_date: fmtDateForHS(currentEnd),
    });

    if (currentEnd >= end) break;
    currentStart = addDays(currentEnd, 1);
    segmentYear += 1;
  }

  return segments;
}

function getLineItemAnnualArr(lineItem) {
  const props = lineItem?.properties || {};
  const quantity = Number(props.dh_quantity || 1) || 1;
  const unitPrice = Number(props.price || 0) || 0;
  return unitPrice * quantity;
}

// Returns the 1-indexed contract year that contains `segmentStart`, anchored
// on the contract's own start date. e.g. a contract starting 2026-05-01 puts
// segments starting 2026-05-01..2027-04-30 in Year 1, 2027-05-01..2028-04-30
// in Year 2, etc.
//
// This is the SERVER-SIDE counterpart to the card's computeContractYearForDate.
// It exists so segment_year is correctly stamped at write time when DealHub
// sends MDQ-style line items (one line per product per year — each line
// would otherwise be year=1 from buildYearSegments' perspective). With the
// contract-anchored year stamped, the card's useSegmentYearBucketing path
// works on the first read.
function computeContractYear(segmentStartStr, contractStartStr) {
  const segStart = parseDateValue(segmentStartStr);
  const contractStart = parseDateValue(contractStartStr);
  if (!segStart || !contractStart) return null;
  const yearsDiff = segStart.getUTCFullYear() - contractStart.getUTCFullYear();
  const monthsDiff = segStart.getUTCMonth() - contractStart.getUTCMonth();
  const daysDiff = segStart.getUTCDate() - contractStart.getUTCDate();
  let totalMonths = yearsDiff * 12 + monthsDiff;
  if (daysDiff < 0) totalMonths -= 1;
  const year = Math.floor(totalMonths / 12) + 1;
  return year >= 1 ? year : 1;
}

function isRecurringLineItem(lineItem) {
  const props = lineItem?.properties || {};

  // Primary signal — DealHub-managed `product_tag` enum ("Recurring" / "One-time").
  const tag = parseProductTag(props.product_tag);
  if (tag === 'recurring') return true;
  if (tag === 'one_time') return false;

  // Secondary signal — DealHub-managed `dh_duration` (months). 0 / blank = one-time.
  const durationRaw = props.dh_duration;
  const durationStr = String(durationRaw == null ? '' : durationRaw).trim();
  if (durationStr !== '') {
    return parseDhDuration(durationStr) !== null;
  }

  // Fallback signals for line items where neither product_tag nor dh_duration
  // was set (legacy imports, manually-created line items, etc.)
  if (Number(props.hs_arr || 0) > 0) return true;
  if (Number(props.hs_mrr || 0) > 0) return true;
  if (Number(props.hs_acv || 0) > 0) return true;
  if (String(props.hs_recurring_billing_terms || '').trim()) return true;
  if (Number(props.hs_term_in_months || 0) > 0) return true;

  // No explicit recurring metadata. Default to RECURRING for SaaS contexts and
  // flip to one-time only for items whose name matches a known one-time charge
  // pattern (setup, onboarding, training, etc.).
  const name = String(props.name || '');
  if (ONE_TIME_NAME_PATTERN.test(name)) return false;

  return true;
}

// Use the FULL SKU as product_code so distinct DealHub SKUs (e.g. SUB-MO,
// SUB-SU, SUB-PL, SRV-EN, SRV-ON) stay distinct on the segment record. The
// previous behavior collapsed everything before the first `-`, so every
// SaaS line item became `SUB` and every services line item became `SRV` —
// that made segment_name identical across products and broke the contract
// card's product breakdown. Falls back to product-name initials only when
// no SKU is present.
function deriveProductCode(lineItem) {
  const props = lineItem?.properties || {};
  const sku = String(props.hs_sku || '').trim();
  if (sku) return sku.toUpperCase();
  const name = String(props.name || '').trim();
  if (!name) return 'PRODUCT';
  const initials = name
    .split(/\s+/)
    .map((w) => w[0])
    .filter(Boolean)
    .join('')
    .toUpperCase();
  return (initials || name).slice(0, 16);
}

async function getObject(hs, typeId, objectId, properties) {
  const { data } = await hs.get(`/crm/v3/objects/${typeId}/${objectId}`, {
    params: { properties: properties.join(',') },
  });
  return data;
}

async function createObject(hs, typeId, properties) {
  const { data } = await hs.post(`/crm/v3/objects/${typeId}`, { properties });
  return data;
}

async function getAssociatedIds(hs, fromType, fromId, toType) {
  try {
    const { data } = await hs.get(`/crm/v4/objects/${fromType}/${fromId}/associations/${toType}`);
    return (data.results || []).map((r) => String(r.toObjectId));
  } catch (err) {
    if (err.response?.status === 404) return [];
    throw err;
  }
}

async function createAssociation(hs, fromType, fromId, toType, toId) {
  await hs.put(`/crm/v4/objects/${fromType}/${fromId}/associations/default/${toType}/${toId}`);
}

function parseHubSpotErrorMessage(err) {
  const responseData = err?.response?.data;
  if (!responseData) return err?.message || 'Unknown HubSpot error';
  if (typeof responseData === 'string') return responseData;
  if (responseData.message) {
    const extras = [];
    if (Array.isArray(responseData.errors) && responseData.errors.length) {
      const firstThree = responseData.errors.slice(0, 3).map((e) => {
        if (!e) return '';
        if (typeof e === 'string') return e;
        return e.message || e.error || JSON.stringify(e);
      }).filter(Boolean);
      if (firstThree.length) extras.push(`errors=[${firstThree.join('; ')}]`);
    }
    if (responseData.correlationId) extras.push(`correlationId=${responseData.correlationId}`);
    if (responseData.category) extras.push(`category=${responseData.category}`);
    return extras.length ? `${responseData.message} (${extras.join(', ')})` : responseData.message;
  }
  try {
    return JSON.stringify(responseData);
  } catch (jsonErr) {
    return err?.message || 'Unknown HubSpot error';
  }
}

function buildHubSpotErrorMessage(err, context) {
  if (!err) return context || 'Unknown error';
  const status = err?.response?.status;
  const url = err?.config?.url;
  const method = String(err?.config?.method || '').toUpperCase();
  const detail = parseHubSpotErrorMessage(err);
  const parts = [];
  if (context) parts.push(context);
  if (status) parts.push(`HTTP ${status}`);
  if (method && url) parts.push(`${method} ${url}`);
  if (detail) parts.push(detail);
  if (!status && err?.code) parts.push(`code=${err.code}`);
  return parts.join(' | ');
}

async function tryStep(label, fn) {
  try {
    return await fn();
  } catch (err) {
    if (err && !err.__step) {
      try { Object.defineProperty(err, '__step', { value: label, enumerable: false }); }
      catch (_) { err.__step = label; }
    }
    throw err;
  }
}

async function createObjectWithFallback(hs, typeId, properties, requiredKeys) {
  try {
    return await createObject(hs, typeId, properties);
  } catch (err) {
    const status = err?.response?.status;
    const firstMessage = parseHubSpotErrorMessage(err);
    if (status !== 400 && status !== 422) {
      throw new Error(`Object create failed (typeId=${typeId}, status=${status || 'n/a'}). ${firstMessage}`);
    }

    const fallback = {};
    for (const key of requiredKeys) {
      if (properties[key] !== undefined && properties[key] !== null && properties[key] !== '') {
        fallback[key] = properties[key];
      }
    }

    if (!Object.keys(fallback).length) {
      throw new Error(`Object create failed (typeId=${typeId}, no fallback keys available). ${firstMessage}`);
    }

    try {
      return await createObject(hs, typeId, fallback);
    } catch (fallbackErr) {
      const second = parseHubSpotErrorMessage(fallbackErr);
      throw new Error(`Object create failed (typeId=${typeId}). full=${firstMessage} | fallback=${second}`);
    }
  }
}

async function associateWithFallback(hs, fromType, fromId, toType, toId) {
  try {
    await createAssociation(hs, fromType, fromId, toType, toId);
    return;
  } catch (firstErr) {
    try {
      await createAssociation(hs, toType, toId, fromType, fromId);
      return;
    } catch (secondErr) {
      const first = parseHubSpotErrorMessage(firstErr);
      const second = parseHubSpotErrorMessage(secondErr);
      throw new Error(`Association failed both directions (${fromType}:${fromId} <-> ${toType}:${toId}). forward=${first} | reverse=${second}`);
    }
  }
}

async function getTypeIdBySchemaName(hs, schemaName) {
  const { data } = await hs.get('/crm/v3/schemas');
  const schema = (data.results || []).find((s) => s.name === schemaName);
  return schema?.objectTypeId || null;
}

async function getSchemaPropertyNamesByTypeId(hs, typeId) {
  const { data } = await hs.get(`/crm/v3/schemas/${typeId}`);
  return new Set((data.properties || []).map((p) => p.name));
}

// Fetches /crm/v3/schemas exactly once and returns both type IDs and property
// name sets for the requested schemas. Saves 3 round-trips vs. calling
// getTypeIdBySchemaName + getSchemaPropertyNamesByTypeId individually for each
// schema -- crucial for staying inside the 20s workflow wall clock.
async function loadSchemasIndex(hs, schemaNames) {
  const { data } = await hs.get('/crm/v3/schemas');
  const all = data?.results || [];
  const out = {};
  for (const name of schemaNames) {
    const schema = all.find((s) => s.name === name);
    if (!schema) {
      out[name] = { typeId: null, propertyNames: new Set() };
      continue;
    }
    out[name] = {
      typeId: schema.objectTypeId,
      propertyNames: new Set((schema.properties || []).map((p) => p.name)),
    };
  }
  return out;
}

// Batch read up to 100 records per call.
async function batchReadObjects(hs, typeId, ids, properties) {
  if (!ids?.length) return [];
  const all = [];
  for (let i = 0; i < ids.length; i += 100) {
    const slice = ids.slice(i, i + 100);
    const { data } = await hs.post(`/crm/v3/objects/${typeId}/batch/read`, {
      properties,
      inputs: slice.map((id) => ({ id: String(id) })),
    });
    all.push(...(data?.results || []));
  }
  return all;
}

// Batch create up to 100 records per call. Returns the same shape per item as
// the single-create endpoint (id + properties). Order is preserved.
async function batchCreateObjects(hs, typeId, items) {
  if (!items?.length) return { created: [], failed: [] };
  const created = [];
  const failed = [];
  for (let i = 0; i < items.length; i += 100) {
    const slice = items.slice(i, i + 100);
    try {
      const resp = await hs.post(`/crm/v3/objects/${typeId}/batch/create`, {
        inputs: slice.map((properties) => ({ properties })),
      });
      const results = resp?.data?.results || [];
      created.push(...results);
      // 207 multi-status surfaces an `errors` array when some inputs failed.
      const partialErrors = resp?.data?.errors;
      if (Array.isArray(partialErrors) && partialErrors.length) {
        failed.push(...partialErrors);
      }
    } catch (err) {
      const status = err?.response?.status;
      const message = parseHubSpotErrorMessage(err);
      throw new Error(`Batch create failed (typeId=${typeId}, batch=${i}-${i + slice.length}, status=${status || 'n/a'}). ${message}`);
    }
  }
  return { created, failed };
}

// Batch associate using the v4 default-label endpoint. If the endpoint isn't
// available for the type pair we fall back to sequential PUTs.
async function batchAssociateDefault(hs, fromType, toType, pairs) {
  if (!pairs?.length) return { ok: 0, failed: 0 };
  let ok = 0;
  let failed = 0;
  for (let i = 0; i < pairs.length; i += 100) {
    const slice = pairs.slice(i, i + 100);
    try {
      await hs.post(`/crm/v4/associations/${fromType}/${toType}/batch/associate/default`, {
        inputs: slice.map(([from, to]) => ({ from: { id: String(from) }, to: { id: String(to) } })),
      });
      ok += slice.length;
    } catch (err) {
      const status = err?.response?.status;
      const message = parseHubSpotErrorMessage(err);
      console.warn(
        `[batch-associate] ${fromType} -> ${toType} batch failed (status=${status || 'n/a'}): ${message}. Falling back to sequential.`
      );
      for (const [from, to] of slice) {
        try {
          await associateWithFallback(hs, fromType, from, toType, to);
          ok += 1;
        } catch (perItemErr) {
          failed += 1;
          console.warn(
            `[batch-associate] sequential fallback failed for ${fromType}:${from} -> ${toType}:${to}: ${perItemErr.message}`
          );
        }
      }
    }
  }
  return { ok, failed };
}

function filterPropertiesForSchema(properties, schemaPropertyNames) {
  const filtered = {};
  for (const [key, value] of Object.entries(properties || {})) {
    if (!schemaPropertyNames?.has(key)) continue;
    if (value === undefined || value === null || value === '') continue;
    filtered[key] = value;
  }
  return filtered;
}

async function getDefaultPropertyGroupName(hs, typeId) {
  try {
    const { data } = await hs.get(`/crm/v3/properties/${typeId}/groups`);
    const groups = data?.results || [];
    if (!groups.length) return null;
    const nonHubspot = groups.find(
      (g) => g?.name && !String(g.name).toLowerCase().startsWith('hs_')
    );
    return (nonHubspot || groups[0])?.name || null;
  } catch (err) {
    console.warn(`[ensure-properties] Could not fetch groups for ${typeId}: ${parseHubSpotErrorMessage(err)}`);
    return null;
  }
}

async function ensureSchemaProperties(hs, typeId, expectedProperties, existingPropertyNames) {
  const missing = expectedProperties.filter((prop) => !existingPropertyNames.has(prop.name));
  if (!missing.length) return [];

  const defaultGroup = await getDefaultPropertyGroupName(hs, typeId);
  const created = [];

  for (const prop of missing) {
    const payload = prop.groupName ? prop : { ...prop, groupName: defaultGroup || prop.groupName };
    if (!payload.groupName) {
      console.warn(
        `[ensure-properties] Skipping ${prop.name} on ${typeId}: no property group available`
      );
      continue;
    }
    try {
      await hs.post(`/crm/v3/properties/${typeId}`, payload);
      existingPropertyNames.add(prop.name);
      created.push(prop.name);
      console.log(`[ensure-properties] Created missing property ${prop.name} on ${typeId}`);
    } catch (err) {
      const status = err?.response?.status;
      if (status === 409) {
        existingPropertyNames.add(prop.name);
        continue;
      }
      console.warn(
        `[ensure-properties] Failed to create ${prop.name} on ${typeId}: ${parseHubSpotErrorMessage(err)}`
      );
    }
  }
  return created;
}

exports.main = async (event, callback) => {
  try {
    const dealId = String(
      event.inputFields?.dealId ||
      event.inputFields?.hs_object_id ||
      event.object?.objectId ||
      ''
    ).trim();

    if (!dealId) {
      throw new Error('Missing deal ID. Provide input field `dealId` or run from a deal-based workflow.');
    }

    const token =
      process.env.PRIVATE_APP_TOKEN ||
      process.env.HUBSPOT_ACCESS_TOKEN ||
      process.env.PRIVATE_APP_ACCESS_TOKEN ||
      process.env.HUBSPOT_PRIVATE_APP_TOKEN;

    if (!token) {
      throw new Error('Missing HubSpot token secret (PRIVATE_APP_TOKEN).');
    }

    const hs = axios.create({
      baseURL: 'https://api.hubapi.com',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      // Per-call timeout MUST be well under the 20s workflow wall clock so a
      // single hung request can't burn the entire budget.
      timeout: 8000,
    });

    // HubSpot Private Apps cap at 100 req / 10 sec. Without retry/backoff a
    // single closed-deal burst trips a 429 and surfaces as "Request failed
    // with status code 429". Retry 429 / 5xx with exponential backoff +
    // jitter, honoring `Retry-After`. Workflow custom code actions have a
    // ~20s wall clock so we keep the budget very tight: max 2 retries with a
    // 1.5s ceiling, capping worst-case backoff at ~3s.
    const HS_MAX_RETRIES = 2;
    const HS_BASE_DELAY_MS = 300;
    const HS_MAX_DELAY_MS = 1500;
    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const shouldRetry = (err) => {
      const status = err?.response?.status;
      if (status === 429 || status === 502 || status === 503 || status === 504) return true;
      if (!err?.response && err?.code && ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN'].includes(err.code)) {
        return true;
      }
      return false;
    };
    const computeDelay = (err, attempt) => {
      const retryAfter = err?.response?.headers?.['retry-after'];
      if (retryAfter) {
        const seconds = Number(retryAfter);
        if (Number.isFinite(seconds) && seconds > 0) {
          return Math.min(seconds * 1000, HS_MAX_DELAY_MS);
        }
      }
      return Math.min(HS_BASE_DELAY_MS * Math.pow(2, attempt) + Math.floor(Math.random() * 200), HS_MAX_DELAY_MS);
    };
    hs.interceptors.response.use(
      (response) => response,
      async (error) => {
        const config = error.config;
        if (!config) return Promise.reject(error);
        config.__hsRetryCount = config.__hsRetryCount || 0;
        if (config.__hsRetryCount >= HS_MAX_RETRIES || !shouldRetry(error)) {
          if (error.response?.status === 429) {
            console.error(
              `[hubspot] 429 after ${config.__hsRetryCount} retries on ${config.method?.toUpperCase()} ${config.url}`
            );
          }
          return Promise.reject(error);
        }
        const delayMs = computeDelay(error, config.__hsRetryCount);
        config.__hsRetryCount += 1;
        console.warn(
          `[hubspot] ${error.response?.status || error.code || 'network'} on ${config.method?.toUpperCase()} ${config.url} ` +
          `— retry ${config.__hsRetryCount}/${HS_MAX_RETRIES} in ${delayMs}ms`
        );
        await sleep(delayMs);
        return hs.request(config);
      }
    );

    // Single /crm/v3/schemas call returns BOTH type IDs and property name
    // sets for both schemas. Saves 3 sequential round-trips vs. lookup +
    // per-schema property fetches.
    const schemaIndex = await tryStep('lookup-schemas', () =>
      loadSchemasIndex(hs, ['fq_contract', 'fq_subscription'])
    );
    const contractTypeId = schemaIndex.fq_contract.typeId;
    const subscriptionTypeId = schemaIndex.fq_subscription.typeId;
    const contractSchemaPropertyNames = schemaIndex.fq_contract.propertyNames;
    const subscriptionSchemaPropertyNames = schemaIndex.fq_subscription.propertyNames;

    if (!contractTypeId) throw new Error('Could not find schema fq_contract.');
    if (!subscriptionTypeId) throw new Error('Could not find schema fq_subscription.');

    // Self-heal any contract properties this workflow needs but the live
    // schema is missing. ensureSchemaProperties is a no-op when nothing is
    // missing (which is the normal case after first run).
    await tryStep('ensure-contract-properties', () =>
      ensureSchemaProperties(
        hs,
        contractTypeId,
        CONTRACT_REQUIRED_PROPERTIES,
        contractSchemaPropertyNames
      )
    );

    // Pull the deal record + its 3 association lists in parallel so the
    // setup phase costs ~1 round-trip instead of 4 sequential ones.
    const [deal, companyIds, contactIds, lineItemIds] = await tryStep(
      `fetch-deal-context:${dealId}`,
      () =>
        Promise.all([
          getObject(hs, '0-3', dealId, DEAL_PROPS),
          getAssociatedIds(hs, '0-3', dealId, '0-2'),
          getAssociatedIds(hs, '0-3', dealId, '0-1'),
          getAssociatedIds(hs, '0-3', dealId, 'line_items'),
        ])
    );
    const dealProps = deal.properties || {};

    const lineItems = lineItemIds.length
      ? await tryStep('batch-read-line-items', () =>
          batchReadObjects(hs, 'line_items', lineItemIds, LINE_ITEM_PROPS)
        )
      : [];

    const lineItemsProcessed = lineItems.length;

    const derivedStartDate =
      dealProps.contract_start_date ||
      fmtDateForHS(parseDateValue(dealProps.closedate) || new Date()) ||
      fmtDateForHS(new Date());
    const derivedEndDate =
      dealProps.contract_end_date ||
      addYearsToDateString(derivedStartDate, 1) ||
      fmtDateForHS(new Date());

    const category = String(dealProps.deal_category || 'new_business').toLowerCase();
    const isRenewal = category === 'renewal';
    const initialStatus = determineStatus(derivedStartDate, derivedEndDate);
    const computedTermMonths = monthsBetween(derivedStartDate, derivedEndDate);
    const cleanedContractNameForPayload = cleanContractName(dealProps.dealname);
    const dealOwnerId = dealProps.hubspot_owner_id || '';
    const todayStr = fmtDateForHS(new Date());

    const contractPayloadRaw = {
      contract_name: cleanedContractNameForPayload,
      // Use the deal name as a human-readable contract number out of the
      // gate. Customers / DealHub can overwrite later; this just keeps the
      // record from looking blank.
      contract_number: cleanedContractNameForPayload,
      status: initialStatus,
      // NOTE: internal property names are `startdate`/`enddate` on this
      // contract object — NOT the long-form `contract_start_date`/
      // `contract_end_date` (those are deal-level properties).
      startdate: derivedStartDate,
      enddate: derivedEndDate,
      co_term_date: derivedEndDate,
      contract_term: computedTermMonths != null ? String(computedTermMonths) : '',
      total_arr: '0',
      total_tcv: '0',
      subscription_count: '0',
      amendment_count: '0',
      contract_data: JSON.stringify({
        source: 'workflow_custom_code',
        deal_id: dealId,
        deal_name: dealProps.dealname || '',
        deal_category: category,
        line_items_processed: lineItemsProcessed,
      }),
    };

    // Inherit the deal owner so the contract record isn't ownerless.
    if (dealOwnerId) {
      contractPayloadRaw.hubspot_owner_id = String(dealOwnerId);
    }

    // Stamp activated_date the moment we create an active contract; future
    // contracts will get it filled when run-status-check flips them.
    if (initialStatus === 'active') {
      contractPayloadRaw.activated_date = todayStr;
    }

    // Renewal-only fields — make the renewal lineage visible on the new
    // record without waiting for a follow-up update.
    if (isRenewal) {
      contractPayloadRaw.renewal_term = computedTermMonths != null ? String(computedTermMonths) : '';
      contractPayloadRaw.contract_renewed_on = todayStr;
    }

    const contractPayload = filterPropertiesForSchema(contractPayloadRaw, contractSchemaPropertyNames);
    // hubspot_owner_id is a built-in property (not in the custom schema's
    // properties array), so filterPropertiesForSchema strips it. Add it back
    // explicitly when present.
    if (dealOwnerId) {
      contractPayload.hubspot_owner_id = String(dealOwnerId);
    }
    const contractRequiredKeys = ['contract_name', 'status', 'contract_data'].filter((key) =>
      contractSchemaPropertyNames.has(key)
    );
    const contract = await tryStep('create-contract', () =>
      createObjectWithFallback(hs, contractTypeId, contractPayload, contractRequiredKeys)
    );

    // Run all post-create contract associations in parallel. Contacts are
    // batched into a single API call instead of looping with a PUT each.
    await tryStep(`associate-contract-context:${contract.id}`, () =>
      Promise.all([
        associateWithFallback(hs, contractTypeId, contract.id, '0-3', dealId),
        companyIds[0]
          ? associateWithFallback(hs, contractTypeId, contract.id, '0-2', companyIds[0])
          : Promise.resolve(),
        contactIds.length
          ? batchAssociateDefault(
              hs,
              contractTypeId,
              '0-1',
              contactIds.map((cid) => [contract.id, cid])
            )
          : Promise.resolve(),
      ])
    );

    // totalArr  = annualized ARR (one year of recurring revenue, summed across recurring line items)
    // totalTcv  = total contract value (annual ARR × number of yearly segments per line item).
    //
    // productLatestLine tracks each product's final-year line item so we can
    // compute the "last segment ARR" — the run rate at the END of the source
    // contract — which is what the renewal deal amount is forecast against
    // (last-segment ARR × renewal term years). When a deal uses MDQ-style
    // line items (one line per product per year), this naturally captures
    // year-over-year uplifts (e.g. Y1=20K, Y2=25K, Y3=30K → last=30K).
    let totalArr = 0;
    let totalTcv = 0;
    const productLatestLine = new Map();
    const segmentErrors = [];

    // One subscription segment per recurring line item per contract year. One-time
    // line items live on the contract as line items only -- they are NOT segments.
    const recurringLineItems = lineItems.filter(isRecurringLineItem);
    const oneTimeLineItems = lineItems.length - recurringLineItems.length;
    const cleanedContractName = cleanContractName(dealProps.dealname);
    const dealRevenueType = normalizeRevenueType('', category === 'renewal' ? 'renewal' : 'new');

    // Build all segment payloads in one pass so we can batch-create them.
    // Year segments are computed PER LINE ITEM from each line item's own
    // hs_recurring_billing_start_date / end / term — not the deal-level
    // contract dates. This ensures a 3-year line item produces 3 segments
    // even when other line items on the same deal are 1- or 2-year.
    // Order matters here -- the response from /batch/create preserves input
    // order so we can use `created[i]` to associate the same segment that
    // came from `segmentPayloads[i]`.
    const segmentPayloads = [];
    const segmentLabels = [];
    for (const [liIndex, lineItem] of recurringLineItems.entries()) {
      const lp = lineItem.properties || {};
      const productName = String(lp.name || 'Product').trim() || 'Product';
      const productCode = deriveProductCode(lineItem);
      const quantity = Number(lp.dh_quantity || 1) || 1;
      const unitPrice = Number(lp.price || 0) || 0;
      const annualArr = getLineItemAnnualArr(lineItem);
      const lineRevenueType = normalizeRevenueType(lp.revenue_type, dealRevenueType);

      const lineSpan = resolveLineItemSpan(lineItem, derivedStartDate, derivedEndDate);
      const lineYearSegments = buildYearSegments(lineSpan.startDate, lineSpan.endDate);

      // Annual ARR is added ONCE per line item; TCV accumulates per yearly segment.
      totalArr += annualArr;
      totalTcv += annualArr * Math.max(lineYearSegments.length, 1);

      // Track the line with the latest end_date per product so we can pick
      // up the final-year ARR (post-uplift) for renewal forecasting. Group
      // by SKU first; fall back to product name when SKU isn't set.
      const productKey =
        String(lp.hs_sku || '').trim() ||
        String(lp.name || '').trim() ||
        productCode ||
        `LINE_${liIndex}`;
      const lineEndDate = parseDateValue(lineSpan.endDate);
      const previousLatest = productLatestLine.get(productKey);
      if (
        !previousLatest ||
        (lineEndDate && (!previousLatest.endDate || lineEndDate > previousLatest.endDate))
      ) {
        productLatestLine.set(productKey, { endDate: lineEndDate, arr: annualArr });
      }

      for (const [yearIdx, segmentYear] of lineYearSegments.entries()) {
        // Anchor segment_year on the CONTRACT start, not the line item's own
        // span. This is what makes DealHub's MDQ-style payloads (one line per
        // product per year) bucket correctly in the contract card: each line
        // produces a single segment whose year is 1/2/3 based on its date
        // within the contract, instead of every line being year=1.
        // Falls back to buildYearSegments' relative year when we can't
        // determine the contract anchor (e.g. degenerate dates).
        const contractAnchoredYear =
          computeContractYear(segmentYear.start_date, derivedStartDate) ||
          segmentYear.year;

        // Prefer product_name in the visible segment_name so distinct
        // products are readable in the contract card. Fall back to
        // product_code only when product_name is missing.
        const displayLabel = productName || productCode;

        const segmentPayloadRaw = {
          segment_name: `${cleanedContractName} — ${displayLabel} Year ${contractAnchoredYear}`,
          product_name: productName,
          product_code: productCode,
          quantity: String(quantity),
          original_quantity: String(quantity),
          unit_price: String(unitPrice),
          arr: String(annualArr),
          mrr: String(annualArr / 12),
          tcv: String(annualArr),
          status: determineStatus(segmentYear.start_date, segmentYear.end_date),
          start_date: segmentYear.start_date,
          end_date: segmentYear.end_date,
          subscription_start_date: lineSpan.startDate || derivedStartDate,
          subscription_end_date: lineSpan.endDate || derivedEndDate,
          arr_start_date: segmentYear.start_date,
          arr_end_date: segmentYear.end_date,
          segment_year: String(contractAnchoredYear),
          segment_index: String(yearIdx + 1),
          segment_label: `Year ${contractAnchoredYear}`,
          segment_key: `${contract.id}-${liIndex + 1}-${yearIdx + 1}`,
          billing_frequency: 'annual',
          charge_type: 'recurring',
          revenue_type: lineRevenueType,
        };

        segmentPayloads.push(filterPropertiesForSchema(segmentPayloadRaw, subscriptionSchemaPropertyNames));
        segmentLabels.push(`${displayLabel} Year ${contractAnchoredYear}`);
      }
    }

    // Batch-create all segments in a single call (or two if >100). If the
    // batch fails outright with a 400/422 (typically a property validation
    // issue affecting every payload), retry once with a required-keys-only
    // fallback so we still write the data.
    let createdSegmentsList = [];
    let segmentBatchFailures = [];
    if (segmentPayloads.length) {
      const segmentRequiredKeys = [
        'segment_name',
        'product_name',
        'product_code',
        'quantity',
        'unit_price',
        'arr',
        'mrr',
        'tcv',
        'status',
        'start_date',
        'end_date',
        'revenue_type',
      ].filter((key) => subscriptionSchemaPropertyNames.has(key));

      try {
        const { created, failed } = await tryStep('batch-create-segments', () =>
          batchCreateObjects(hs, subscriptionTypeId, segmentPayloads)
        );
        createdSegmentsList = created;
        segmentBatchFailures = failed;
      } catch (batchErr) {
        const messageFromAxios = parseHubSpotErrorMessage(batchErr);
        console.warn(`[batch-create-segments] full payload batch failed: ${messageFromAxios}. Retrying with required-keys-only fallback.`);
        const reducedPayloads = segmentPayloads.map((payload) => {
          const reduced = {};
          for (const key of segmentRequiredKeys) {
            if (payload[key] !== undefined && payload[key] !== null && payload[key] !== '') {
              reduced[key] = payload[key];
            }
          }
          return reduced;
        });
        try {
          const { created, failed } = await tryStep('batch-create-segments-fallback', () =>
            batchCreateObjects(hs, subscriptionTypeId, reducedPayloads)
          );
          createdSegmentsList = created;
          segmentBatchFailures = failed;
        } catch (fallbackErr) {
          const fallbackMessage = parseHubSpotErrorMessage(fallbackErr);
          segmentErrors.push(`Batch create failed (full + fallback). full=${messageFromAxios} | fallback=${fallbackMessage}`);
        }
      }

      if (segmentBatchFailures.length) {
        for (const item of segmentBatchFailures.slice(0, 5)) {
          segmentErrors.push(item?.message || JSON.stringify(item).slice(0, 200));
        }
      }
    }

    const createdSegments = createdSegmentsList.length;

    // Batch-associate every created segment to the contract (and to the
    // company if present). One call per from->to type pair regardless of
    // segment count.
    if (createdSegments) {
      try {
        await tryStep('batch-associate-segments-to-contract', () =>
          batchAssociateDefault(
            hs,
            subscriptionTypeId,
            contractTypeId,
            createdSegmentsList.map((seg) => [seg.id, contract.id])
          )
        );
      } catch (assocErr) {
        segmentErrors.push(`Segment->contract associations failed: ${parseHubSpotErrorMessage(assocErr)}`);
      }

      if (companyIds[0]) {
        try {
          await tryStep('batch-associate-segments-to-company', () =>
            batchAssociateDefault(
              hs,
              subscriptionTypeId,
              '0-2',
              createdSegmentsList.map((seg) => [seg.id, companyIds[0]])
            )
          );
        } catch (assocErr) {
          segmentErrors.push(`Segment->company associations failed: ${parseHubSpotErrorMessage(assocErr)}`);
        }
      }
    }

    if (segmentLabels.length && createdSegments < segmentLabels.length) {
      console.warn(
        `Only ${createdSegments}/${segmentLabels.length} segments were created. First missing: ${segmentLabels[createdSegments]}`
      );
    }

    // ── Copy ALL deal line items onto the contract ───────────────────────────
    // Mirrors railway-api/server.js copyLineItemsToContract: every line item
    // (recurring + one-time) is cloned to the contract with the full set of
    // term/billing/revenue fields preserved, and start/end dates are stamped
    // on every line so finance always sees a complete span. Batched into one
    // /crm/v3/objects/line_items/batch/create call so a 50-line contract
    // costs ~1 round-trip instead of 50.
    let contractLineItemsCopied = 0;
    const contractLineItemErrors = [];
    if (lineItems.length) {
      const contractLineInputs = lineItems.map((li) => {
        const lp = li.properties || {};
        const span = resolveLineItemSpan(li, derivedStartDate, derivedEndDate);
        const recurring = isRecurringLineItem(li);
        const propsRaw = {
          name: lp.name || 'Product',
          description: lp.description || '',
          dh_quantity: lp.dh_quantity || '1',
          price: lp.price || '0',
          amount: lp.amount || '',
          hs_sku: lp.hs_sku || '',
          hs_line_item_currency_code: lp.hs_line_item_currency_code || '',
          dh_duration: lp.dh_duration || '',
          product_tag: lp.product_tag || (recurring ? 'Recurring' : 'One-time'),
          hs_recurring_billing_start_date: span.startDate || lp.hs_recurring_billing_start_date || '',
          hs_recurring_billing_end_date: span.endDate || lp.hs_recurring_billing_end_date || '',
          hs_recurring_billing_number_of_payments: lp.hs_recurring_billing_number_of_payments || '',
          hs_recurring_billing_terms: lp.hs_recurring_billing_terms || '',
          hs_acv: lp.hs_acv || '',
          hs_arr: lp.hs_arr || '',
          hs_mrr: lp.hs_mrr || '',
          hs_term_in_months: lp.hs_term_in_months || '',
          revenue_type: lp.revenue_type || '',
        };
        const cleanedProps = {};
        for (const key of CONTRACT_LINE_ITEM_PROPS) {
          const value = propsRaw[key];
          if (value === undefined || value === null || value === '') continue;
          cleanedProps[key] = String(value);
        }
        return { properties: cleanedProps };
      });

      let createdContractLines = [];
      try {
        const { created, failed } = await tryStep('batch-create-contract-line-items', () =>
          batchCreateObjects(hs, 'line_items', contractLineInputs.map((input) => input.properties))
        );
        createdContractLines = created;
        if (failed.length) {
          for (const fail of failed.slice(0, 3)) {
            contractLineItemErrors.push(fail?.message || JSON.stringify(fail).slice(0, 200));
          }
        }
      } catch (lineCreateErr) {
        contractLineItemErrors.push(parseHubSpotErrorMessage(lineCreateErr));
      }

      if (createdContractLines.length) {
        try {
          await tryStep('batch-associate-contract-line-items', () =>
            batchAssociateDefault(
              hs,
              'line_items',
              contractTypeId,
              createdContractLines.map((line) => [line.id, contract.id])
            )
          );
          contractLineItemsCopied = createdContractLines.length;
        } catch (assocErr) {
          contractLineItemErrors.push(
            `Contract line item associations failed: ${parseHubSpotErrorMessage(assocErr)}`
          );
        }
      }

      if (contractLineItemErrors.length) {
        for (const msg of contractLineItemErrors.slice(0, 2)) {
          segmentErrors.push(`contract-line-items: ${msg}`);
        }
      }

      console.log(
        `[copy-line-items] Copied ${contractLineItemsCopied}/${lineItems.length} line items to contract ${contract.id}`
      );
    }

    const rollupPayload = filterPropertiesForSchema(
      {
        total_arr: String(totalArr),
        total_tcv: String(totalTcv),
        subscription_count: String(createdSegments),
      },
      contractSchemaPropertyNames
    );
    if (Object.keys(rollupPayload).length) {
      try {
        await hs.patch(`/crm/v3/objects/${contractTypeId}/${contract.id}`, {
          properties: rollupPayload,
        });
      } catch (rollupErr) {
        console.warn(`Contract rollup patch failed: ${parseHubSpotErrorMessage(rollupErr)}`);
      }
    }

    // ── Auto-spawn next-cycle renewal deal ───────────────────────────────────
    // Per Apr 28 training: renewal deals must be generated IMMEDIATELY on
    // Closed Won (not waiting until contract end + 1 day) so they land in the
    // forecast for the contract's end-date quarter. Only fires for new
    // business + renewal categories; amendments / expansions / contractions
    // never spawn renewal deals from this workflow.
    //
    // Per May 1 correction:
    //   • Term: the renewal deal inherits the FULL TERM of the source contract
    //     (a 3-year source spawns a 3-year renewal), not a fixed 1-year span.
    //   • Line items: NONE — the renewal deal is a placeholder for DealHub to
    //     quote into. We used to seed final-year recurring lines but that was
    //     wrong; DealHub builds the renewal product set from scratch.
    //   • Amount: LAST-SEGMENT ARR (one year of recurring revenue at the
    //     post-uplift run rate). For a source priced Y1=30K, Y2=35K, Y3=40K
    //     the renewal amount = 40K. NOT multiplied by renewal years — the
    //     renewal is a placeholder for DealHub to overwrite with their own
    //     quote, and finance wants a single-year run-rate forecast on the
    //     deal until that happens. Products whose final segment ends well
    //     before contract end (terminated mid-contract) are excluded so
    //     they don't inflate the figure.
    let renewalDealId = '';
    let renewalDealClosedate = '';
    let renewalLineItemsSeeded = 0;
    let renewalDealAmount = 0;
    let renewalDealTermMonths = 0;
    const renewalEligibleCategories = ['new_business', 'renewal'];
    if (renewalEligibleCategories.includes(category)) {
      try {
        // Inherit the full source-contract term. Fall back to 12 months only
        // when we couldn't compute a real term from the source dates.
        const renewalTermMonths = computedTermMonths != null && computedTermMonths > 0
          ? computedTermMonths
          : 12;
        renewalDealTermMonths = renewalTermMonths;

        const nextStart = parseDateValue(derivedEndDate) || new Date();
        nextStart.setUTCDate(nextStart.getUTCDate() + 1);
        // End of the renewal term = start + termMonths - 1 day so the
        // renewal spans exactly the same number of months as the source
        // contract (mirrors the line-item span helpers above).
        const nextEnd = addDays(addMonths(nextStart, renewalTermMonths), -1);
        renewalDealClosedate = derivedEndDate;

        // ── Compute last-segment ARR (run rate at source contract end) ─────
        // For each product (grouped by SKU), pick the line with the latest
        // end_date. Filter out products whose latest line ended well before
        // the source contract end — those terminated mid-contract and won't
        // be in the renewal. 30-day grace handles boundary-day rounding.
        const contractEndForRenewal = parseDateValue(derivedEndDate);
        const renewalCutoff = contractEndForRenewal
          ? addDays(contractEndForRenewal, -30)
          : null;
        let lastSegmentArrSum = 0;
        for (const { endDate, arr } of productLatestLine.values()) {
          if (renewalCutoff && endDate && endDate < renewalCutoff) continue;
          lastSegmentArrSum += arr;
        }

        // Renewal deal amount = last-segment ARR (single-year run rate, NOT
        // multiplied by renewal term years). Falls back to totalArr when no
        // products survive the cutoff filter (rare; happens when every line
        // item terminates well before contract end).
        const lastSegmentArrForRenewal = lastSegmentArrSum > 0
          ? lastSegmentArrSum
          : totalArr;
        renewalDealAmount = lastSegmentArrForRenewal;

        // total_tcv and year_1_arr both equal `amount` for renewals — the
        // deal carries no line items for HubSpot to roll up, so we stamp
        // them as a single-year forecast placeholder until DealHub overwrites
        // when they configure the actual quote.
        const renewalTotalTcv = renewalDealAmount;
        const renewalYear1Arr = renewalDealAmount;

        const renewalDealName = `${cleanedContractName} — Renewal`;
        const renewalDeal = await createObjectWithFallback(
          hs,
          '0-3',
          {
            dealname: renewalDealName,
            pipeline: RENEWAL_PIPELINE_ID,
            dealstage: RENEWAL_DEAL_STAGE,
            deal_category: 'renewal',
            contract_start_date: fmtDateForHS(nextStart),
            contract_end_date: fmtDateForHS(nextEnd),
            closedate: derivedEndDate,
            amount: String(renewalDealAmount),
            total_tcv: String(renewalTotalTcv),
            year_1_arr: String(renewalYear1Arr),
          },
          ['dealname', 'dealstage', 'amount', 'closedate']
        );
        renewalDealId = String(renewalDeal.id);

        // Wire up the renewal deal to the contract, company, and contacts in
        // parallel. Each handler swallows its own error so a single bad
        // association cannot prevent the others from completing. NOTE: no
        // line items are seeded — DealHub configures the renewal product
        // set from scratch when quoting.
        await Promise.all([
          associateWithFallback(hs, contractTypeId, contract.id, '0-3', renewalDeal.id).catch((err) => {
            console.warn(`Renewal deal -> contract association failed: ${err.message}`);
          }),
          companyIds[0]
            ? associateWithFallback(hs, '0-3', renewalDeal.id, '0-2', companyIds[0]).catch((err) => {
                console.warn(`Renewal deal company association failed: ${err.message}`);
              })
            : Promise.resolve(),
          contactIds.length
            ? batchAssociateDefault(
                hs,
                '0-3',
                '0-1',
                contactIds.map((cid) => [renewalDeal.id, cid])
              ).catch((err) => {
                console.warn(`Renewal deal contacts association failed: ${err.message}`);
              })
            : Promise.resolve(),
        ]);

        console.log(
          `Renewal deal ${renewalDeal.id} auto-created for contract ${contract.id} ` +
          `(${fmtDateForHS(nextStart)} → ${fmtDateForHS(nextEnd)}, ` +
          `term=${renewalTermMonths}mo, closedate=${derivedEndDate}, ` +
          `amount=${renewalDealAmount} [last-segment ARR], ` +
          `total_tcv=${renewalTotalTcv}, year_1_arr=${renewalYear1Arr}, ` +
          `no line items seeded)`
        );
      } catch (renewalErr) {
        // Failing to spawn the renewal deal must NOT fail the contract creation.
        console.warn(`Renewal deal auto-creation failed: ${renewalErr.message}`);
      }
    } else {
      console.log(`Skipping renewal deal auto-creation; deal_category=${category} is not new_business or renewal`);
    }

    callback({
      outputFields: {
        success: 'true',
        contractId: String(contract.id),
        segmentsCreated: createdSegments,
        recurringLineItems: recurringLineItems.length,
        oneTimeLineItems,
        totalArr,
        totalTcv,
        status: determineStatus(derivedStartDate, derivedEndDate),
        renewalDealId,
        renewalDealClosedate,
        renewalLineItemsSeeded,
        renewalDealAmount,
        renewalDealTermMonths,
        contractLineItemsCopied,
        errorMessage: segmentErrors.length ? segmentErrors.slice(0, 3).join(' | ') : '',
      },
    });
  } catch (err) {
    const step = err?.__step ? `step=${err.__step}` : null;
    const detailedMessage = buildHubSpotErrorMessage(err, step);
    console.error('Workflow action failed:', {
      step: err?.__step || null,
      message: err?.message,
      status: err?.response?.status,
      url: err?.config?.url,
      method: err?.config?.method,
      data: err?.response?.data,
      stack: err?.stack,
    });
    // HubSpot workflow output fields cap below ~64 KB; trim defensively.
    const safeMessage = (detailedMessage || 'Unknown error').slice(0, 4000);
    callback({
      outputFields: {
        success: 'false',
        contractId: '',
        segmentsCreated: 0,
        recurringLineItems: 0,
        oneTimeLineItems: 0,
        totalArr: 0,
        totalTcv: 0,
        status: '',
        renewalDealId: '',
        renewalDealClosedate: '',
        renewalLineItemsSeeded: 0,
        renewalDealAmount: 0,
        renewalDealTermMonths: 0,
        contractLineItemsCopied: 0,
        errorMessage: safeMessage,
      },
    });
  }
};
