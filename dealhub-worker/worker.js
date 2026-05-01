/**
 * FinQuery — DealHub API (Cloudflare Worker)
 * --------------------------------------------------------------------------
 * Read-only HTTP API designed for DealHub to consume when building renewal,
 * expansion, and contraction proposals on FinQuery deals.
 *
 * Given a HubSpot deal ID, DealHub can fetch:
 *   - the deal itself,
 *   - the associated source contract (fq_contract),
 *   - all subscription segments (fq_subscription) grouped by contract year,
 *   - the "last period" segments (the most recent year — the seed for the
 *     next renewal proposal),
 *   - renewal-ready line items (one per product, deduped, with current
 *     quantity / unit price / SKU) ready to drop into a DealHub quote.
 *
 * Auth model:
 *   - Worker authenticates to HubSpot via Private App token (HUBSPOT_ACCESS_TOKEN secret).
 *   - The worker itself is unauthenticated. It is a demo / reference endpoint
 *     for showing DealHub the call/response/schema chain. DealHub queries
 *     HubSpot directly in production; they don't proxy through this worker.
 *
 * Endpoints (all GET, all JSON):
 *   GET /v1/health
 *   GET /v1/deals/{dealId}
 *   GET /v1/contracts/{contractId}
 *   GET /v1/contracts/{contractId}/segments
 *   GET /v1/contracts/{contractId}/renewal-line-items
 *
 * See DEALHUB_API_REFERENCE.md for full request/response schemas and examples.
 * --------------------------------------------------------------------------
 */

const HUBSPOT_API_BASE = 'https://api.hubapi.com';

// Property bags kept tight to what DealHub needs. Mirrors railway-api/server.js.
const CONTRACT_PROPS = [
  'contract_name', 'contract_number', 'sf_contract_id', 'description',
  'status', 'termination_reason',
  'startdate', 'enddate', 'co_term_date', 'activated_date', 'terminated_date',
  'amendment_start_date', 'contract_renewed_on',
  'contract_term', 'renewal_term', 'evergreen',
  'total_arr', 'lq_arr', 'fcm_arr', 'total_tcv',
  'price_cap', 'max_uplift', 'renewal_uplift_rate',
  'amendment_renewal_behavior', 'mdq_renewal_behavior',
  'renewal_forecast', 'renewal_quoted',
  'subscription_count', 'amendment_count',
  'has_legacy_products',
  'replaced_by_contract', 'replaces_contract',
  'billing_street', 'billing_city', 'billing_state', 'billing_postal_code', 'billing_country',
];

const SUBSCRIPTION_PROPS = [
  'segment_name', 'subscription_number', 'sf_subscription_id',
  'product_code', 'product_name', 'charge_type', 'billing_frequency',
  'status', 'amendment_indicator', 'revenue_type', 'bundled',
  'start_date', 'end_date',
  'arr_start_date', 'arr_end_date',
  'segment_year', 'segment_label', 'segment_index',
  'segment_start_date', 'segment_end_date',
  'quantity', 'original_quantity', 'renewal_quantity',
  'unit_price', 'list_price', 'net_price',
  'discount_percent', 'discount_amount',
  'arr', 'mrr', 'tcv',
  'renewal_price', 'renewal_uplift_rate',
];

const DEAL_PROPS = [
  'dealname', 'dealstage', 'pipeline', 'amount', 'closedate',
  'deal_category', 'revenue_type',
  'contract_start_date', 'contract_end_date',
  'hs_is_closed', 'hs_is_closed_won', 'hubspot_owner_id',
];

const COMPANY_PROPS = ['name', 'domain', 'city', 'state', 'country'];
const CONTACT_PROPS = ['firstname', 'lastname', 'email', 'jobtitle'];

// ── Entry point ────────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const cors = corsHeaders(env, request);
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    try {
      const url = new URL(request.url);
      const path = url.pathname.replace(/\/+$/, '');

      if (path === '' || path === '/') {
        return json(buildIndex(url), 200, cors);
      }

      if (path === '/v1/health' || path === '/health') {
        return json({ ok: true, service: 'finquery-dealhub-api', version: '1.0.0' }, 200, cors);
      }

      if (!env.HUBSPOT_ACCESS_TOKEN) {
        return json({ error: 'server_misconfigured', message: 'HUBSPOT_ACCESS_TOKEN secret not set' }, 500, cors);
      }

      const hs = createHubSpotClient(env.HUBSPOT_ACCESS_TOKEN);
      const types = await resolveTypeIds(hs);

      const dealMatch = path.match(/^\/v1\/deals\/([^/]+)$/);
      if (dealMatch) {
        return json(await loadDealContext(hs, types, dealMatch[1], url.searchParams), 200, cors);
      }

      const contractMatch = path.match(/^\/v1\/contracts\/([^/]+)$/);
      if (contractMatch) {
        return json(await loadContractContext(hs, types, contractMatch[1], url.searchParams), 200, cors);
      }

      const segmentsMatch = path.match(/^\/v1\/contracts\/([^/]+)\/segments$/);
      if (segmentsMatch) {
        return json(await loadContractSegments(hs, types, segmentsMatch[1]), 200, cors);
      }

      const renewalMatch = path.match(/^\/v1\/contracts\/([^/]+)\/renewal-line-items$/);
      if (renewalMatch) {
        return json(await loadRenewalLineItems(hs, types, renewalMatch[1], url.searchParams), 200, cors);
      }

      return json({ error: 'not_found', message: `Unknown route ${path}` }, 404, cors);
    } catch (err) {
      const status = err.statusCode && Number.isFinite(err.statusCode) ? err.statusCode : 500;
      const payload = {
        error: err.code || (status === 404 ? 'not_found' : 'server_error'),
        message: err.message || 'Unexpected error',
      };
      if (err.hubspot) payload.hubspot = err.hubspot;
      return json(payload, status, corsHeaders(env, request));
    }
  },
};

// ── Index ──────────────────────────────────────────────────────────────────

function buildIndex(url) {
  const base = `${url.protocol}//${url.host}`;
  return {
    service: 'finquery-dealhub-api',
    version: '1.0.0',
    description: 'Read-only HTTP API that demonstrates the FinQuery contract / subscription / line-item data flow for DealHub. Reference implementation backed by HubSpot.',
    docs: {
      fieldMappings: 'FIELD_MAPPINGS.md',
      apiReference: 'DEALHUB_API_REFERENCE.md',
      dataFlow: 'DEALHUB_DATA_FLOW.md',
    },
    endpoints: [
      { method: 'GET', path: '/v1/health', description: 'Liveness probe', example: `${base}/v1/health` },
      { method: 'GET', path: '/v1/deals/{dealId}', description: 'Full deal + contract + segments grouped by year + renewal line items + company + contacts', example: `${base}/v1/deals/123456789` },
      { method: 'GET', path: '/v1/contracts/{contractId}', description: 'Contract + segments grouped by year + renewal line items + company', example: `${base}/v1/contracts/987654321` },
      { method: 'GET', path: '/v1/contracts/{contractId}/segments', description: 'Just subscription segments grouped by contract year', example: `${base}/v1/contracts/987654321/segments` },
      { method: 'GET', path: '/v1/contracts/{contractId}/renewal-line-items', description: 'Renewal-ready line items derived from the last period (?year=N to override)', example: `${base}/v1/contracts/987654321/renewal-line-items` },
    ],
  };
}

// ── CORS ───────────────────────────────────────────────────────────────────

function corsHeaders(env, request) {
  const allowed = (env.ALLOWED_ORIGINS || '*').split(',').map((s) => s.trim()).filter(Boolean);
  const origin = request.headers.get('origin') || '';
  const allowOrigin = allowed.includes('*') || allowed.includes(origin) ? (origin || '*') : allowed[0] || '*';
  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin',
  };
}

function json(body, status = 200, cors = {}) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...cors },
  });
}

// ── HubSpot client ─────────────────────────────────────────────────────────

function createHubSpotClient(token) {
  async function request(method, path, { params, body } = {}) {
    const url = new URL(`${HUBSPOT_API_BASE}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v == null) continue;
        url.searchParams.set(k, String(v));
      }
    }
    const res = await fetch(url.toString(), {
      method,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) {
      const err = new Error(`HubSpot ${method} ${path} failed: ${res.status}`);
      err.statusCode = res.status === 404 ? 404 : 502;
      err.code = res.status === 404 ? 'not_found' : 'hubspot_error';
      try { err.hubspot = await res.json(); } catch { /* ignore */ }
      throw err;
    }
    if (res.status === 204) return null;
    return res.json();
  }
  return {
    get: (path, params) => request('GET', path, { params }),
    post: (path, body) => request('POST', path, { body }),
  };
}

let cachedTypeIds = null;

async function resolveTypeIds(hs) {
  if (cachedTypeIds) return cachedTypeIds;
  const data = await hs.get('/crm/v3/schemas');
  const out = { contractTypeId: null, subscriptionTypeId: null };
  for (const s of data.results || []) {
    if (s.name === 'fq_contract') out.contractTypeId = s.objectTypeId;
    if (s.name === 'fq_subscription') out.subscriptionTypeId = s.objectTypeId;
  }
  if (!out.contractTypeId || !out.subscriptionTypeId) {
    const err = new Error('FinQuery custom object schemas (fq_contract, fq_subscription) not found in this portal');
    err.statusCode = 500;
    err.code = 'schemas_missing';
    throw err;
  }
  cachedTypeIds = out;
  return out;
}

async function getObject(hs, typeId, objectId, properties) {
  return hs.get(`/crm/v3/objects/${typeId}/${objectId}`, { properties: properties.join(',') });
}

async function getAssociatedIds(hs, fromType, fromId, toType) {
  try {
    const data = await hs.get(`/crm/v4/objects/${fromType}/${fromId}/associations/${toType}`);
    return (data.results || []).map((r) => r.toObjectId);
  } catch (e) {
    if (e.statusCode === 404) return [];
    throw e;
  }
}

// ── Endpoint: deal context ─────────────────────────────────────────────────
//
// This is the endpoint DealHub will call most often. It takes a deal ID and
// returns everything needed to build a renewal/amendment proposal in one call.

async function loadDealContext(hs, types, dealId, searchParams) {
  const deal = await getObject(hs, '0-3', dealId, DEAL_PROPS);

  const contractIds = await getAssociatedIds(hs, '0-3', dealId, types.contractTypeId);
  const contractId = contractIds[0] || null;

  let contract = null;
  let segmentsByYear = [];
  let allSegments = [];
  let lastPeriod = null;
  let renewalLineItems = [];

  if (contractId) {
    contract = await getObject(hs, types.contractTypeId, contractId, CONTRACT_PROPS);
    const segData = await loadContractSegmentsRaw(hs, types, contractId);
    allSegments = segData.segments;
    segmentsByYear = segData.segmentsByYear;
    lastPeriod = segData.lastPeriod;
    renewalLineItems = buildRenewalLineItems(lastPeriod);
  }

  const companyIds = await getAssociatedIds(hs, '0-3', dealId, '0-2');
  let company = null;
  if (companyIds[0]) {
    try { company = await getObject(hs, '0-2', companyIds[0], COMPANY_PROPS); } catch { /* skip */ }
  }

  const contactIds = await getAssociatedIds(hs, '0-3', dealId, '0-1');
  const contacts = [];
  for (const cid of contactIds) {
    try {
      const c = await getObject(hs, '0-1', cid, CONTACT_PROPS);
      contacts.push(formatContact(c));
    } catch { /* skip */ }
  }

  const includeLineItems = searchParams.get('includeLineItems') !== 'false';
  let dealLineItems = [];
  if (includeLineItems) {
    dealLineItems = await loadDealLineItems(hs, dealId);
  }

  return {
    deal: formatDeal(deal),
    contractId,
    contract: contract ? formatContract(contract) : null,
    segmentsByYear,
    segments: allSegments,
    lastPeriod,
    renewalLineItems,
    dealLineItems,
    company: company ? formatCompany(company) : null,
    contacts,
  };
}

// ── Endpoint: contract context ────────────────────────────────────────────

async function loadContractContext(hs, types, contractId) {
  const contract = await getObject(hs, types.contractTypeId, contractId, CONTRACT_PROPS);
  const segData = await loadContractSegmentsRaw(hs, types, contractId);
  const renewalLineItems = buildRenewalLineItems(segData.lastPeriod);

  const companyIds = await getAssociatedIds(hs, types.contractTypeId, contractId, '0-2');
  let company = null;
  if (companyIds[0]) {
    try { company = await getObject(hs, '0-2', companyIds[0], COMPANY_PROPS); } catch { /* skip */ }
  }

  return {
    contract: formatContract(contract),
    segmentsByYear: segData.segmentsByYear,
    segments: segData.segments,
    lastPeriod: segData.lastPeriod,
    renewalLineItems,
    company: company ? formatCompany(company) : null,
  };
}

// ── Endpoint: segments only ───────────────────────────────────────────────

async function loadContractSegments(hs, types, contractId) {
  const segData = await loadContractSegmentsRaw(hs, types, contractId);
  return {
    contractId,
    segmentsByYear: segData.segmentsByYear,
    segments: segData.segments,
    lastPeriod: segData.lastPeriod,
  };
}

// ── Endpoint: renewal line items only ─────────────────────────────────────

async function loadRenewalLineItems(hs, types, contractId, searchParams) {
  // Caller can override which year drives the renewal seed via ?year=N.
  // Default behavior: use the last (highest) year's segments.
  const year = parseInt(searchParams.get('year'), 10);
  const segData = await loadContractSegmentsRaw(hs, types, contractId);
  const period = Number.isFinite(year) && year > 0
    ? segData.segmentsByYear.find((g) => g.year === year) || segData.lastPeriod
    : segData.lastPeriod;
  return {
    contractId,
    sourcedFromYear: period ? period.year : null,
    periodStartDate: period?.startDate || null,
    periodEndDate: period?.endDate || null,
    renewalLineItems: buildRenewalLineItems(period),
  };
}

// ── Core: load + group + dedup segments ───────────────────────────────────
//
// This is where the contract-card grouping logic lives. We mirror the JSX
// card so DealHub sees the same year groupings the FinQuery CSM sees.

async function loadContractSegmentsRaw(hs, types, contractId) {
  const subIds = await getAssociatedIds(hs, types.contractTypeId, contractId, types.subscriptionTypeId);
  if (subIds.length === 0) {
    return { segments: [], segmentsByYear: [], lastPeriod: null };
  }

  const fetched = await Promise.all(
    subIds.map((id) => getObject(hs, types.subscriptionTypeId, id, SUBSCRIPTION_PROPS).catch(() => null))
  );
  const segments = fetched.filter(Boolean).map(formatSegment);

  const contract = await getObject(hs, types.contractTypeId, contractId, ['startdate', 'enddate']);
  const contractStart = parseDate(contract?.properties?.startdate);

  const segmentsByYear = groupSegmentsByYear(segments, contractStart);
  const lastPeriod = pickLastPeriod(segmentsByYear);

  return { segments, segmentsByYear, lastPeriod };
}

function groupSegmentsByYear(segments, contractStart) {
  const MIN_SEGMENT_DAYS = 14;
  const groups = new Map();

  // Sort by start date for deterministic grouping fallback (segments without
  // a start date end up at the tail).
  const sorted = [...segments].sort((a, b) => {
    const as = parseDate(a.startDate);
    const bs = parseDate(b.startDate);
    return (as ? as.getTime() : Infinity) - (bs ? bs.getTime() : Infinity);
  });

  // Fallback year assignment when contract.startdate is missing — assign each
  // unique [start,end] window a sequential year number.
  const rangeYearMap = new Map();
  let nextDerivedYear = 1;
  for (const seg of sorted) {
    const key = `${seg.startDate || 'na'}-${seg.endDate || 'na'}`;
    if (!rangeYearMap.has(key)) rangeYearMap.set(key, nextDerivedYear++);
  }

  for (const seg of sorted) {
    const start = parseDate(seg.startDate);
    const end = parseDate(seg.endDate);

    // Skip micro-segments — known artifact of Salesforce-imported data where a
    // line item ends 1 day past the proper year boundary, producing a 1-day
    // duplicate "Year 2" segment. Anything <14 days isn't a real annual seg.
    if (start && end) {
      const days = Math.round((end - start) / 86400000) + 1;
      if (days > 0 && days < MIN_SEGMENT_DAYS) continue;
    }

    let year = null;
    if (start && contractStart) {
      year = computeContractYearForDate(start, contractStart);
    }
    if (!year) {
      const rangeKey = `${seg.startDate || 'na'}-${seg.endDate || 'na'}`;
      year = seg.segmentYear || seg.segmentIndex || rangeYearMap.get(rangeKey) || 1;
    }

    if (!groups.has(year)) {
      groups.set(year, {
        year,
        label: `Year ${year}`,
        startDate: null,
        endDate: null,
        totalArr: 0,
        totalMrr: 0,
        totalTcv: 0,
        segmentCount: 0,
        productCodes: new Set(),
        segments: [],
      });
    }
    const g = groups.get(year);
    g.segments.push(seg);
    g.segmentCount += 1;
    g.totalArr += Number(seg.arr) || 0;
    g.totalMrr += Number(seg.mrr) || 0;
    g.totalTcv += Number(seg.tcv) || 0;
    if (seg.productCode) g.productCodes.add(seg.productCode);
    if (start && (!g.startDate || start < parseDate(g.startDate))) g.startDate = seg.startDate;
    if (end && (!g.endDate || end > parseDate(g.endDate))) g.endDate = seg.endDate;
  }

  const today = stripTime(new Date());
  const sortedGroups = Array.from(groups.values()).sort((a, b) => a.year - b.year);

  for (const g of sortedGroups) {
    g.productCodes = Array.from(g.productCodes);
    const start = parseDate(g.startDate);
    const end = parseDate(g.endDate);
    g.isCurrent = !!(start && end && start <= today && end >= today);
    g.totalArr = round2(g.totalArr);
    g.totalMrr = round2(g.totalMrr);
    g.totalTcv = round2(g.totalTcv);
  }

  return sortedGroups;
}

// "Last period" = the highest-year group with at least one inheritable segment.
// Falls back to the latest group regardless of status if everything is
// terminated (DealHub can decide whether to use it for context).
function pickLastPeriod(segmentsByYear) {
  if (segmentsByYear.length === 0) return null;
  for (let i = segmentsByYear.length - 1; i >= 0; i--) {
    const g = segmentsByYear[i];
    if (g.segments.some(isInheritable)) return g;
  }
  return segmentsByYear[segmentsByYear.length - 1];
}

function isInheritable(seg) {
  const status = String(seg.status || '').toLowerCase();
  if (status === 'active' || status === 'future') return true;
  if (!status) {
    // Imported/legacy segments without a status — inheritable when end date
    // hasn't passed yet.
    const end = parseDate(seg.endDate);
    if (!end) return true;
    return end >= stripTime(new Date());
  }
  return false;
}

// ── Build renewal line items ──────────────────────────────────────────────
//
// One line item per product. Mirrors the renewal-deal seeding logic from
// railway-api/server.js (syncContractRecurringLineItemsToDeal):
//
//   1. Filter to inheritable segments (active/future, or running with no status)
//   2. One per product (highest segment_year wins inside the period)
//   3. Compute unit_price from arr/quantity if missing
//   4. Map billing_frequency -> dh_duration (months: 12, 1, 3, 6)
//
// DealHub takes this array and uses it as the seed for their renewal proposal.
function buildRenewalLineItems(period) {
  if (!period) return [];

  const inheritable = period.segments.filter(isInheritable);
  const byProduct = new Map();
  for (const seg of inheritable) {
    const key = (seg.productCode || seg.productName || `__sub_${seg.id}`).toLowerCase();
    const existing = byProduct.get(key);
    if (!existing) {
      byProduct.set(key, seg);
      continue;
    }
    // Prefer the segment with the higher segment_year, then later start date.
    const a = existing;
    const b = seg;
    const yearDiff = (b.segmentYear || 0) - (a.segmentYear || 0);
    if (yearDiff > 0) byProduct.set(key, b);
    else if (yearDiff === 0) {
      const aStart = parseDate(a.startDate)?.getTime() || 0;
      const bStart = parseDate(b.startDate)?.getTime() || 0;
      if (bStart > aStart) byProduct.set(key, b);
    }
  }

  return Array.from(byProduct.values()).map((seg) => {
    const quantity = Math.max(1, Number(seg.renewalQuantity || seg.quantity || seg.originalQuantity || 1));
    const arr = Number(seg.arr) || 0;
    const mrr = Number(seg.mrr) || 0;
    const explicitUnitPrice = Number(seg.unitPrice) || 0;
    const annualAmount = arr > 0 ? arr : mrr * 12;
    const unitPrice = explicitUnitPrice > 0
      ? explicitUnitPrice
      : (quantity > 0 ? annualAmount / quantity : annualAmount);

    return {
      sourceSegmentId: seg.id,
      productCode: seg.productCode || null,
      productName: seg.productName || seg.productCode || 'Subscription',
      sku: seg.productCode || null,
      quantity,
      unitPrice: round2(unitPrice),
      lineAmount: round2(unitPrice * quantity),
      currency: 'USD',
      billingFrequency: seg.billingFrequency || 'annual',
      duration: billingFrequencyToMonths(seg.billingFrequency),
      productTag: 'Recurring',
      revenueType: 'renewal',
      sourceArr: round2(arr),
      sourceMrr: round2(mrr),
      sourceSegmentYear: seg.segmentYear || null,
      sourceSegmentLabel: seg.segmentLabel || null,
    };
  });
}

function billingFrequencyToMonths(billingFrequency) {
  const v = String(billingFrequency || '').toLowerCase();
  if (v === 'monthly') return 1;
  if (v === 'quarterly') return 3;
  if (v === 'semiannual' || v === 'semi-annual' || v === 'semi_annual') return 6;
  if (v === 'annual' || v === 'yearly' || v === '') return 12;
  return 12;
}

// ── Deal line items ───────────────────────────────────────────────────────

async function loadDealLineItems(hs, dealId) {
  const ids = await getAssociatedIds(hs, '0-3', dealId, 'line_items');
  if (ids.length === 0) return [];
  const fetched = await Promise.all(
    ids.map((id) => getObject(hs, 'line_items', id, [
      'name', 'description', 'dh_quantity', 'price', 'amount', 'hs_sku',
      'dh_duration', 'product_tag', 'hs_recurring_billing_start_date',
      'revenue_type',
    ]).catch(() => null))
  );
  return fetched.filter(Boolean).map((li) => {
    const p = li.properties || {};
    const durationMonths = numOrNull(p.dh_duration);
    const productTag = String(p.product_tag || '').trim();
    const tagLower = productTag.toLowerCase().replace(/[\s_]+/g, '-');
    let isRecurring;
    if (tagLower === 'recurring' || tagLower === 'subscription') {
      isRecurring = true;
    } else if (tagLower === 'one-time' || tagLower === 'onetime' || tagLower === 'ad-hoc' || tagLower === 'adhoc') {
      isRecurring = false;
    } else {
      isRecurring = durationMonths !== null && durationMonths > 0;
    }
    return {
      id: li.id,
      name: p.name || null,
      sku: p.hs_sku || null,
      description: p.description || null,
      quantity: numOrNull(p.dh_quantity),
      unitPrice: numOrNull(p.price),
      amount: numOrNull(p.amount),
      duration: durationMonths,
      productTag: productTag || null,
      recurringBillingStartDate: p.hs_recurring_billing_start_date || null,
      revenueType: p.revenue_type || null,
      isRecurring,
    };
  });
}

// ── Formatters ────────────────────────────────────────────────────────────

function formatDeal(deal) {
  const p = deal.properties || {};
  return {
    id: deal.id,
    name: p.dealname || null,
    stage: p.dealstage || null,
    pipeline: p.pipeline || null,
    amount: numOrNull(p.amount),
    closeDate: p.closedate || null,
    category: p.deal_category || null,
    revenueType: p.revenue_type || null,
    contractStartDate: p.contract_start_date || null,
    contractEndDate: p.contract_end_date || null,
    isClosed: p.hs_is_closed === 'true',
    isClosedWon: p.hs_is_closed_won === 'true',
    ownerId: p.hubspot_owner_id || null,
  };
}

function formatContract(contract) {
  const p = contract.properties || {};
  return {
    id: contract.id,
    name: p.contract_name || null,
    contractNumber: p.contract_number || null,
    salesforceId: p.sf_contract_id || null,
    description: p.description || null,
    status: p.status || null,
    terminationReason: p.termination_reason || null,
    startDate: p.startdate || null,
    endDate: p.enddate || null,
    coTermDate: p.co_term_date || null,
    activatedDate: p.activated_date || null,
    terminatedDate: p.terminated_date || null,
    amendmentStartDate: p.amendment_start_date || null,
    contractRenewedOn: p.contract_renewed_on || null,
    contractTerm: numOrNull(p.contract_term),
    renewalTerm: numOrNull(p.renewal_term),
    evergreen: p.evergreen === 'true',
    totalArr: numOrNull(p.total_arr),
    totalTcv: numOrNull(p.total_tcv),
    arrByProduct: {
      LQ: numOrNull(p.lq_arr),
      FCM: numOrNull(p.fcm_arr),
    },
    priceCap: numOrNull(p.price_cap),
    maxUplift: numOrNull(p.max_uplift),
    renewalUpliftRate: numOrNull(p.renewal_uplift_rate),
    amendmentRenewalBehavior: p.amendment_renewal_behavior || null,
    mdqRenewalBehavior: p.mdq_renewal_behavior || null,
    renewalForecast: p.renewal_forecast === 'true',
    renewalQuoted: p.renewal_quoted === 'true',
    subscriptionCount: numOrNull(p.subscription_count),
    amendmentCount: numOrNull(p.amendment_count),
    hasLegacyProducts: p.has_legacy_products === 'true',
    replacedByContract: p.replaced_by_contract || null,
    replacesContract: p.replaces_contract || null,
    billingAddress: {
      street: p.billing_street || null,
      city: p.billing_city || null,
      state: p.billing_state || null,
      postalCode: p.billing_postal_code || null,
      country: p.billing_country || null,
    },
  };
}

function formatSegment(seg) {
  const p = seg.properties || {};
  return {
    id: seg.id,
    segmentName: p.segment_name || null,
    subscriptionNumber: p.subscription_number || null,
    salesforceId: p.sf_subscription_id || null,
    productCode: p.product_code || null,
    productName: p.product_name || null,
    chargeType: p.charge_type || null,
    billingFrequency: p.billing_frequency || null,
    status: p.status || null,
    amendmentIndicator: p.amendment_indicator || null,
    revenueType: p.revenue_type || null,
    bundled: p.bundled === 'true',
    segmentYear: numOrNull(p.segment_year),
    segmentLabel: p.segment_label || null,
    segmentIndex: numOrNull(p.segment_index),
    startDate: p.arr_start_date || p.segment_start_date || p.start_date || null,
    endDate: p.arr_end_date || p.segment_end_date || p.end_date || null,
    quantity: numOrNull(p.quantity),
    originalQuantity: numOrNull(p.original_quantity),
    renewalQuantity: numOrNull(p.renewal_quantity),
    unitPrice: numOrNull(p.unit_price),
    listPrice: numOrNull(p.list_price),
    netPrice: numOrNull(p.net_price),
    discountPercent: numOrNull(p.discount_percent),
    discountAmount: numOrNull(p.discount_amount),
    arr: numOrNull(p.arr),
    mrr: numOrNull(p.mrr),
    tcv: numOrNull(p.tcv),
    renewalPrice: numOrNull(p.renewal_price),
    renewalUpliftRate: numOrNull(p.renewal_uplift_rate),
  };
}

function formatCompany(company) {
  const p = company.properties || {};
  return {
    id: company.id,
    name: p.name || null,
    domain: p.domain || null,
    city: p.city || null,
    state: p.state || null,
    country: p.country || null,
  };
}

function formatContact(contact) {
  const p = contact.properties || {};
  return {
    id: contact.id,
    firstName: p.firstname || null,
    lastName: p.lastname || null,
    fullName: [p.firstname, p.lastname].filter(Boolean).join(' ') || null,
    email: p.email || null,
    title: p.jobtitle || null,
  };
}

// ── Misc helpers ──────────────────────────────────────────────────────────

function parseDate(value) {
  if (!value) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function stripTime(date) {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  return d;
}

function computeContractYearForDate(segmentStart, contractStart) {
  if (!(segmentStart instanceof Date) || !(contractStart instanceof Date)) return null;
  const yearsDiff = segmentStart.getFullYear() - contractStart.getFullYear();
  const monthsDiff = segmentStart.getMonth() - contractStart.getMonth();
  const daysDiff = segmentStart.getDate() - contractStart.getDate();
  let totalMonths = yearsDiff * 12 + monthsDiff;
  if (daysDiff < 0) totalMonths -= 1;
  const year = Math.floor(totalMonths / 12) + 1;
  return year >= 1 ? year : 1;
}

function numOrNull(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round2(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}
