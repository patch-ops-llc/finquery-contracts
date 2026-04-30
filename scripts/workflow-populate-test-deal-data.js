/**
 * HubSpot Workflow Custom Code Action
 *
 * Purpose:
 *   Populate a freshly-created sales deal with realistic FinQuery test data:
 *     - Fake company (with billing address + ARR-friendly defaults)
 *     - 1-3 fake contacts (primary contact + optional billing/exec contacts)
 *     - Recurring line items (LeaseQuery + FCM products from the FinQuery
 *       PRODUCT_REGISTRY) plus an optional one-time implementation fee
 *     - Sensible deal property defaults for a "new business" deal
 *       (deal_category, contract_start_date, contract_end_date, revenue_type,
 *       amount, closedate)
 *     - All associations: company <-> deal, contacts <-> deal, contacts <->
 *       company, line items <-> deal
 *
 *   The action is idempotent at the deal level: if the deal already has a
 *   primary company associated, no new company is created and the existing
 *   one is reused. Line items, however, are always added so re-running will
 *   stack them; gate the workflow with a "ran before" custom property if you
 *   want strict one-shot behavior.
 *
 * Workflow setup:
 *   1) Trigger: deal-based workflow ("when a deal is created" filter on a
 *      pipeline / stage of your choosing -- e.g. a Sandbox pipeline).
 *   2) Action: Custom code -> paste this file's contents.
 *   3) Secret: PRIVATE_APP_TOKEN
 *        (a Private App token with: crm.objects.deals.read/write,
 *         crm.objects.companies.read/write, crm.objects.contacts.read/write,
 *         crm.objects.line_items.read/write, crm.schemas.deals.read,
 *         crm.objects.owners.read).
 *   4) Property to include in code (read by the action):
 *        - hs_object_id  (deal ID -- already implicit, but required to be
 *          included in the "Properties to include in code" list)
 *   5) Optional input fields (any can be left blank):
 *        - seed (string)              : Override the PRNG seed (defaults to
 *                                       the deal ID -- same deal -> same data)
 *        - persona (string)           : "smb" | "midmarket" | "enterprise"
 *                                       (default: deterministic by seed)
 *        - dealAmount (number)        : Override total deal amount.
 *        - productMix (string)        : "lq" | "fcm" | "both" (default:
 *                                       persona-driven)
 *        - termMonths (number)        : Contract term in months (default: 36
 *                                       so multi-year segment logic gets
 *                                       exercised by default).
 *        - includeOneTime (string)    : "true"/"false" -- add an implementation
 *                                       fee one-time line item (default: true)
 *        - companyName (string)       : Override company name (default: random)
 *        - numContacts (number)       : 1, 2, or 3 (default: random by seed)
 *
 *   Recurring products are emitted as one line item PER 12-month segment year
 *   (so a 36-month term produces 3 LQ + 3 FCM line items, each tagged with
 *   its own hs_recurring_billing_start_date / hs_recurring_billing_end_date).
 *   This mirrors the MDQ pattern the contract object expects -- one line item
 *   per product per segment year -- and lets the contract card render
 *   multi-year subscription segments correctly.
 *   6) Output fields:
 *        - success (string)
 *        - companyId (string)
 *        - contactIds (string)        : comma-separated
 *        - lineItemIds (string)       : comma-separated
 *        - lineItemsCreated (number)
 *        - dealAmount (number)
 *        - persona (string)           : which persona was used
 *        - seedSource (string)        : the actual seed value used
 *        - errorMessage (string)
 *
 * Seeding model:
 *   All randomness flows through a seeded mulberry32 PRNG keyed off the
 *   deal ID (or the `seed` input override). This means:
 *     - Re-running on the same deal produces the SAME company/contacts/etc.
 *     - Different deals produce DIFFERENT but realistic data.
 *     - Pass an explicit `seed` to reproduce a specific bug across deals.
 */

const axios = require('axios');

// ── Constants ───────────────────────────────────────────────────────────────

const TYPE = {
  DEAL: '0-3',
  COMPANY: '0-2',
  CONTACT: '0-1',
  LINE_ITEM: '0-8',
};

// Mirrors PRODUCT_REGISTRY in railway-api/server.js.
const PRODUCTS = {
  LQ: {
    code: 'LQ',
    name: 'LeaseQuery',
    sku: 'LQ-CORE',
    unitPrice: 18000,
    quantity: 1,
    recurring: true,
  },
  FCM: {
    code: 'FCM',
    name: 'FinQuery Contract Management',
    sku: 'FCM-CORE',
    unitPrice: 24000,
    quantity: 1,
    recurring: true,
  },
  IMPLEMENTATION: {
    code: 'IMPL',
    name: 'Implementation & Onboarding',
    sku: 'IMPL-ONETIME',
    unitPrice: 5000,
    quantity: 1,
    recurring: false,
  },
};

const FIRST_NAMES = [
  'Alex', 'Jordan', 'Taylor', 'Morgan', 'Riley', 'Casey', 'Quinn',
  'Avery', 'Sydney', 'Cameron', 'Reese', 'Hayden', 'Parker', 'Drew',
  'Skyler', 'Rowan', 'Emerson', 'Blair', 'Kendall', 'Sasha',
];
const LAST_NAMES = [
  'Patel', 'Nguyen', 'Garcia', 'Smith', 'Johnson', 'Lee', 'Brown',
  'Davis', 'Miller', 'Wilson', 'Anderson', 'Thomas', 'Martinez', 'Clark',
  'Robinson', 'Walker', 'Hall', 'Young', 'King', 'Wright',
];
const COMPANY_PREFIXES = [
  'Northwind', 'Cascade', 'Beacon', 'Summit', 'Harbor', 'Pioneer',
  'Atlas', 'Vertex', 'Cobalt', 'Granite', 'Lumen', 'Catalyst',
  'Meridian', 'Ironwood', 'Aurora', 'Redleaf', 'Halcyon', 'Bluestone',
];
const COMPANY_SUFFIXES = [
  'Logistics', 'Holdings', 'Industries', 'Partners', 'Capital',
  'Solutions', 'Group', 'Networks', 'Systems', 'Labs',
  'Ventures', 'Works', 'Collective', 'Co',
];
const STREET_NAMES = [
  'Main', 'Oak', 'Elm', 'Lakeview', 'Congress', 'Cedar',
  'Pine', 'Maple', 'Mission', 'Industrial',
];
const CITIES = [
  { city: 'Austin', state: 'TX', zip: '78701', areaCode: '512' },
  { city: 'Denver', state: 'CO', zip: '80202', areaCode: '303' },
  { city: 'Chicago', state: 'IL', zip: '60601', areaCode: '312' },
  { city: 'Atlanta', state: 'GA', zip: '30303', areaCode: '404' },
  { city: 'Boston', state: 'MA', zip: '02110', areaCode: '617' },
  { city: 'Seattle', state: 'WA', zip: '98101', areaCode: '206' },
  { city: 'Nashville', state: 'TN', zip: '37203', areaCode: '615' },
  { city: 'Minneapolis', state: 'MN', zip: '55401', areaCode: '612' },
];
const JOB_TITLES = [
  'CFO', 'VP of Finance', 'Controller', 'Director of Accounting',
  'AP Manager', 'Procurement Lead', 'Lease Accounting Manager',
  'Director of FP&A', 'Senior Accountant',
];

// Personas keep the generated data internally consistent: an "enterprise"
// company gets enterprise-sized employee count, ARR, and deal amount, not
// a random mismatch (e.g. 75 employees + $50M deal).
const PERSONAS = [
  {
    key: 'smb',
    industry: 'COMPUTER_SOFTWARE',
    employees: [25, 200],
    annualRevenue: [3_000_000, 25_000_000],
    productMix: ['LQ'],
    contractMonths: [12, 12],
    discountPct: [0, 5],
  },
  {
    key: 'midmarket',
    industry: 'FINANCIAL_SERVICES',
    employees: [200, 1500],
    annualRevenue: [25_000_000, 250_000_000],
    productMix: ['LQ', 'FCM'],
    contractMonths: [12, 24],
    discountPct: [0, 10],
  },
  {
    key: 'enterprise',
    industry: 'RETAIL',
    employees: [1500, 15000],
    annualRevenue: [250_000_000, 5_000_000_000],
    productMix: ['LQ', 'FCM'],
    contractMonths: [24, 36],
    discountPct: [5, 15],
  },
];

// ── Seeded PRNG ─────────────────────────────────────────────────────────────
// `mulberry32` -- tiny seedable PRNG. Same seed -> same sequence, every time.
// Keyed off the deal ID (or an explicit `seed` input) so re-runs on the
// same deal produce the exact same fake data, while different deals get
// different data.

function hashStringToSeed(str) {
  // FNV-1a 32-bit -- deterministic, no deps, >>>0 keeps it unsigned.
  let h = 0x811c9dc5;
  const s = String(str);
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function makeRng(seed) {
  let a = (seed >>> 0) || 1;
  return function rng() {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── Helpers (all seeded) ────────────────────────────────────────────────────

function pick(rng, arr) {
  return arr[Math.floor(rng() * arr.length)];
}

function randomInt(rng, min, max) {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function randomFloat(rng, min, max) {
  return rng() * (max - min) + min;
}

function fmtDate(date) {
  const d = date instanceof Date ? date : new Date(date);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function addMonths(date, months) {
  const d = new Date(date);
  d.setUTCMonth(d.getUTCMonth() + months);
  return d;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  const v = String(value).trim().toLowerCase();
  if (['true', 'yes', '1', 'y'].includes(v)) return true;
  if (['false', 'no', '0', 'n'].includes(v)) return false;
  return fallback;
}

function describeHubSpotError(err) {
  const data = err?.response?.data;
  if (!data) return err?.message || 'Unknown error';
  if (typeof data === 'string') return data;
  if (data.message) {
    const ctx = data.errors?.length ? ` :: ${JSON.stringify(data.errors)}` : '';
    return `${data.message}${ctx}`;
  }
  try { return JSON.stringify(data); } catch { return err.message; }
}

// ── HubSpot API wrappers ────────────────────────────────────────────────────

// HubSpot Private Apps cap at 100 req / 10 sec. Workflow custom code actions
// run in parallel for batched enrollments, so even modest test-data workflows
// can trip 429s. We retry 429 / 502 / 503 / 504 with exponential backoff and
// honor `Retry-After`. Workflow actions have a ~20s wall clock so we cap delay.
const HS_MAX_RETRIES = 4;
const HS_BASE_DELAY_MS = 400;
const HS_MAX_DELAY_MS = 4000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryHubSpot(error) {
  if (!error || !error.config) return false;
  const status = error.response?.status;
  if (status === 429) return true;
  if (status === 502 || status === 503 || status === 504) return true;
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
      return Math.min(seconds * 1000, HS_MAX_DELAY_MS);
    }
  }
  const exp = HS_BASE_DELAY_MS * Math.pow(2, attempt);
  const jitter = Math.floor(Math.random() * 200);
  return Math.min(exp + jitter, HS_MAX_DELAY_MS);
}

function createClient(token) {
  const client = axios.create({
    baseURL: 'https://api.hubapi.com',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    timeout: 15000,
  });

  client.interceptors.response.use(
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
      return client.request(config);
    }
  );

  return client;
}

async function getObject(hs, typeId, objectId, properties = []) {
  const params = properties.length ? { properties: properties.join(',') } : {};
  const { data } = await hs.get(`/crm/v3/objects/${typeId}/${objectId}`, { params });
  return data;
}

async function updateObject(hs, typeId, objectId, properties) {
  const { data } = await hs.patch(`/crm/v3/objects/${typeId}/${objectId}`, { properties });
  return data;
}

async function createObject(hs, typeId, properties, associations) {
  const body = { properties };
  if (associations && associations.length) body.associations = associations;
  const { data } = await hs.post(`/crm/v3/objects/${typeId}`, body);
  return data;
}

// Line-item creation can fail with HTTP 400 when the portal hasn't
// provisioned a property we tried to set (most commonly the custom
// `start_date` / `end_date` fields, or `revenue_type` on a fresh test
// portal). Strip the offending property from the payload and retry rather
// than failing the entire workflow run. Each retry walks the message,
// removes any property whose name appears in the error, and tries again
// until either the create succeeds or no more properties can be stripped.
const OPTIONAL_LINE_ITEM_PROPS = [
  'start_date',
  'end_date',
  'hs_recurring_billing_start_date',
  'hs_recurring_billing_end_date',
  'hs_recurring_billing_period',
  'hs_recurring_billing_number_of_payments',
  'recurringbillingfrequency',
  'revenue_type',
];

async function createLineItem(hs, properties) {
  let attemptProps = { ...properties };
  let lastError;
  for (let attempt = 0; attempt < OPTIONAL_LINE_ITEM_PROPS.length + 1; attempt++) {
    try {
      return await createObject(hs, TYPE.LINE_ITEM, attemptProps);
    } catch (err) {
      lastError = err;
      if (err?.response?.status !== 400) throw err;
      const message = JSON.stringify(err?.response?.data || err?.message || '').toLowerCase();
      const offender = OPTIONAL_LINE_ITEM_PROPS.find(
        (key) => message.includes(key) && Object.prototype.hasOwnProperty.call(attemptProps, key),
      );
      if (!offender) throw err;
      console.warn(
        `[populate-test-deal-data] Stripping unsupported line item property "${offender}" and retrying.`,
      );
      const { [offender]: _omit, ...rest } = attemptProps;
      attemptProps = rest;
    }
  }
  throw lastError;
}

async function getAssociatedIds(hs, fromType, fromId, toType) {
  try {
    const { data } = await hs.get(`/crm/v4/objects/${fromType}/${fromId}/associations/${toType}`);
    return (data.results || []).map((r) => String(r.toObjectId));
  } catch (err) {
    if (err?.response?.status === 404) return [];
    throw err;
  }
}

async function associateDefault(hs, fromType, fromId, toType, toId) {
  await hs.put(`/crm/v4/objects/${fromType}/${fromId}/associations/default/${toType}/${toId}`);
}

// ── Test-data builders ──────────────────────────────────────────────────────

function buildCompanyProperties(rng, persona, overrideName) {
  const name = overrideName
    || `${pick(rng, COMPANY_PREFIXES)} ${pick(rng, COMPANY_SUFFIXES)} ${randomInt(rng, 100, 999)}`;
  const loc = pick(rng, CITIES);
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '');
  return {
    name,
    domain: `${slug}.example.com`,
    industry: persona.industry,
    type: 'PROSPECT',
    numberofemployees: String(randomInt(rng, persona.employees[0], persona.employees[1])),
    annualrevenue: String(randomInt(rng, persona.annualRevenue[0], persona.annualRevenue[1])),
    phone: `+1 ${loc.areaCode} 555 ${String(randomInt(rng, 1000, 9999))}`,
    address: `${randomInt(rng, 100, 9999)} ${pick(rng, STREET_NAMES)} St`,
    city: loc.city,
    state: loc.state,
    zip: loc.zip,
    country: 'United States',
    description: `Auto-generated test company (${persona.key} persona, FinQuery sandbox).`,
  };
}

function buildContactProperties(rng, companyName, companyDomain, areaCode, isPrimary) {
  const first = pick(rng, FIRST_NAMES);
  const last = pick(rng, LAST_NAMES);
  const slug = `${first}.${last}.${randomInt(rng, 100, 999)}`.toLowerCase();
  const email = `${slug}@${companyDomain}`;
  return {
    firstname: first,
    lastname: last,
    email,
    phone: `+1 ${areaCode || '512'} 555 ${String(randomInt(rng, 1000, 9999))}`,
    jobtitle: isPrimary ? 'CFO' : pick(rng, JOB_TITLES),
    company: companyName,
    lifecyclestage: 'opportunity',
  };
}

// Builds a single line item. Recurring products are split into one line per
// 12-month segment so multi-year contracts produce the MDQ pattern (one line
// item per product per segment year). Each line item -- recurring AND
// one-time -- gets its own start/end dates written to BOTH the HubSpot
// system fields (`hs_recurring_billing_start_date` /
// `hs_recurring_billing_end_date`) AND the simple `start_date` / `end_date`
// custom properties, so whichever set the portal exposes is populated.
//
// CRITICAL: do NOT set `hs_recurring_billing_period` and
// `hs_recurring_billing_number_of_payments` together. When both are present
// HubSpot recomputes `hs_recurring_billing_end_date` as
// `start + period × number_of_payments`, which leaves the persisted end date
// 1 day past the intended term boundary (e.g. 2027-05-16 instead of
// 2027-05-15 for a 1-year line that started 2026-05-16). Downstream the
// contract object's `buildYearSegments` then emits a bogus single-day
// "Year 2" segment per line item — the exact bug the contract card was
// surfacing on Apr 30 test contracts.
function buildLineItemProperties(product, lineStart, lineEnd, segmentYear, totalSegments) {
  const amount = product.unitPrice * product.quantity;
  const isMultiSegment = product.recurring && totalSegments > 1;
  const yearSuffix = isMultiSegment ? ` (Year ${segmentYear} of ${totalSegments})` : '';
  const startStr = fmtDate(lineStart);
  const endStr = fmtDate(lineEnd);
  const props = {
    name: `${product.name}${yearSuffix}`,
    hs_sku: product.sku,
    description: `Auto-generated test ${product.name}${yearSuffix}`,
    quantity: String(product.quantity),
    price: String(product.unitPrice),
    amount: String(amount),
    hs_line_item_currency_code: 'USD',
    revenue_type: 'new',

    // HubSpot system date fields (used by the contract object's segment
    // derivation logic via resolveLineItemSpan).
    hs_recurring_billing_start_date: startStr,
    hs_recurring_billing_end_date: endStr,

    // Simple custom date fields — populated alongside the system fields so
    // either set works for downstream consumers.
    start_date: startStr,
    end_date: endStr,
  };
  if (product.recurring) {
    // Period only — number_of_payments intentionally omitted (see header).
    props.hs_recurring_billing_period = 'P12M';
  } else {
    // Mark one-time charges (Implementation, Onboarding, Setup, etc.)
    // unambiguously. The contract creation logic uses `recurringbillingfrequency`
    // and the line item name to filter recurring vs one-time, but the explicit
    // marker means we don't depend on name-pattern matching.
    props.recurringbillingfrequency = 'one_time';
  }
  return props;
}

// Produces a list of line-item descriptors for the deal. Recurring products
// are exploded into per-year segments; one-time products produce a single
// entry dated to the contract start.
function buildLineItemPlan(products, contractStart, contractEnd, termMonths) {
  const totalSegments = Math.max(1, Math.ceil(termMonths / 12));
  const plan = [];
  for (const product of products) {
    if (!product.recurring) {
      plan.push({
        product,
        lineStart: contractStart,
        lineEnd: contractStart,
        segmentYear: 1,
        totalSegments: 1,
      });
      continue;
    }
    let segmentStart = new Date(contractStart);
    for (let year = 1; year <= totalSegments; year++) {
      const naiveSegmentEnd = addDays(addMonths(segmentStart, 12), -1);
      const segmentEnd = naiveSegmentEnd > contractEnd ? new Date(contractEnd) : naiveSegmentEnd;
      plan.push({
        product,
        lineStart: new Date(segmentStart),
        lineEnd: new Date(segmentEnd),
        segmentYear: year,
        totalSegments,
      });
      segmentStart = addDays(segmentEnd, 1);
      if (segmentStart > contractEnd) break;
    }
  }
  return plan;
}

function chooseProductMix(productMix, persona) {
  const mix = String(productMix || '').trim().toLowerCase();
  if (mix === 'lq') return [PRODUCTS.LQ];
  if (mix === 'fcm') return [PRODUCTS.FCM];
  if (mix === 'both') return [PRODUCTS.LQ, PRODUCTS.FCM];
  return persona.productMix.map((code) => PRODUCTS[code]).filter(Boolean);
}

// ── Main ────────────────────────────────────────────────────────────────────

exports.main = async (event, callback) => {
  const token = process.env.PRIVATE_APP_TOKEN || process.env.HUBSPOT_ACCESS_TOKEN;
  if (!token) {
    return callback({
      outputFields: {
        success: 'false',
        errorMessage: 'Missing PRIVATE_APP_TOKEN secret on the workflow action.',
      },
    });
  }

  const inputs = event.inputFields || {};
  const dealId = String(
    inputs.dealId
      || inputs.hs_object_id
      || event.object?.objectId
      || ''
  ).trim();

  if (!dealId) {
    return callback({
      outputFields: {
        success: 'false',
        errorMessage: 'Could not resolve dealId from event.object.objectId or inputs.',
      },
    });
  }

  const hs = createClient(token);

  // ── Seed the RNG ──────────────────────────────────────────────────────
  // Use the explicit `seed` input if provided, otherwise hash the deal ID.
  // Same seed -> same fake data. Different deals -> different data.
  const seedSource = String(inputs.seed || dealId);
  const rng = makeRng(hashStringToSeed(seedSource));

  try {
    // ── Resolve deal + existing associations ─────────────────────────────
    const deal = await getObject(hs, TYPE.DEAL, dealId, [
      'dealname', 'amount', 'closedate', 'pipeline', 'dealstage',
      'deal_category', 'contract_start_date', 'contract_end_date',
    ]);
    const dealProps = deal.properties || {};

    const [existingCompanies, existingContacts] = await Promise.all([
      getAssociatedIds(hs, TYPE.DEAL, dealId, TYPE.COMPANY),
      getAssociatedIds(hs, TYPE.DEAL, dealId, TYPE.CONTACT),
    ]);

    // ── Persona + inputs / defaults ───────────────────────────────────────
    const persona = inputs.persona
      ? (PERSONAS.find((p) => p.key === String(inputs.persona).toLowerCase()) || pick(rng, PERSONAS))
      : pick(rng, PERSONAS);

    const productMix = chooseProductMix(inputs.productMix, persona);
    const includeOneTime = parseBool(inputs.includeOneTime, true);
    const numContacts = Math.min(3, Math.max(1,
      Number(inputs.numContacts) || randomInt(rng, 1, 3)));
    // Default to 36 months (3 years) so the multi-year segment logic gets
    // exercised on every fresh test deal. Personas can still bias shorter
    // terms via the `termMonths` input override.
    const termMonths = Math.max(1, Number(inputs.termMonths) || 36);
    const overrideAmount = Number(inputs.dealAmount);

    const today = new Date();
    const contractStart = addDays(today, randomInt(rng, 7, 45));
    const contractEnd = addDays(addMonths(contractStart, termMonths), -1);

    const products = [...productMix];
    if (includeOneTime) products.push(PRODUCTS.IMPLEMENTATION);

    const lineItemPlan = buildLineItemPlan(products, contractStart, contractEnd, termMonths);

    const discountPct = randomFloat(rng, persona.discountPct[0], persona.discountPct[1]);
    // Recurring products charge per segment year, so the deal amount needs
    // to reflect the full multi-year TCV (one charge per line in the plan).
    const computedAmount = lineItemPlan.reduce(
      (sum, entry) => sum + (entry.product.unitPrice * entry.product.quantity * (1 - discountPct / 100)),
      0,
    );
    const dealAmount = Number.isFinite(overrideAmount) && overrideAmount > 0
      ? overrideAmount
      : Math.round(computedAmount);

    // ── Company ──────────────────────────────────────────────────────────
    let companyId;
    let companyName;
    let companyDomain;
    let companyAreaCode = '512';
    if (existingCompanies.length) {
      companyId = existingCompanies[0];
      const existing = await getObject(hs, TYPE.COMPANY, companyId, ['name', 'domain', 'state']);
      companyName = existing.properties?.name || `Company ${companyId}`;
      companyDomain = existing.properties?.domain || `company${companyId}.example.com`;
      const matchedCity = CITIES.find((c) => c.state === existing.properties?.state);
      if (matchedCity) companyAreaCode = matchedCity.areaCode;
    } else {
      const companyProps = buildCompanyProperties(rng, persona, inputs.companyName);
      const created = await createObject(hs, TYPE.COMPANY, companyProps);
      companyId = created.id;
      companyName = companyProps.name;
      companyDomain = companyProps.domain;
      const matchedCity = CITIES.find((c) => c.state === companyProps.state);
      if (matchedCity) companyAreaCode = matchedCity.areaCode;
      await associateDefault(hs, TYPE.DEAL, dealId, TYPE.COMPANY, companyId);
    }

    // ── Contacts ─────────────────────────────────────────────────────────
    const contactIds = [...existingContacts];
    const contactsToCreate = Math.max(0, numContacts - existingContacts.length);
    for (let i = 0; i < contactsToCreate; i++) {
      const isPrimary = existingContacts.length === 0 && i === 0;
      const contactProps = buildContactProperties(rng, companyName, companyDomain, companyAreaCode, isPrimary);
      const created = await createObject(hs, TYPE.CONTACT, contactProps);
      const contactId = created.id;
      contactIds.push(contactId);
      await Promise.all([
        associateDefault(hs, TYPE.DEAL, dealId, TYPE.CONTACT, contactId),
        associateDefault(hs, TYPE.COMPANY, companyId, TYPE.CONTACT, contactId),
      ]);
    }

    // ── Line items ───────────────────────────────────────────────────────
    // One line item per recurring product per 12-month segment year, plus a
    // single one-time line for the implementation fee. Each line carries its
    // own start/end dates so the contract object can derive segment spans
    // directly from the line item rather than the deal-level dates.
    const lineItemIds = [];
    for (const entry of lineItemPlan) {
      const lineProps = buildLineItemProperties(
        entry.product,
        entry.lineStart,
        entry.lineEnd,
        entry.segmentYear,
        entry.totalSegments,
      );
      const created = await createLineItem(hs, lineProps);
      const lineItemId = created.id;
      lineItemIds.push(lineItemId);
      await associateDefault(hs, TYPE.DEAL, dealId, TYPE.LINE_ITEM, lineItemId);
    }

    // ── Deal property defaults ───────────────────────────────────────────
    const dealUpdates = {};
    if (!dealProps.deal_category) dealUpdates.deal_category = 'new_business';
    if (!dealProps.revenue_type) dealUpdates.revenue_type = 'new';
    if (!dealProps.contract_start_date) dealUpdates.contract_start_date = fmtDate(contractStart);
    if (!dealProps.contract_end_date) dealUpdates.contract_end_date = fmtDate(contractEnd);
    if (!dealProps.closedate) dealUpdates.closedate = fmtDate(addDays(today, 30));
    if (!dealProps.amount || Number(dealProps.amount) === 0) {
      dealUpdates.amount = String(dealAmount);
    }
    if (!dealProps.dealname || /^untitled/i.test(dealProps.dealname)) {
      dealUpdates.dealname = `${companyName} - New Business (Test)`;
    }
    if (Object.keys(dealUpdates).length) {
      await updateObject(hs, TYPE.DEAL, dealId, dealUpdates);
    }

    return callback({
      outputFields: {
        success: 'true',
        companyId: String(companyId),
        contactIds: contactIds.join(','),
        lineItemIds: lineItemIds.join(','),
        lineItemsCreated: lineItemIds.length,
        dealAmount,
        persona: persona.key,
        seedSource,
        errorMessage: '',
      },
    });
  } catch (err) {
    const message = describeHubSpotError(err);
    console.error('[populate-test-deal-data] Failed:', message, err?.response?.status);
    return callback({
      outputFields: {
        success: 'false',
        companyId: '',
        contactIds: '',
        lineItemIds: '',
        lineItemsCreated: 0,
        dealAmount: 0,
        errorMessage: message,
      },
    });
  }
};
