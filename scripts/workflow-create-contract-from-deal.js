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
 *    - totalArr (number)
 *    - status (string)
 *    - renewalDealId (string)         — populated for new_business / renewal closes
 *    - renewalDealClosedate (string)  — YYYY-MM-DD; matches the new contract's end date
 *    - errorMessage (string)
 */
const axios = require('axios');

const DEAL_PROPS = [
  'dealname',
  'amount',
  'closedate',
  'dealstage',
  'deal_category',
  'contract_start_date',
  'contract_end_date',
];

const LINE_ITEM_PROPS = [
  'name',
  'description',
  'quantity',
  'price',
  'amount',
  'hs_sku',
  'hs_line_item_currency_code',
  'hs_recurring_billing_period',
  'hs_recurring_billing_start_date',
  'hs_recurring_billing_number_of_payments',
  'revenue_type',
];

// Minimum contract properties this workflow needs to write. We self-heal any
// missing ones on the contract schema before creating the record so the
// workflow never fails with PROPERTY_DOESNT_EXIST in portals where the schema
// was created with an older / leaner definition.
const CONTRACT_REQUIRED_PROPERTIES = [
  { name: 'contract_name', label: 'Contract Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
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
  { name: 'contract_start_date', label: 'Contract Start Date', type: 'date', fieldType: 'date' },
  { name: 'contract_end_date', label: 'Contract End Date', type: 'date', fieldType: 'date' },
  { name: 'co_term_date', label: 'Co-Term Date', type: 'date', fieldType: 'date' },
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
  const quantity = Number(props.quantity || 1) || 1;
  const unitPrice = Number(props.price || 0) || 0;
  return unitPrice * quantity;
}

function isRecurringLineItem(lineItem) {
  const period = String(lineItem?.properties?.hs_recurring_billing_period || '').toLowerCase();
  return Boolean(period) && period !== 'one_time' && period !== 'onetime';
}

function deriveProductCode(lineItem) {
  const props = lineItem?.properties || {};
  const sku = String(props.hs_sku || '').trim();
  if (sku) {
    const upper = sku.toUpperCase();
    const head = upper.split(/[-_\s]/)[0];
    if (head) return head;
    return upper;
  }
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
  if (responseData.message) return responseData.message;
  try {
    return JSON.stringify(responseData);
  } catch (jsonErr) {
    return err?.message || 'Unknown HubSpot error';
  }
}

async function createObjectWithFallback(hs, typeId, properties, requiredKeys) {
  try {
    return await createObject(hs, typeId, properties);
  } catch (err) {
    if (err?.response?.status !== 400 && err?.response?.status !== 422) throw err;

    const fallback = {};
    for (const key of requiredKeys) {
      if (properties[key] !== undefined && properties[key] !== null && properties[key] !== '') {
        fallback[key] = properties[key];
      }
    }

    if (!Object.keys(fallback).length) throw err;

    try {
      return await createObject(hs, typeId, fallback);
    } catch (fallbackErr) {
      const first = parseHubSpotErrorMessage(err);
      const second = parseHubSpotErrorMessage(fallbackErr);
      throw new Error(`Object create failed (full + fallback payload). ${first} | ${second}`);
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
      throw new Error(`Association failed both directions. ${first} | ${second}`);
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

function filterPropertiesForSchema(properties, schemaPropertyNames) {
  const filtered = {};
  for (const [key, value] of Object.entries(properties || {})) {
    if (!schemaPropertyNames?.has(key)) continue;
    if (value === undefined || value === null || value === '') continue;
    filtered[key] = value;
  }
  return filtered;
}

async function ensureSchemaProperties(hs, typeId, expectedProperties, existingPropertyNames) {
  const created = [];
  for (const prop of expectedProperties) {
    if (existingPropertyNames.has(prop.name)) continue;
    try {
      await hs.post(`/crm/v3/properties/${typeId}`, prop);
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
      timeout: 20000,
    });

    // HubSpot Private Apps cap at 100 req / 10 sec. Without retry/backoff a
    // single closed-deal burst (segment + association calls) trips a 429 and
    // surfaces as "Request failed with status code 429". Retry 429 / 5xx with
    // exponential backoff + jitter, honoring `Retry-After`. Workflow custom
    // code actions have a ~20s wall clock so we keep the budget tight.
    const HS_MAX_RETRIES = 4;
    const HS_BASE_DELAY_MS = 400;
    const HS_MAX_DELAY_MS = 4000;
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

    const [contractTypeId, subscriptionTypeId] = await Promise.all([
      getTypeIdBySchemaName(hs, 'fq_contract'),
      getTypeIdBySchemaName(hs, 'fq_subscription'),
    ]);

    if (!contractTypeId) throw new Error('Could not find schema fq_contract.');
    if (!subscriptionTypeId) throw new Error('Could not find schema fq_subscription.');

    const [contractSchemaPropertyNames, subscriptionSchemaPropertyNames] = await Promise.all([
      getSchemaPropertyNamesByTypeId(hs, contractTypeId),
      getSchemaPropertyNamesByTypeId(hs, subscriptionTypeId),
    ]);

    // Self-heal any contract properties this workflow needs but the live
    // schema is missing (e.g. older portals where start_date / end_date were
    // never created on fq_contract).
    await ensureSchemaProperties(
      hs,
      contractTypeId,
      CONTRACT_REQUIRED_PROPERTIES,
      contractSchemaPropertyNames
    );

    const deal = await getObject(hs, '0-3', dealId, DEAL_PROPS);
    const dealProps = deal.properties || {};

    const companyIds = await getAssociatedIds(hs, '0-3', dealId, '0-2');
    const contactIds = await getAssociatedIds(hs, '0-3', dealId, '0-1');
    const lineItemIds = await getAssociatedIds(hs, '0-3', dealId, 'line_items');

    const lineItems = lineItemIds.length
      ? await Promise.all(lineItemIds.map((id) => getObject(hs, 'line_items', id, LINE_ITEM_PROPS)))
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

    const contractPayloadRaw = {
      contract_name: cleanContractName(dealProps.dealname),
      status: determineStatus(derivedStartDate, derivedEndDate),
      contract_start_date: derivedStartDate,
      contract_end_date: derivedEndDate,
      co_term_date: derivedEndDate,
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

    const contractPayload = filterPropertiesForSchema(contractPayloadRaw, contractSchemaPropertyNames);
    const contractRequiredKeys = ['contract_name', 'status', 'contract_data'].filter((key) =>
      contractSchemaPropertyNames.has(key)
    );
    const contract = await createObjectWithFallback(
      hs,
      contractTypeId,
      contractPayload,
      contractRequiredKeys
    );

    await associateWithFallback(hs, contractTypeId, contract.id, '0-3', dealId);

    if (companyIds[0]) {
      await associateWithFallback(hs, contractTypeId, contract.id, '0-2', companyIds[0]);
    }

    for (const contactId of contactIds) {
      try {
        await associateWithFallback(hs, contractTypeId, contract.id, '0-1', contactId);
      } catch (err) {
        // Best effort: avoid failing action on single bad contact association.
        console.warn(`Contact association failed for ${contactId}: ${err.message}`);
      }
    }

    let createdSegments = 0;
    let totalArr = 0;
    const segmentErrors = [];
    const yearSegments = buildYearSegments(derivedStartDate, derivedEndDate);

    // One subscription segment per recurring line item per contract year. One-time
    // line items live on the contract as line items only -- they are NOT segments.
    const recurringLineItems = lineItems.filter(isRecurringLineItem);
    const oneTimeLineItems = lineItems.length - recurringLineItems.length;
    const cleanedContractName = cleanContractName(dealProps.dealname);
    const dealRevenueType = normalizeRevenueType('', category === 'renewal' ? 'renewal' : 'new');

    for (const [liIndex, lineItem] of recurringLineItems.entries()) {
      const lp = lineItem.properties || {};
      const productName = String(lp.name || 'Product').trim() || 'Product';
      const productCode = deriveProductCode(lineItem);
      const quantity = Number(lp.quantity || 1) || 1;
      const unitPrice = Number(lp.price || 0) || 0;
      const annualArr = getLineItemAnnualArr(lineItem);
      const lineRevenueType = normalizeRevenueType(lp.revenue_type, dealRevenueType);

      for (const [yearIdx, segmentYear] of yearSegments.entries()) {
        totalArr += annualArr;

        const segmentPayloadRaw = {
          segment_name: `${cleanedContractName} — ${productCode} Year ${segmentYear.year}`,
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
          subscription_start_date: derivedStartDate,
          subscription_end_date: derivedEndDate,
          arr_start_date: segmentYear.start_date,
          arr_end_date: segmentYear.end_date,
          segment_year: String(segmentYear.year),
          segment_index: String(yearIdx + 1),
          segment_label: `Year ${segmentYear.year}`,
          segment_key: `${contract.id}-${liIndex + 1}-${yearIdx + 1}`,
          billing_frequency: 'annual',
          charge_type: 'recurring',
          revenue_type: lineRevenueType,
        };

        const segmentPayload = filterPropertiesForSchema(segmentPayloadRaw, subscriptionSchemaPropertyNames);
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
          const segment = await createObjectWithFallback(
            hs,
            subscriptionTypeId,
            segmentPayload,
            segmentRequiredKeys
          );

          await associateWithFallback(hs, subscriptionTypeId, segment.id, contractTypeId, contract.id);

          if (companyIds[0]) {
            try {
              await associateWithFallback(hs, subscriptionTypeId, segment.id, '0-2', companyIds[0]);
            } catch (err) {
              console.warn(`Company association failed for segment ${segment.id}: ${err.message}`);
            }
          }

          createdSegments += 1;
        } catch (segmentErr) {
          const segmentErrorMessage = parseHubSpotErrorMessage(segmentErr);
          const label = `${productName} Year ${segmentYear.year}`;
          segmentErrors.push(`${label}: ${segmentErrorMessage}`);
          console.warn(`Segment creation failed for ${label}: ${segmentErrorMessage}`);
        }
      }
    }

    const rollupPayload = filterPropertiesForSchema(
      {
        total_arr: String(totalArr),
        total_tcv: String(totalArr),
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
    let renewalDealId = '';
    let renewalDealClosedate = '';
    const renewalEligibleCategories = ['new_business', 'renewal'];
    if (renewalEligibleCategories.includes(category)) {
      try {
        // Skip if an open renewal deal already exists on this contract.
        const existingDealIds = await getAssociatedIds(hs, contractTypeId, contract.id, '0-3');
        let openRenewalExists = false;
        for (const did of existingDealIds.slice(0, 25)) {
          try {
            const existingDeal = await getObject(hs, '0-3', did, ['dealstage', 'deal_category']);
            const stage = String(existingDeal?.properties?.dealstage || '').toLowerCase();
            const cat = String(existingDeal?.properties?.deal_category || '').toLowerCase();
            if (cat === 'renewal' && stage !== 'closedwon' && stage !== 'closedlost') {
              openRenewalExists = true;
              break;
            }
          } catch (peekErr) {
            // Best-effort: if a single deal lookup fails, keep scanning.
          }
        }

        if (!openRenewalExists) {
          // Renewal term: day after current end → +1 year. Close date is the
          // current contract end date so the deal lands in the right
          // forecast quarter.
          const nextStart = parseDateValue(derivedEndDate) || new Date();
          nextStart.setUTCDate(nextStart.getUTCDate() + 1);
          const nextEnd = new Date(nextStart);
          nextEnd.setUTCFullYear(nextEnd.getUTCFullYear() + 1);
          renewalDealClosedate = derivedEndDate;

          const renewalDealName = `${cleanedContractName} — Renewal`;
          const renewalDeal = await createObject(hs, '0-3', {
            dealname: renewalDealName,
            dealstage: 'appointmentscheduled',
            deal_category: 'renewal',
            contract_start_date: fmtDateForHS(nextStart),
            contract_end_date: fmtDateForHS(nextEnd),
            closedate: derivedEndDate,
            amount: String(totalArr || 0),
            pipeline: 'default',
          });
          renewalDealId = String(renewalDeal.id);

          // Carry the company + contacts + new contract over to the renewal
          // deal so DealHub has the full context immediately.
          if (companyIds[0]) {
            try {
              await associateWithFallback(hs, '0-3', renewalDeal.id, '0-2', companyIds[0]);
            } catch (assocErr) {
              console.warn(`Renewal deal company association failed: ${assocErr.message}`);
            }
          }
          try {
            await associateWithFallback(hs, contractTypeId, contract.id, '0-3', renewalDeal.id);
          } catch (assocErr) {
            console.warn(`Renewal deal -> contract association failed: ${assocErr.message}`);
          }
          for (const contactId of contactIds) {
            try {
              await associateWithFallback(hs, '0-3', renewalDeal.id, '0-1', contactId);
            } catch (contactErr) {
              // Skip invalid contacts — best-effort.
            }
          }

          console.log(
            `Renewal deal ${renewalDeal.id} auto-created for contract ${contract.id} ` +
            `(${fmtDateForHS(nextStart)} → ${fmtDateForHS(nextEnd)}, closedate=${derivedEndDate})`
          );
        } else {
          console.log(`Skipping renewal deal auto-creation; an open renewal already exists for contract ${contract.id}`);
        }
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
        status: determineStatus(derivedStartDate, derivedEndDate),
        renewalDealId,
        renewalDealClosedate,
        errorMessage: segmentErrors.length ? segmentErrors.slice(0, 3).join(' | ') : '',
      },
    });
  } catch (err) {
    console.error('Workflow action failed:', err.response?.data || err.message);
    callback({
      outputFields: {
        success: 'false',
        contractId: '',
        segmentsCreated: 0,
        recurringLineItems: 0,
        oneTimeLineItems: 0,
        totalArr: 0,
        status: '',
        renewalDealId: '',
        renewalDealClosedate: '',
        errorMessage: err.message || 'Unknown error',
      },
    });
  }
};
