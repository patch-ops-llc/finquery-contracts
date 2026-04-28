/**
 * HubSpot Workflow Custom Code Action
 *
 * Purpose:
 * - Input: deal ID
 * - Reads deal + associated line items
 * - Creates one Contract record
 * - Creates supporting Subscription Segment records by engagement year
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

    const [contractTypeId, subscriptionTypeId] = await Promise.all([
      getTypeIdBySchemaName(hs, 'fq_contract'),
      getTypeIdBySchemaName(hs, 'fq_subscription'),
    ]);

    if (!contractTypeId) throw new Error('Could not find schema fq_contract.');
    if (!subscriptionTypeId) throw new Error('Could not find schema fq_subscription.');

    const subscriptionSchemaPropertyNames = await getSchemaPropertyNamesByTypeId(hs, subscriptionTypeId);

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

    const contractPayload = {
      contract_name: cleanContractName(dealProps.dealname),
      status: determineStatus(derivedStartDate, derivedEndDate),
      start_date: derivedStartDate,
      end_date: derivedEndDate,
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

    const contract = await createObject(hs, contractTypeId, contractPayload);

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

    await hs.patch(`/crm/v3/objects/${contractTypeId}/${contract.id}`, {
      properties: {
        total_arr: String(totalArr),
        total_tcv: String(totalArr),
        subscription_count: String(createdSegments),
      },
    });

    callback({
      outputFields: {
        success: 'true',
        contractId: String(contract.id),
        segmentsCreated: createdSegments,
        recurringLineItems: recurringLineItems.length,
        oneTimeLineItems,
        totalArr,
        status: determineStatus(derivedStartDate, derivedEndDate),
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
        errorMessage: err.message || 'Unknown error',
      },
    });
  }
};
