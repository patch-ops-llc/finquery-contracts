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
  searchableProperties: ['contract_name', 'status'],
  properties: [
    { name: 'contract_name', label: 'Contract Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
    {
      name: 'status', label: 'Status', type: 'enumeration', fieldType: 'select',
      options: [
        { label: 'Active', value: 'active' },
        { label: 'Future', value: 'future' },
        { label: 'Inactive', value: 'inactive' },
        { label: 'Terminated', value: 'terminated' },
      ],
    },
    { name: 'start_date', label: 'Start Date', type: 'date', fieldType: 'date' },
    { name: 'end_date', label: 'End Date', type: 'date', fieldType: 'date' },
    { name: 'co_term_date', label: 'Co-Term Date', type: 'date', fieldType: 'date' },
    { name: 'total_arr', label: 'Total ARR', type: 'number', fieldType: 'number' },
    { name: 'lq_arr', label: 'LQ ARR', type: 'number', fieldType: 'number' },
    { name: 'fcm_arr', label: 'FCM ARR', type: 'number', fieldType: 'number' },
    { name: 'total_tcv', label: 'Total TCV', type: 'number', fieldType: 'number' },
    { name: 'subscription_count', label: 'Subscription Count', type: 'number', fieldType: 'number' },
    { name: 'amendment_count', label: 'Amendment Count', type: 'number', fieldType: 'number' },
    { name: 'contract_data', label: 'Contract Data', type: 'string', fieldType: 'textarea' },
    { name: 'activated_date', label: 'Activated Date', type: 'date', fieldType: 'date' },
    { name: 'terminated_date', label: 'Terminated Date', type: 'date', fieldType: 'date' },
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
  ],
  associatedObjects: ['COMPANY', 'DEAL', 'CONTACT'],
};

const SUBSCRIPTION_SCHEMA = {
  name: 'fq_subscription',
  labels: { singular: 'Subscription Segment', plural: 'Subscription Segments' },
  primaryDisplayProperty: 'segment_name',
  requiredProperties: ['segment_name'],
  searchableProperties: ['segment_name', 'product_code', 'status'],
  properties: [
    { name: 'segment_name', label: 'Segment Name', type: 'string', fieldType: 'text', hasUniqueValue: false },
    { name: 'segment_year', label: 'Segment Year', type: 'number', fieldType: 'number' },
    { name: 'start_date', label: 'Start Date', type: 'date', fieldType: 'date' },
    { name: 'end_date', label: 'End Date', type: 'date', fieldType: 'date' },
    { name: 'product_code', label: 'Product Code', type: 'string', fieldType: 'text' },
    { name: 'product_name', label: 'Product Name', type: 'string', fieldType: 'text' },
    { name: 'arr', label: 'ARR', type: 'number', fieldType: 'number' },
    { name: 'tcv', label: 'TCV', type: 'number', fieldType: 'number' },
    { name: 'quantity', label: 'Quantity', type: 'number', fieldType: 'number' },
    { name: 'unit_price', label: 'Unit Price', type: 'number', fieldType: 'number' },
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
  'contract_name', 'status', 'start_date', 'end_date', 'co_term_date',
  'total_arr', 'lq_arr', 'fcm_arr', 'total_tcv', 'subscription_count',
  'amendment_count', 'contract_data', 'activated_date', 'terminated_date',
  'termination_reason',
];

const SUBSCRIPTION_PROPS = [
  'segment_name', 'segment_year', 'start_date', 'end_date', 'product_code',
  'product_name', 'arr', 'tcv', 'quantity', 'unit_price', 'status',
  'proration_status', 'amendment_indicator',
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
    `/crm/v4/objects/${fromType}/${fromId}/associations/${toType}/${toId}`,
    [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 1 }]
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

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    if (companyIds.length > 0) {
      await createAssociation('0-3', deal.id, '0-2', companyIds[0]);
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

    const companyIds = await getAssociatedIds(contractTypeId, contractId, '0-2');
    if (companyIds.length > 0) {
      await createAssociation('0-3', deal.id, '0-2', companyIds[0]);
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
