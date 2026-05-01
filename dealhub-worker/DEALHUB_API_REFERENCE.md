# FinQuery — DealHub API Reference

> **For:** DealHub Implementation Team
> **Prepared by:** PatchOps (Zach West)
> **Service:** Cloudflare Worker (`finquery-dealhub-api`) — demo / reference endpoint
> **Auth:** none (demo endpoint)
> **Data source:** HubSpot CRM (`deals`, `line_items`, `fq_contract`, `fq_subscription`)

This document is the schema contract for the FinQuery → DealHub data flow.
It covers every call, every response field, and shows exactly how to take a
deal ID, find the associated contract, fetch all subscription segments, and
translate the most recent year's segments into renewal line items for the
next proposal.

The accompanying Cloudflare Worker is a working reference implementation
PatchOps hosts so DealHub can see live JSON responses against the real
schema. **DealHub queries HubSpot directly in production** — they do not
proxy through this worker. The schemas and logic documented here are what
DealHub reproduces against the HubSpot CRM API in their own integration.

---

## Table of contents

1. [Call & response map](#call--response-map)
2. [How segments are organized on the contract UIE](#how-segments-are-organized-on-the-contract-uie)
3. [How segments translate into renewal line items](#how-segments-translate-into-renewal-line-items)
4. [Endpoint reference](#endpoint-reference)
   - [GET /v1/health](#get-v1health)
   - [GET /v1/deals/{dealId}](#get-v1dealsdealid)
   - [GET /v1/contracts/{contractId}](#get-v1contractscontractid)
   - [GET /v1/contracts/{contractId}/segments](#get-v1contractscontractidsegments)
   - [GET /v1/contracts/{contractId}/renewal-line-items](#get-v1contractscontractidrenewal-line-items)
5. [Object schemas](#object-schemas)
   - [Deal](#deal)
   - [Contract](#contract)
   - [Subscription Segment](#subscription-segment)
   - [Year Group](#year-group)
   - [Renewal Line Item](#renewal-line-item)
   - [Company](#company)
   - [Contact](#contact)
   - [Deal Line Item](#deal-line-item)
6. [Error responses](#error-responses)
7. [Worked example: end-to-end renewal flow](#worked-example-end-to-end-renewal-flow)

---

## Call & response map

```
┌───────────────────────────────────────────────────────────────────┐
│                                                                   │
│  Caller knows:  dealId  (the renewal/amendment deal being quoted) │
│                                                                   │
└───────────────────────────────┬───────────────────────────────────┘
                                │
                                ▼  GET /v1/deals/{dealId}
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│  Worker resolves:                                                 │
│    1. The deal (HubSpot deal record)                              │
│    2. The associated source contract  (deal ↔ fq_contract)        │
│    3. All subscription segments        (contract ↔ fq_subscription)│
│    4. Groups segments by contract year (Year 1, Year 2, Year 3, ..)│
│    5. Identifies the LAST PERIOD       (highest year)             │
│    6. Builds RENEWAL LINE ITEMS        (one per product, deduped) │
│                                                                   │
└───────────────────────────────┬───────────────────────────────────┘
                                │
                                ▼  Single JSON response
                                │
┌───────────────────────────────────────────────────────────────────┐
│  DealHub uses `renewalLineItems` as the seed for the new proposal │
│  and presents `segmentsByYear` as historical context to the rep.  │
└───────────────────────────────────────────────────────────────────┘
```

The single-call shape is:

```jsonc
{
  "deal":              { /* Deal */ },
  "contractId":        "string|null",
  "contract":          { /* Contract */ } /* or null */,
  "segmentsByYear":    [ /* YearGroup[] */ ],
  "segments":          [ /* SubscriptionSegment[] */ ], // flat list
  "lastPeriod":        { /* YearGroup */ } /* or null */,
  "renewalLineItems":  [ /* RenewalLineItem[] */ ],     // <-- the proposal seed
  "dealLineItems":     [ /* DealLineItem[] */ ],
  "company":           { /* Company */ } /* or null */,
  "contacts":          [ /* Contact[] */ ]
}
```

---

## How segments are organized on the contract UIE

The FinQuery contract card groups subscription segments **by contract year**,
not by `segment_year` alone. The worker mirrors this logic exactly so
DealHub's view matches what FinQuery CSMs see in HubSpot.

### Year-assignment rules

For each segment:

1. If the contract has a `startDate` AND the segment has a parseable
   `startDate`, compute the year as:
   ```
   year = floor( monthsBetween(contract.startDate, segment.startDate) / 12 ) + 1
   ```
   This anchors on the contract's effective start. A contract starting
   `2026-05-16` puts:
   - `2026-05-16 → 2027-05-15` in **Year 1**
   - `2027-05-16 → 2028-05-15` in **Year 2**
   - `2028-05-16 → 2029-05-15` in **Year 3**
2. If the contract start is missing, fall back to the segment's
   `segmentYear` field, then `segmentIndex`, then a sequentially-assigned
   year for each unique [start,end] window.
3. Segments shorter than 14 days are dropped as Salesforce-import artifacts
   (off-by-one duplicate "Year 2" lines that were 1 day long).

### Example

Contract `2026-05-16 → 2029-05-15` (3-year), one product `LQ`, $120k ARR:

| Segment | Status | Start | End | segment_year | Assigned to |
|---|---|---|---|---|---|
| LQ Y1 | active | 2026-05-16 | 2027-05-15 | 1 | **Year 1** |
| LQ Y2 | future | 2027-05-16 | 2028-05-15 | 2 | **Year 2** |
| LQ Y3 | future | 2028-05-16 | 2029-05-15 | 3 | **Year 3** ← `lastPeriod` |

Multiple products produce multiple segments per year. Year 3 of a 2-product
3-year contract has 2 segments (one per product), and that's what becomes
the renewal seed.

### `lastPeriod` selection

The worker picks the **highest-year group that contains at least one
inheritable segment** as `lastPeriod`. "Inheritable" means:

- `status === 'active'` OR
- `status === 'future'` OR
- `status` is empty AND the segment's end date hasn't passed

If every segment is terminated/expired, `lastPeriod` falls back to the
highest-year group regardless of status (so DealHub still sees historical
context).

---

## How segments translate into renewal line items

`renewalLineItems` is the array DealHub drops into a new quote. It is built
from `lastPeriod.segments` using these rules (mirrors the renewal-deal
seeding logic in the existing Railway API):

1. Filter `lastPeriod.segments` to inheritable segments (rules above).
2. Group by `productCode` (case-insensitive). One line item per product.
3. Within a product, prefer the segment with the higher `segmentYear`, then
   the later `startDate`.
4. Compute `quantity` as `renewalQuantity || quantity || originalQuantity || 1`.
5. Compute `unitPrice`:
   - Use `segment.unitPrice` if set (>0)
   - Otherwise `arr / quantity` (annualized for monthly segments via `mrr * 12`)
6. Map `billingFrequency` to `dh_duration` (months):
   `monthly→1`, `quarterly→3`, `semiannual→6`, `annual→12` (default).
7. Stamp `productTag: "Recurring"` on every renewal line — these always seed
   subscription segments on close-won.
8. Tag every line as `revenueType: "renewal"`. DealHub can override per line
   based on its own product-family logic before writing back to the deal.

Each renewal line carries `sourceSegmentId`, `sourceSegmentYear`, and
`sourceArr` so DealHub can show the rep "this came from Year N at $X ARR".

### Override the source year

`GET /v1/contracts/{contractId}/renewal-line-items?year=2` pins the seed to
Year 2 instead of the last period. Useful for amendments that should
co-term to a specific year.

---

## Endpoint reference

Base URL: `https://finquery-dealhub-api.<account>.workers.dev`
(or the custom domain configured in Cloudflare).

### `GET /v1/health`

Liveness probe.

```http
GET /v1/health
```

Response `200`:

```json
{ "ok": true, "service": "finquery-dealhub-api", "version": "1.0.0" }
```

---

### `GET /v1/deals/{dealId}`

The primary endpoint. Returns deal + contract + segments + renewal line
items + company + contacts in a single call.

**Path params**

| Name | Type | Description |
|---|---|---|
| `dealId` | string | HubSpot deal ID |

**Query params (optional)**

| Name | Type | Default | Description |
|---|---|---|---|
| `includeLineItems` | boolean | `true` | Include the deal's current line items in the response |

**Example request**

```bash
curl "$WORKER/v1/deals/123456789"
```

**Example response (truncated)**

```jsonc
{
  "deal": {
    "id": "123456789",
    "name": "Acme Corp — Renewal FY27",
    "stage": "1331037727",
    "pipeline": "860641302",
    "amount": 145000,
    "closeDate": "2027-05-15",
    "category": "renewal",
    "revenueType": "renewal",
    "contractStartDate": "2027-05-16",
    "contractEndDate": "2028-05-15",
    "isClosed": false,
    "isClosedWon": false,
    "ownerId": "12345"
  },
  "contractId": "987654321",
  "contract": {
    "id": "987654321",
    "name": "Acme Corp — Master Subscription Agreement",
    "contractNumber": "C-00482",
    "salesforceId": "8016g000001abcdAAA",
    "status": "active",
    "startDate": "2024-05-16",
    "endDate": "2027-05-15",
    "contractTerm": 36,
    "renewalTerm": 12,
    "evergreen": false,
    "totalArr": 120000,
    "totalTcv": 360000,
    "arrByProduct": { "LQ": 90000, "FCM": 30000 },
    "renewalUpliftRate": 7.0,
    "priceCap": 10.0,
    "subscriptionCount": 6,
    "amendmentCount": 0,
    "hasLegacyProducts": false,
    "replacedByContract": null,
    "replacesContract": null
    /* ... */
  },
  "segmentsByYear": [
    {
      "year": 1,
      "label": "Year 1",
      "startDate": "2024-05-16",
      "endDate": "2025-05-15",
      "totalArr": 110000,
      "totalMrr": 9166.67,
      "totalTcv": 110000,
      "segmentCount": 2,
      "productCodes": ["LQ", "FCM"],
      "isCurrent": false,
      "segments": [/* SubscriptionSegment[] */]
    },
    {
      "year": 2,
      "label": "Year 2",
      "startDate": "2025-05-16",
      "endDate": "2026-05-15",
      "totalArr": 117700,
      "totalMrr": 9808.33,
      "totalTcv": 117700,
      "segmentCount": 2,
      "productCodes": ["LQ", "FCM"],
      "isCurrent": true,
      "segments": [/* ... */]
    },
    {
      "year": 3,
      "label": "Year 3",
      "startDate": "2026-05-16",
      "endDate": "2027-05-15",
      "totalArr": 120000,
      "totalMrr": 10000,
      "totalTcv": 120000,
      "segmentCount": 2,
      "productCodes": ["LQ", "FCM"],
      "isCurrent": false,
      "segments": [/* ... */]
    }
  ],
  "lastPeriod": { /* same shape as segmentsByYear[2] above */ },
  "renewalLineItems": [
    {
      "sourceSegmentId": "445566778899",
      "productCode": "LQ",
      "productName": "LeaseQuery",
      "sku": "LQ",
      "quantity": 50,
      "unitPrice": 1800,
      "lineAmount": 90000,
      "currency": "USD",
      "billingFrequency": "annual",
      "duration": 12,
      "revenueType": "renewal",
      "sourceArr": 90000,
      "sourceMrr": 7500,
      "sourceSegmentYear": 3,
      "sourceSegmentLabel": "Year 3"
    },
    {
      "sourceSegmentId": "445566778900",
      "productCode": "FCM",
      "productName": "FinQuery Contract Management",
      "sku": "FCM",
      "quantity": 1,
      "unitPrice": 30000,
      "lineAmount": 30000,
      "currency": "USD",
      "billingFrequency": "annual",
      "duration": 12,
      "revenueType": "renewal",
      "sourceArr": 30000,
      "sourceMrr": 2500,
      "sourceSegmentYear": 3,
      "sourceSegmentLabel": "Year 3"
    }
  ],
  "dealLineItems": [/* current deal line items, if any */],
  "company": {
    "id": "111222333",
    "name": "Acme Corp",
    "domain": "acme.com",
    "city": "Austin",
    "state": "TX",
    "country": "US"
  },
  "contacts": [
    { "id": "555", "firstName": "Jane", "lastName": "Doe", "fullName": "Jane Doe", "email": "jane@acme.com", "title": "VP Finance" }
  ]
}
```

---

### `GET /v1/contracts/{contractId}`

Same shape as `/v1/deals/{dealId}` but starting from a contract ID. Returns
`contract`, `segmentsByYear`, `segments`, `lastPeriod`, `renewalLineItems`,
`company`. (No `deal`, `dealLineItems`, or `contacts` since those are deal-scoped.)

```bash
curl "$WORKER/v1/contracts/987654321"
```

---

### `GET /v1/contracts/{contractId}/segments`

Just the segment data — no contract/company/renewal-line-item computation.

**Response**

```jsonc
{
  "contractId": "987654321",
  "segmentsByYear": [/* YearGroup[] */],
  "segments":      [/* SubscriptionSegment[] */],
  "lastPeriod":    { /* YearGroup */ } /* or null */
}
```

---

### `GET /v1/contracts/{contractId}/renewal-line-items`

Just the renewal line items — fastest endpoint, smallest payload.

**Query params (optional)**

| Name | Type | Description |
|---|---|---|
| `year` | integer | Pin the seed to a specific contract year. Defaults to the last period. |

**Response**

```jsonc
{
  "contractId":      "987654321",
  "sourcedFromYear": 3,
  "periodStartDate": "2026-05-16",
  "periodEndDate":   "2027-05-15",
  "renewalLineItems": [/* RenewalLineItem[] */]
}
```

---

## Object schemas

### Deal

```ts
type Deal = {
  id: string;
  name: string | null;
  stage: string | null;            // HubSpot deal stage ID
  pipeline: string | null;         // HubSpot pipeline ID
  amount: number | null;
  closeDate: string | null;        // ISO date (YYYY-MM-DD)
  category: 'new_business' | 'renewal' | 'expansion' | 'contraction' | null;
  revenueType: 'new' | 'renewal' | 'expansion' | 'contraction' | 'cross_sell' | null;
  contractStartDate: string | null;  // ISO date
  contractEndDate: string | null;    // ISO date
  isClosed: boolean;
  isClosedWon: boolean;
  ownerId: string | null;
};
```

### Contract

```ts
type Contract = {
  id: string;
  name: string | null;
  contractNumber: string | null;
  salesforceId: string | null;
  description: string | null;
  status: 'draft' | 'in_approval_process' | 'active' | 'future'
        | 'inactive' | 'expired' | 'terminated' | null;
  terminationReason: string | null;
  startDate: string | null;            // ISO date
  endDate: string | null;              // ISO date
  coTermDate: string | null;
  activatedDate: string | null;
  terminatedDate: string | null;
  amendmentStartDate: string | null;
  contractRenewedOn: string | null;
  contractTerm: number | null;         // months
  renewalTerm: number | null;          // months
  evergreen: boolean;
  totalArr: number | null;
  totalTcv: number | null;
  arrByProduct: { LQ: number | null; FCM: number | null };
  priceCap: number | null;             // %
  maxUplift: number | null;            // %
  renewalUpliftRate: number | null;    // %
  amendmentRenewalBehavior: 'latest_end_date' | 'earliest_end_date' | null;
  mdqRenewalBehavior: 'de_segmented' | null;
  renewalForecast: boolean;
  renewalQuoted: boolean;
  subscriptionCount: number | null;
  amendmentCount: number | null;
  hasLegacyProducts: boolean;
  replacedByContract: string | null;   // ID of the new contract (if renewed)
  replacesContract: string | null;     // ID of the prior contract
  billingAddress: {
    street: string | null;
    city: string | null;
    state: string | null;
    postalCode: string | null;
    country: string | null;
  };
};
```

### Subscription Segment

```ts
type SubscriptionSegment = {
  id: string;
  segmentName: string | null;
  subscriptionNumber: string | null;
  salesforceId: string | null;
  productCode: string | null;          // 'LQ', 'FCM', etc.
  productName: string | null;
  chargeType: 'one_time' | 'recurring' | 'usage' | null;
  billingFrequency: 'annual' | 'monthly' | 'quarterly' | 'semiannual' | 'invoice_plan' | null;
  status: 'active' | 'future' | 'inactive' | 'expired' | 'terminated' | null;
  amendmentIndicator: 'Expansion' | 'Contraction' | null;
  revenueType: 'new' | 'renewal' | 'expansion' | 'contraction' | 'cross_sell' | null;
  bundled: boolean;
  segmentYear: number | null;          // 1, 2, 3, ...
  segmentLabel: string | null;         // 'Year 1', etc.
  segmentIndex: number | null;
  startDate: string | null;            // ISO date — arr_start_date || segment_start_date || start_date
  endDate: string | null;              // ISO date — arr_end_date || segment_end_date || end_date
  quantity: number | null;
  originalQuantity: number | null;
  renewalQuantity: number | null;
  unitPrice: number | null;
  listPrice: number | null;
  netPrice: number | null;
  discountPercent: number | null;
  discountAmount: number | null;
  arr: number | null;                  // annualized recurring revenue for this segment
  mrr: number | null;
  tcv: number | null;
  renewalPrice: number | null;
  renewalUpliftRate: number | null;
};
```

### Year Group

```ts
type YearGroup = {
  year: number;                        // 1, 2, 3, ...
  label: string;                       // 'Year 1'
  startDate: string | null;            // earliest segment start in this year
  endDate: string | null;              // latest segment end in this year
  totalArr: number;
  totalMrr: number;
  totalTcv: number;
  segmentCount: number;
  productCodes: string[];              // unique product codes in this year
  isCurrent: boolean;                  // today is between startDate and endDate
  segments: SubscriptionSegment[];
};
```

### Renewal Line Item

The shape DealHub drops into a new quote. Fields mirror what HubSpot
expects on `line_items` so DealHub can write them straight back.

```ts
type RenewalLineItem = {
  sourceSegmentId: string;             // For traceability — the segment this came from
  productCode: string | null;
  productName: string;
  sku: string | null;                  // = productCode (the value to write to hs_sku)
  quantity: number;                    // value to write to dh_quantity
  unitPrice: number;                   // dollars, 2dp
  lineAmount: number;                  // quantity * unitPrice
  currency: 'USD';
  billingFrequency: 'annual' | 'monthly' | 'quarterly' | 'semiannual' | string;
  duration: 12 | 1 | 3 | 6 | number;   // number of months — value to write to dh_duration
  productTag: 'Recurring';             // value to write to product_tag (always Recurring on renewal seeds)
  revenueType: 'renewal';              // override per-line in DealHub before writing
  sourceArr: number;                   // ARR of the source segment
  sourceMrr: number;                   // MRR of the source segment
  sourceSegmentYear: number | null;    // Which year this came from (last period)
  sourceSegmentLabel: string | null;   // 'Year 3'
};
```

### Company

```ts
type Company = {
  id: string;
  name: string | null;
  domain: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
};
```

### Contact

```ts
type Contact = {
  id: string;
  firstName: string | null;
  lastName: string | null;
  fullName: string | null;
  email: string | null;
  title: string | null;
};
```

### Deal Line Item

The deal's current line items (what's already on the deal in HubSpot).

```ts
type DealLineItem = {
  id: string;
  name: string | null;
  sku: string | null;
  description: string | null;
  quantity: number | null;             // sourced from dh_quantity
  unitPrice: number | null;
  amount: number | null;
  duration: number | null;             // months — sourced from dh_duration
  productTag: 'Recurring' | 'One-time' | string | null; // sourced from product_tag — primary recurring/one-time signal
  recurringBillingStartDate: string | null;
  revenueType: string | null;
  isRecurring: boolean;                // resolved from productTag (preferred), then duration; false = one-time charge
};
```

---

## Error responses

All errors return JSON:

```json
{ "error": "<code>", "message": "<human readable>" }
```

| HTTP | `error` code | When |
|------|--------------|------|
| 404 | `not_found` | Deal/contract ID doesn't exist in HubSpot |
| 500 | `server_misconfigured` | `HUBSPOT_ACCESS_TOKEN` secret missing |
| 500 | `schemas_missing` | `fq_contract` / `fq_subscription` schemas not found in portal |
| 502 | `hubspot_error` | HubSpot returned a non-404 error (full response in `hubspot` field) |

---

## Worked example: end-to-end renewal flow

**Scenario:** DealHub is building a renewal quote on deal `123456789`. The
deal is associated to contract `987654321` (a 3-year contract ending May 2027,
$120k ARR, 2 products: LQ ($90k/yr × 50 seats) + FCM ($30k/yr × 1)).

### 1. The caller hits the worker

```bash
curl "$WORKER/v1/deals/123456789"
```

### 2. The worker does

```
[deal 123456789]
   ├── associations.fq_contract -> [987654321]
   ├── load fq_contract 987654321 (CONTRACT_PROPS)
   ├── associations(987654321).fq_subscription -> [s1...s6]   (6 segments: 3 years × 2 products)
   ├── load all 6 fq_subscription records (SUBSCRIPTION_PROPS)
   ├── group by contract year:
   │     Year 1 -> [LQ Y1, FCM Y1]   total ARR $110k
   │     Year 2 -> [LQ Y2, FCM Y2]   total ARR $117.7k
   │     Year 3 -> [LQ Y3, FCM Y3]   total ARR $120k   <-- lastPeriod
   ├── build renewalLineItems from Year 3:
   │     [{LQ, qty 50, $1800, $90k}, {FCM, qty 1, $30000, $30k}]
   ├── associations(deal).company  -> Acme Corp
   ├── associations(deal).contacts -> [Jane Doe, ...]
   └── (optional) associations(deal).line_items -> current deal lines
```

### 3. DealHub uses the response

- Display `segmentsByYear` as a "Contract history" panel showing all 3 years
- Show `lastPeriod` prominently as "Renewing from Year 3"
- Use `renewalLineItems` directly as the starting line items in the new quote
- Apply the contract's `renewalUpliftRate` (or DealHub's own pricing logic) on top
- Tag each line's `revenueType` based on whether it's a `renewal` (carry-forward), `expansion` (added seats), `cross_sell` (new product family), or `contraction` (reduced)
- On close-won, DealHub writes the final line items back to the deal in HubSpot via the standard line-items API

### 4. The Railway API takes over on close-won

The "Process Deal → Contract" action on the HubSpot deal card calls
`POST /api/update-contract-from-deal` (existing Railway endpoint), which
creates the new contract, copies the deal's line items, generates fresh
subscription segments for the new term, and links the new contract to the
old one via `replaced_by_contract` / `replaces_contract`. DealHub does not
need to call this endpoint — the contract card UI handles it.

---

## Questions

For schema or behavior questions: **Zach West — zach@patchops.io**
