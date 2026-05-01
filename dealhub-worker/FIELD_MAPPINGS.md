# FinQuery — Field Mappings Quick Reference

> One-page cheat sheet for the DealHub team.
> Full schemas + endpoint reference: [`DEALHUB_API_REFERENCE.md`](./DEALHUB_API_REFERENCE.md)
> Full data flow narrative: [`../DEALHUB_DATA_FLOW.md`](../DEALHUB_DATA_FLOW.md)

| Symbol | Meaning |
|---|---|
| W | DealHub **writes** this field |
| R | DealHub **reads** this field |
| RO | Read-only — populated by FinQuery's contract API, never written by DealHub |

---

## 1. Deal (HubSpot native — `deals`)

DealHub's primary object during quoting. Custom properties listed below;
standard HubSpot deal properties (`dealname`, `dealstage`, `amount`,
`closedate`, etc.) are also available.

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values / Notes |
|---|---|---|---|---|
| `deal_category` | `deal.category` | enum | **W** | `new_business`, `renewal`, `expansion`, `contraction` — set on deal creation |
| `contract_start_date` | `deal.contractStartDate` | date | **W** | ISO date — start of the contract term this deal covers |
| `contract_end_date` | `deal.contractEndDate` | date | **W** | ISO date — end of the contract term this deal covers |
| `revenue_type` | `deal.revenueType` | enum | **W** | `new`, `renewal`, `expansion`, `contraction`, `cross_sell` — high-level deal categorization |

---

## 2. Line Item (HubSpot native — `line_items`)

DealHub writes these on the deal during quoting. On close-won, the
contract API copies them to the contract; recurring items also create
subscription segments.

> **Note:** `dh_quantity`, `dh_duration`, and `product_tag` are DealHub-
> managed custom properties on `line_items`. `dh_quantity` and `dh_duration`
> replace the HubSpot-native `quantity` and `hs_recurring_billing_period`
> fields; `product_tag` is the explicit recurring/one-time classifier that
> drives subscription segment creation. The HubSpot defaults are no longer
> read or written by the contract API.

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values / Notes |
|---|---|---|---|---|
| `name` | `dealLineItems[].name` | string | **W** | Product display name |
| `dh_quantity` | `dealLineItems[].quantity` | number | **W** | Integer seats/units (DealHub custom property) |
| `price` | `dealLineItems[].unitPrice` | number | **W** | Unit price (per quantity) |
| `hs_sku` | `dealLineItems[].sku` | string | **W** | Product code: `LQ`, `FCM`, etc. The contract API uses the **full SKU** as `product_code` on the resulting segment — distinct SKUs (e.g. `SUB-MO`, `SUB-SU`) stay distinct. |
| `description` | `dealLineItems[].description` | string | **W** | Optional |
| `product_tag` | `dealLineItems[].productTag` | enum | **W** | **Primary recurring/one-time signal.** Values: `Recurring` or `One-time`. `Recurring` lines create a subscription segment; `One-time` lines live on the contract as line items only. DealHub custom property. |
| `dh_duration` | `dealLineItems[].duration` | number | **W** | Number of months (12 = annual, 1 = monthly, 3 = quarterly, 6 = semi-annual). Used as a secondary signal when `product_tag` is unset, and to compute segment span. |
| `hs_recurring_billing_start_date` | `dealLineItems[].recurringBillingStartDate` | date | **W** | Preferred — start date for this line's billing period. See "Line item date fields" below for alternates. |
| `hs_recurring_billing_end_date` | `dealLineItems[].recurringBillingEndDate` | date | **W** | Optional — end date for this line's billing period. If omitted, the contract API computes it from `recurringBillingStartDate + dh_duration`. |
| `start_date` | `dealLineItems[].startDate` | date | **W** | Alternate to `hs_recurring_billing_start_date`. The contract API tries this if the HubSpot-native field isn't set. |
| `end_date` | `dealLineItems[].endDate` | date | **W** | Alternate to `hs_recurring_billing_end_date`. Same fallback behavior. |
| `revenue_type` | `dealLineItems[].revenueType` | enum | **W** | Per-line: `new`, `renewal`, `expansion`, `contraction`, `cross_sell` — set on each line item |

> **Critical:** `revenue_type` is set **per line item**, not per deal. A
> single amendment can mix `expansion`, `contraction`, and `cross_sell`
> lines. The contract API reads each line's tag to set the corresponding
> subscription segment's `amendment_indicator`.

### Line item date fields — what the contract API reads

The contract API resolves each line item's `[start, end]` span using this
priority order. DealHub can populate either the HubSpot-native fields, the
simple `start_date` / `end_date` custom fields, or a mix — whichever is
present wins.

| Priority | Start source | End source |
|---|---|---|
| 1 | `hs_recurring_billing_start_date` | `hs_recurring_billing_end_date` |
| 2 | `start_date` | `end_date` |
| 3 | (start required) | `start + dh_duration months − 1 day` (when `dh_duration` is set without a payments count — typical MDQ-style payload) |
| 4 | (start required) | `start + (number_of_payments × dh_duration) − 1 day` (when both are set) |
| 5 | Deal `contract_start_date` (last-resort fallback) | Deal `contract_end_date` (last-resort fallback) |

If none of priorities 1–4 produce a usable span, the line collapses to the
deal-level contract dates — that's the failure mode that makes every
segment look identical. **Always populate at least one of priorities 1–3.**

### MDQ-style payloads are supported

DealHub may send recurring products as either:

- **One line item per product spanning the full multi-year term**
  (e.g. `LQ Advanced Module`, `start_date = 2026-05-01`,
  `end_date = 2029-04-30`, `dh_duration = 12`). The contract API splits
  this into N annual segments anchored on the line's start.

- **One line item per product per segment year (MDQ-style)**
  (e.g. `LQ Advanced Module Y1`, `start_date = 2026-05-01`,
  `end_date = 2027-04-30`; `LQ Advanced Module Y2`,
  `start_date = 2027-05-01`, `end_date = 2028-04-30`; …). The contract API
  produces one segment per line and sets `segment_year` from the segment's
  start date relative to the **contract** start (not the line's own span).

Either pattern produces the same final shape on the contract — N segments
with `segment_year = 1, 2, 3 …` based on contract anchoring. Mixed payloads
(some products multi-year, others MDQ-style) work too.

---

## 3. Contract (HubSpot custom object — `fq_contract`)

DealHub reads these for renewal/amendment context. Never written by DealHub.

### Identity & Status

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `contract_name` | `contract.name` | string | **R** | |
| `contract_number` | `contract.contractNumber` | string | **R** | |
| `sf_contract_id` | `contract.salesforceId` | string | **R** | Legacy SFDC reference |
| `status` | `contract.status` | enum | **R** | `draft`, `in_approval_process`, `active`, `future`, `expired`, `terminated` |
| `has_legacy_products` | `contract.hasLegacyProducts` | boolean | **R** | True = contract has pre-migration SKUs |

### Dates

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `startdate` | `contract.startDate` | date | **R** | Contract effective start |
| `enddate` | `contract.endDate` | date | **R** | Contract expiration |
| `co_term_date` | `contract.coTermDate` | date | **R** | Co-termination date for multi-product alignment |
| `amendment_start_date` | `contract.amendmentStartDate` | date | **R** | Most recent amendment effective date |
| `contract_renewed_on` | `contract.contractRenewedOn` | date | **R** | Last renewal processing date |

### Term

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `contract_term` | `contract.contractTerm` | number | **R** | Term length in months |
| `renewal_term` | `contract.renewalTerm` | number | **R** | Default renewal term in months |
| `evergreen` | `contract.evergreen` | boolean | **R** | Auto-renewing contract |

### Financials (rollups)

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `total_arr` | `contract.totalArr` | number | **R/RO** | Sum of active subscription segment ARR |
| `total_tcv` | `contract.totalTcv` | number | **R/RO** | Total contract value |
| `lq_arr` | `contract.arrByProduct.LQ` | number | **R/RO** | LeaseQuery ARR |
| `fcm_arr` | `contract.arrByProduct.FCM` | number | **R/RO** | FinQuery Contract Management ARR |
| `subscription_count` | `contract.subscriptionCount` | number | **R/RO** | |
| `amendment_count` | `contract.amendmentCount` | number | **R/RO** | |

### Pricing & Renewal

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `price_cap` | `contract.priceCap` | number | **R** | Max annual % increase |
| `max_uplift` | `contract.maxUplift` | number | **R** | Max % uplift on renewal |
| `renewal_uplift_rate` | `contract.renewalUpliftRate` | number | **R** | Default uplift % applied on renewal |
| `amendment_renewal_behavior` | `contract.amendmentRenewalBehavior` | enum | **R** | `latest_end_date`, `earliest_end_date` |
| `mdq_renewal_behavior` | `contract.mdqRenewalBehavior` | enum | **R** | `de_segmented` |

### Lineage

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `replaced_by_contract` | `contract.replacedByContract` | string | **R** | ID of contract that replaced this one (renewal target) |
| `replaces_contract` | `contract.replacesContract` | string | **R** | ID of contract this one replaced |

---

## 4. Subscription Segment (HubSpot custom object — `fq_subscription`)

One per product per segment year on the contract. DealHub reads these for
renewal context — particularly the **last period** (most recent year),
which is the seed for the next renewal proposal. Never written by DealHub.

### Identity & Product

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `segment_name` | `segment.segmentName` | string | **R** | e.g. "Acme Corp — LQ Year 2" |
| `subscription_number` | `segment.subscriptionNumber` | string | **R** | |
| `sf_subscription_id` | `segment.salesforceId` | string | **R** | Legacy SFDC reference |
| `product_code` | `segment.productCode` | string | **R** | `LQ`, `FCM`, etc. |
| `product_name` | `segment.productName` | string | **R** | |
| `charge_type` | `segment.chargeType` | enum | **R** | `one_time`, `recurring`, `usage` |
| `billing_frequency` | `segment.billingFrequency` | enum | **R** | `annual`, `monthly`, `quarterly`, `semiannual`, `invoice_plan` |

### Status

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `status` | `segment.status` | enum | **R** | `active`, `future`, `expired`, `terminated` |
| `amendment_indicator` | `segment.amendmentIndicator` | string | **R** | `Expansion`, `Contraction`, or empty |
| `revenue_type` | `segment.revenueType` | enum | **R** | Carried from the line item that created this segment |

### Segment Dimensions

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `segment_year` | `segment.segmentYear` | number | **R** | 1, 2, 3 ... — which year of the contract |
| `segment_label` | `segment.segmentLabel` | string | **R** | "Year 1", "Year 2" ... |
| `arr_start_date` / `start_date` | `segment.startDate` | date | **R** | Segment period start |
| `arr_end_date` / `end_date` | `segment.endDate` | date | **R** | Segment period end |

### Quantity & Pricing

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `quantity` | `segment.quantity` | number | **R** | Current quantity |
| `original_quantity` | `segment.originalQuantity` | number | **R** | Quantity at original signing |
| `renewal_quantity` | `segment.renewalQuantity` | number | **R** | Pre-set renewal quantity (if any) |
| `unit_price` | `segment.unitPrice` | number | **R** | Per-unit price |
| `list_price` | `segment.listPrice` | number | **R** | Catalog list price |
| `discount_percent` | `segment.discountPercent` | number | **R** | |
| `discount_amount` | `segment.discountAmount` | number | **R** | |

### Revenue

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `arr` | `segment.arr` | number | **R** | Annual recurring revenue for this segment |
| `mrr` | `segment.mrr` | number | **R** | Monthly recurring revenue |
| `tcv` | `segment.tcv` | number | **R** | Total contract value for this segment |

### Renewal Pricing

| HubSpot Internal Name | API JSON Path | Type | DealHub | Values |
|---|---|---|---|---|
| `renewal_price` | `segment.renewalPrice` | number | **R** | Pre-set renewal price (if any) |
| `renewal_uplift_rate` | `segment.renewalUpliftRate` | number | **R** | Per-segment uplift override |

---

## 5. Revenue Type — values & rules

| Value | When DealHub uses it |
|---|---|
| `new` | Net new product or customer (no prior contract) |
| `renewal` | Existing product carried forward to a new term |
| `expansion` | More seats / new product added to an existing contract |
| `contraction` | Seats / products reduced or removed |
| `cross_sell` | New **product family** added (e.g. LQ → FCM). Same family with new SKU = NOT cross-sell |

A single amendment deal can — and frequently will — contain line items
with different `revenue_type` values. Set per line, not per deal.

---

## 6. Object association map (read paths)

```
deal (0-3)
  ├──▶ fq_contract                          (1 source contract for renewals/amendments)
  │       └──▶ fq_subscription              (N segments per contract)
  ├──▶ company (0-2)                        (1 account)
  ├──▶ contact (0-1)                        (N contacts)
  └──▶ line_items                           (N line items currently on the deal)
```

DealHub's typical read path on an open renewal deal:
`deal → fq_contract → fq_subscription[]` → group by `segment_year` →
take the highest year → translate to renewal line items.
