# FinQuery — DealHub API (Cloudflare Worker)

Read-only HTTP API that demonstrates the call/response/schema chain DealHub
needs for renewal, expansion, and contraction proposals against FinQuery
deals: the deal, the source contract, all subscription segments grouped by
contract year, and renewal-ready line items derived from the most recent
period.

**This is a demo / reference endpoint, not a production API.** DealHub
queries HubSpot directly in their production integration. The worker exists
so PatchOps can show DealHub live JSON responses against the real schema
during the kickoff and so DealHub engineers have a working example to
reproduce against HubSpot themselves.

---

## What DealHub gets in one call

`GET /v1/deals/{dealId}` returns:

- The deal (name, stage, category, contract dates, owner, etc.)
- The associated contract (`fq_contract`) — discrete fields
- All subscription segments (`fq_subscription`)
- Segments **grouped by contract year** (the same grouping the Contract UIE shows)
- The **last period** — the highest-year group, which is the basis for the next renewal
- **Renewal line items** — one per product, deduped, with quantity / unit price / SKU pre-computed from the last period and ready to drop into a DealHub quote

DealHub's principal need ("fetch subscription segments and use the last period
to load line items for the renewal proposal") is satisfied by the
`renewalLineItems` array on every response.

---

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/v1/health` | Liveness probe |
| GET | `/v1/deals/{dealId}` | Full deal + contract + segments + renewal line items (one stop shop) |
| GET | `/v1/contracts/{contractId}` | Contract + segments + renewal line items (when only contract ID is known) |
| GET | `/v1/contracts/{contractId}/segments` | Just segments grouped by year |
| GET | `/v1/contracts/{contractId}/renewal-line-items?year=N` | Just renewal line items, optionally pinned to a specific year |

All responses are JSON. Full schemas in [DEALHUB_API_REFERENCE.md](./DEALHUB_API_REFERENCE.md).

---

## Auth

The worker is **unauthenticated** — it's a demo / reference endpoint, not a
production API. The only secret involved is the HubSpot Private App token
the worker uses to read from the CRM, which is stored in the
`HUBSPOT_ACCESS_TOKEN` Cloudflare secret and never leaves the worker.

If you later decide to gate this (e.g. before pointing it at production data
or sharing the URL more widely), add a bearer-token check in `worker.js`
back; the `DEALHUB_API_KEY` pattern is straightforward to reintroduce.

---

## Setup & deploy

### 1. Install Wrangler (one time)

```bash
npm install -g wrangler@latest
# or use the local devDependency:
cd "FinQuery Contracts Card/dealhub-worker"
npm install
```

### 2. Authenticate with Cloudflare

```bash
npx wrangler login
```

### 3. Set the HubSpot secret

```bash
# HubSpot Private App token with these scopes (read-only):
#   crm.objects.deals.read
#   crm.objects.line_items.read
#   crm.objects.contacts.read
#   crm.objects.companies.read
#   crm.objects.custom.read           (for fq_contract, fq_subscription)
#   crm.schemas.custom.read
npx wrangler secret put HUBSPOT_ACCESS_TOKEN
```

### 4. Deploy

```bash
npx wrangler deploy
```

You'll get a URL like `https://finquery-dealhub-api.<account>.workers.dev`.
Share that URL with DealHub during the kickoff so they can see live
responses against the schema.

### 5. (Optional) Custom domain

In Cloudflare dashboard → Workers & Pages → your worker → Settings → Domains
& Routes → Add a custom domain (e.g. `dealhub-api.finquery.com`). Or uncomment
the `routes` block in `wrangler.jsonc` and re-deploy.

### 6. Local dev

```bash
npx wrangler dev
```

Hits `http://127.0.0.1:8787` and uses a `.dev.vars` file for secrets:

```
HUBSPOT_ACCESS_TOKEN=pat-na1-...
```

---

## Smoke test after deploy

```bash
WORKER=https://finquery-dealhub-api.<account>.workers.dev

curl "$WORKER/v1/health"

curl "$WORKER/v1/deals/123456789" | jq

curl "$WORKER/v1/contracts/987654321/renewal-line-items" | jq
```

---

## Operational notes

- The worker caches HubSpot custom-object type IDs in module scope on first
  request — restart-cheap, no KV needed.
- All HubSpot calls go through a single thin client with bearer-token auth
  and structured error pass-through (HubSpot 404s become worker 404s; other
  HubSpot failures become worker 502s).
- CORS is wide open by default (`ALLOWED_ORIGINS=*`). Tighten via
  `wrangler.jsonc` if DealHub tells us the calling origin.
- No write endpoints. DealHub still writes line items to deals via the
  HubSpot CRM API (or via the Railway API's `/api/update-contract-from-deal`
  on close-won), not through this worker.

---

## Relationship to existing systems

```
PatchOps demo / DealHub eng ─▶ Cloudflare Worker (this) ─▶ HubSpot CRM API
                                       (read-only)
HubSpot Deal Card ───────────▶ Railway API (existing)   ─▶ HubSpot CRM API
                                       (read + write)
DealHub (production) ─────────────────────────────────── ▶ HubSpot CRM API
                                       (their own integration)
```

The worker mirrors the year-grouping logic from `FinQueryContractCard.jsx`
so DealHub's view of "Year 3 segments" matches what FinQuery CSMs see in
HubSpot. DealHub reproduces the same logic against HubSpot directly in
their own integration.
