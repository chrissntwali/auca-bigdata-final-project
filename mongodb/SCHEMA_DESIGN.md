# MongoDB Schema Design

## Why MongoDB for these three collections

MongoDB's document model is the right fit wherever data is naturally **nested,
read together, and queried as a whole object** rather than joined row-by-row.
All three collections below share that property: a single read of "the
product", "the user", or "the transaction" pulls back everything an
application screen or analytics query typically needs in one round trip.

---

## 1. `products` collection

```json
{
  "_id": "prod_00123",
  "product_id": "prod_00123",
  "name": "Innovative Executive Paradigm",
  "category_id": "cat_007",
  "category_name": "Johnson-Williams",          // denormalized for fast reads
  "subcategory_id": "sub_007_01",
  "subcategory_name": "Mesh Virtual Deliverables", // denormalized
  "base_price": 129.99,
  "current_stock": 47,
  "is_active": true,
  "price_history": [
    { "price": 149.99, "date": ISODate("2024-12-20") },
    { "price": 129.99, "date": ISODate("2025-02-15") }
  ],
  "creation_date": ISODate("2024-12-20")
}
```

**Design decisions**
- `_id` is set to `product_id` directly (no separate ObjectId) since
  `product_id` is already globally unique and is the natural lookup key for
  every query pattern we have (product detail page, cart, transaction line
  items all reference it).
- `price_history` is **embedded** as an array, not a separate collection.
  Price history is small (1-3 entries), always read together with the
  product, and never queried independently across products — a classic
  "embed when it's always accessed together and bounded in size" case.
- `category_name` / `subcategory_name` are **denormalized** (duplicated) onto
  the product so that product listing and search queries never need a join
  back to `categories`. This trades a small amount of update complexity
  (renaming a category requires updating N products) for much faster reads,
  which is the right trade for a catalog that's read far more than it's
  written.

**Indexes**
```js
db.products.createIndex({ category_id: 1, is_active: 1 })
db.products.createIndex({ "price_history.date": -1 })
```

---

## 2. `users` collection

```json
{
  "_id": "user_000042",
  "user_id": "user_000042",
  "geo_data": {
    "city": "North Michaelville",
    "state": "WY",
    "country": "US"
  },
  "registration_date": ISODate("2024-12-15T08:42:13"),
  "last_active": ISODate("2025-03-12T16:23:47"),

  // --- Embedded, frequently-accessed summary (computed during load) ---
  "purchase_summary": {
    "total_orders": 4,
    "total_spent": 612.45,
    "avg_order_value": 153.11,
    "last_purchase_date": ISODate("2025-04-01T18:27:15"),
    "favorite_category": "cat_007"
  }
}
```

**Design decisions**
- Raw geo and registration fields are embedded directly (1:1 with the user,
  never queried separately).
- `purchase_summary` is a **materialized/denormalized summary**, computed
  once during the ETL load by aggregating `transactions`, then embedded back
  onto the user document. This is the classic MongoDB "computed pattern":
  instead of running an expensive aggregation across all transactions every
  time we need to segment users (e.g., "users who spent >$500"), we pay the
  cost once at load/update time and get O(1) reads afterward. The trade-off
  — summary can go stale until the next batch refresh — is acceptable for
  analytics use cases that don't require millisecond-fresh totals.
- Full transaction history is **not** embedded here (would be unbounded
  growth on a single document, against MongoDB's 16MB document limit and
  best practice). It stays in its own `transactions` collection,
  referenced by `user_id`.

**Indexes**
```js
db.users.createIndex({ "geo_data.country": 1, "geo_data.state": 1 })
db.users.createIndex({ "purchase_summary.total_spent": -1 })
db.users.createIndex({ registration_date: 1 })
```

---

## 3. `transactions` collection

```json
{
  "_id": "txn_c8d9e7f3a2b1",
  "transaction_id": "txn_c8d9e7f3a2b1",
  "session_id": "sess_a7b3c9d8e2",
  "user_id": "user_000042",
  "timestamp": ISODate("2025-03-12T14:52:41"),
  "items": [
    {
      "product_id": "prod_00123",
      "product_name": "Innovative Executive Paradigm",  // denormalized
      "category_id": "cat_007",                          // denormalized
      "quantity": 2,
      "unit_price": 129.99,
      "subtotal": 259.98
    }
  ],
  "subtotal": 259.98,
  "discount": 25.99,
  "total": 233.99,
  "payment_method": "credit_card",
  "status": "completed"
}
```

**Design decisions**
- `items` (line items) are **embedded**: a transaction and its line items
  are created together, read together (order confirmation, receipt,
  refunds), and never independently queried at scale outside the context of
  their parent transaction. Embedding avoids a join on every order-history
  read.
- `product_name` and `category_id` are denormalized onto each line item so
  that revenue-by-category and product-popularity aggregations
  (`PART 1` requirement) run directly off `transactions` without a `$lookup`
  join to `products` for every line item — a meaningful performance win at
  scale, since transactions vastly outnumber products.
- Top-level `transaction_id`/`user_id`/`session_id`/`timestamp` are kept
  flat (not nested) because they're the primary filter/sort/join keys used
  in nearly every query.

**Indexes**
```js
db.transactions.createIndex({ user_id: 1, timestamp: -1 })
db.transactions.createIndex({ "items.category_id": 1 })
db.transactions.createIndex({ timestamp: -1 })
db.transactions.createIndex({ status: 1 })
```

---

## Why this data lives in MongoDB and not HBase

| Concern | MongoDB choice |
|---|---|
| Data shape | Naturally nested (line items, price history, summaries) |
| Access pattern | "Give me everything about this one entity" (product page, order receipt, user profile) |
| Query needs | Rich ad-hoc aggregation (`$group`, `$lookup`, `$facet`) for business analytics |
| Write pattern | Moderate volume, document-at-a-time writes (one order, one product update) |
| Schema stability | Mostly stable but benefits from flexible/optional fields (e.g., not every product needs identical metadata) |

This is the inverse of what HBase is good at (see `hbase/SCHEMA_DESIGN.md`):
HBase is reserved for the **high-volume, narrow, time-series event stream**
(session page views, per-day product metrics) where rows are written once,
scanned by row-key range, and rarely updated in place.
