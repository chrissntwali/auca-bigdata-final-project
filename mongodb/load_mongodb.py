"""
load_mongodb.py
----------------
Loads the generated e-commerce dataset into MongoDB, applying the schema
design described in SCHEMA_DESIGN.md:
  - products: embeds price_history, denormalizes category/subcategory names
  - users: embeds a computed purchase_summary (materialized aggregate)
  - transactions: embeds line items, denormalizes product_name/category_id

Run after mongod is running locally (mongosh --eval "db.version()" should
succeed) and after ../data/dataset_generator.py has produced the JSON files.

Usage:
    pip install pymongo
    python load_mongodb.py
"""
import json
import glob
import os
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient, UpdateOne, InsertOne, ASCENDING, DESCENDING

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "ecommerce_analytics"
DATA_DIR = os.environ.get("DATA_DIR", "../data")


def parse_dt(s):
    return datetime.fromisoformat(s) if s else None


def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path) as f:
        return json.load(f)


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print("Loading raw JSON files...")
    categories = load_json("categories.json")
    products = load_json("products.json")
    users = load_json("users.json")
    transactions = load_json("transactions.json")

    # Lookup maps for denormalization
    cat_by_id = {c["category_id"]: c for c in categories}
    subcat_by_id = {}
    for c in categories:
        for s in c["subcategories"]:
            subcat_by_id[s["subcategory_id"]] = s

    product_by_id = {p["product_id"]: p for p in products}

    # ---------------------------------------------------------------
    # 1. Load products (denormalize category/subcategory names)
    # ---------------------------------------------------------------
    print(f"Preparing {len(products):,} product documents...")
    db.products.drop()
    product_docs = []
    for p in products:
        cat = cat_by_id.get(p["category_id"], {})
        subcat = subcat_by_id.get(p.get("subcategory_id"), {})
        doc = {
            "_id": p["product_id"],
            "product_id": p["product_id"],
            "name": p["name"],
            "category_id": p["category_id"],
            "category_name": cat.get("name"),
            "subcategory_id": p.get("subcategory_id"),
            "subcategory_name": subcat.get("name"),
            "base_price": p["base_price"],
            "current_stock": p["current_stock"],
            "is_active": p["is_active"],
            "price_history": [
                {"price": ph["price"], "date": parse_dt(ph["date"])}
                for ph in p["price_history"]
            ],
            "creation_date": parse_dt(p["creation_date"]),
        }
        product_docs.append(doc)

    if product_docs:
        db.products.insert_many(product_docs, ordered=False)
    db.products.create_index([("category_id", ASCENDING), ("is_active", ASCENDING)])
    db.products.create_index([("price_history.date", DESCENDING)])
    print(f"  -> Inserted {db.products.count_documents({}):,} products")

    # ---------------------------------------------------------------
    # 2. Compute purchase summaries per user from transactions
    # ---------------------------------------------------------------
    print("Computing per-user purchase summaries...")
    summary = defaultdict(lambda: {
        "total_orders": 0, "total_spent": 0.0,
        "last_purchase_date": None, "category_counts": defaultdict(int)
    })

    for t in transactions:
        uid = t["user_id"]
        s = summary[uid]
        s["total_orders"] += 1
        s["total_spent"] += t["total"]
        ts = parse_dt(t["timestamp"])
        if s["last_purchase_date"] is None or ts > s["last_purchase_date"]:
            s["last_purchase_date"] = ts
        for item in t["items"]:
            prod = product_by_id.get(item["product_id"])
            if prod:
                s["category_counts"][prod["category_id"]] += item["quantity"]

    # ---------------------------------------------------------------
    # 3. Load users (embed computed purchase_summary)
    # ---------------------------------------------------------------
    print(f"Preparing {len(users):,} user documents...")
    db.users.drop()
    user_docs = []
    for u in users:
        s = summary.get(u["user_id"])
        if s and s["total_orders"] > 0:
            fav_cat = max(s["category_counts"].items(), key=lambda kv: kv[1])[0] if s["category_counts"] else None
            purchase_summary = {
                "total_orders": s["total_orders"],
                "total_spent": round(s["total_spent"], 2),
                "avg_order_value": round(s["total_spent"] / s["total_orders"], 2),
                "last_purchase_date": s["last_purchase_date"],
                "favorite_category": fav_cat,
            }
        else:
            purchase_summary = {
                "total_orders": 0, "total_spent": 0.0,
                "avg_order_value": 0.0, "last_purchase_date": None,
                "favorite_category": None,
            }

        doc = {
            "_id": u["user_id"],
            "user_id": u["user_id"],
            "geo_data": u["geo_data"],
            "registration_date": parse_dt(u["registration_date"]),
            "last_active": parse_dt(u["last_active"]),
            "purchase_summary": purchase_summary,
        }
        user_docs.append(doc)

    if user_docs:
        db.users.insert_many(user_docs, ordered=False)
    db.users.create_index([("geo_data.country", ASCENDING), ("geo_data.state", ASCENDING)])
    db.users.create_index([("purchase_summary.total_spent", DESCENDING)])
    db.users.create_index([("registration_date", ASCENDING)])
    print(f"  -> Inserted {db.users.count_documents({}):,} users")

    # ---------------------------------------------------------------
    # 4. Load transactions (denormalize product_name/category_id per item)
    # ---------------------------------------------------------------
    print(f"Preparing {len(transactions):,} transaction documents...")
    db.transactions.drop()
    BATCH = 5000
    txn_docs = []
    inserted = 0
    for t in transactions:
        items = []
        for item in t["items"]:
            prod = product_by_id.get(item["product_id"], {})
            items.append({
                "product_id": item["product_id"],
                "product_name": prod.get("name"),
                "category_id": prod.get("category_id"),
                "quantity": item["quantity"],
                "unit_price": item["unit_price"],
                "subtotal": item["subtotal"],
            })
        doc = {
            "_id": t["transaction_id"],
            "transaction_id": t["transaction_id"],
            "session_id": t.get("session_id"),
            "user_id": t["user_id"],
            "timestamp": parse_dt(t["timestamp"]),
            "items": items,
            "subtotal": t["subtotal"],
            "discount": t["discount"],
            "total": t["total"],
            "payment_method": t["payment_method"],
            "status": t["status"],
        }
        txn_docs.append(doc)
        if len(txn_docs) >= BATCH:
            db.transactions.insert_many(txn_docs, ordered=False)
            inserted += len(txn_docs)
            txn_docs = []
            print(f"  ...{inserted:,} transactions inserted")
    if txn_docs:
        db.transactions.insert_many(txn_docs, ordered=False)
        inserted += len(txn_docs)

    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    db.transactions.create_index([("items.category_id", ASCENDING)])
    db.transactions.create_index([("timestamp", DESCENDING)])
    db.transactions.create_index([("status", ASCENDING)])
    print(f"  -> Inserted {db.transactions.count_documents({}):,} transactions")

    print("\nMongoDB load complete.")
    print(f"  products:     {db.products.count_documents({}):,}")
    print(f"  users:        {db.users.count_documents({}):,}")
    print(f"  transactions: {db.transactions.count_documents({}):,}")


if __name__ == "__main__":
    main()
