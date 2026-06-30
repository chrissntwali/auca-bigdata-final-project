"""
aggregations.py
----------------
Three non-trivial MongoDB aggregation pipelines satisfying PART 1's
requirement (at least two):

  1. Product popularity analysis      -> top-selling products by revenue & units
  2. User segmentation                -> users bucketed by purchasing frequency/spend
  3. Revenue analytics                -> revenue by category over time (monthly)

Run after load_mongodb.py has populated the database.

Usage:
    python aggregations.py
"""
import os
import json
from datetime import datetime
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "ecommerce_analytics"


def pretty(title, results):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
    for r in results:
        print(json.dumps(r, indent=2, default=str))


def product_popularity(db, limit=10):
    """
    Top-selling products by total revenue and units sold, joined against
    the products collection to enrich with current stock / active status.
    """
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "product_name": {"$first": "$items.product_name"},
            "category_id": {"$first": "$items.category_id"},
            "units_sold": {"$sum": "$items.quantity"},
            "revenue": {"$sum": "$items.subtotal"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"revenue": -1}},
        {"$limit": limit},
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "product_info",
        }},
        {"$unwind": {"path": "$product_info", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "product_id": "$_id",
            "product_name": 1,
            "category_id": 1,
            "units_sold": 1,
            "revenue": {"$round": ["$revenue", 2]},
            "order_count": 1,
            "current_stock": "$product_info.current_stock",
            "is_active": "$product_info.is_active",
        }},
    ]
    return list(db.transactions.aggregate(pipeline))


def user_segmentation(db):
    """
    Buckets users by purchasing frequency (order count) using the
    pre-computed purchase_summary embedded on each user document --
    demonstrating the "computed pattern" read benefit (no re-scan of
    transactions needed).
    """
    pipeline = [
        {"$bucket": {
            "groupBy": "$purchase_summary.total_orders",
            "boundaries": [0, 1, 3, 6, 11, 100000],
            "default": "11+",
            "output": {
                "user_count": {"$sum": 1},
                "avg_total_spent": {"$avg": "$purchase_summary.total_spent"},
                "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
            }
        }},
        {"$project": {
            "_id": 0,
            "order_count_bucket": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$_id", 0]}, "then": "0 orders (never purchased)"},
                        {"case": {"$eq": ["$_id", 1]}, "then": "1-2 orders"},
                        {"case": {"$eq": ["$_id", 3]}, "then": "3-5 orders"},
                        {"case": {"$eq": ["$_id", 6]}, "then": "6-10 orders"},
                        {"case": {"$eq": ["$_id", 11]}, "then": "11+ orders"},
                    ],
                    "default": "$_id"
                }
            },
            "user_count": 1,
            "avg_total_spent": {"$round": ["$avg_total_spent", 2]},
            "avg_order_value": {"$round": ["$avg_order_value", 2]},
        }}
    ]
    return list(db.users.aggregate(pipeline))


def revenue_by_category_monthly(db):
    """
    Revenue analytics: total revenue per category, per month, sorted
    chronologically -- supports the "sales performance over time/category"
    visualization required in PART 4.
    """
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": {
                "category_id": "$items.category_id",
                "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
            },
            "revenue": {"$sum": "$items.subtotal"},
            "units_sold": {"$sum": "$items.quantity"},
        }},
        {"$sort": {"_id.year_month": 1, "revenue": -1}},
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "year_month": "$_id.year_month",
            "revenue": {"$round": ["$revenue", 2]},
            "units_sold": 1,
        }},
    ]
    return list(db.transactions.aggregate(pipeline))


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    pretty("1. PRODUCT POPULARITY (Top 10 by revenue)", product_popularity(db))
    pretty("2. USER SEGMENTATION (by purchase frequency)", user_segmentation(db))
    results = revenue_by_category_monthly(db)
    pretty(f"3. REVENUE BY CATEGORY x MONTH (first 15 of {len(results)} rows)", results[:15])


if __name__ == "__main__":
    main()
