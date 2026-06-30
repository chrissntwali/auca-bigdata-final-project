"""
visualizations.py
------------------
PART 4: Visualizations — 4 charts from real analytics results.

Charts produced:
  1. Top 10 Products by Revenue (bar chart) — from MongoDB aggregation
  2. Monthly Revenue Trend (line chart) — from Spark SQL query 4
  3. Customer Segmentation — CLV Tier Distribution (pie + bar combo)
     from Part 3 integration
  4. Conversion Rate by Device and Referrer (heatmap) — from Spark SQL query 3

Run from the visualizations/ directory:
    python3 visualizations.py
"""
import os
import json
import matplotlib
matplotlib.use("Agg")  # non-interactive backend, works in WSL without display
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd
import numpy as np
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "ecommerce_analytics"
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Consistent style across all charts
sns.set_theme(style="whitegrid", palette="muted")
COLORS = sns.color_palette("Blues_d", 10)
ACCENT = "#2196F3"

# -----------------------------------------------------------------------
# Data helpers
# -----------------------------------------------------------------------

def get_mongo_product_popularity(limit=10):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "product_name": {"$first": "$items.product_name"},
            "revenue": {"$sum": "$items.subtotal"},
            "units_sold": {"$sum": "$items.quantity"},
        }},
        {"$sort": {"revenue": -1}},
        {"$limit": limit},
    ]
    results = list(db.transactions.aggregate(pipeline))
    client.close()
    return pd.DataFrame(results)


def get_mongo_revenue_by_category():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": {
                "category_id": "$items.category_id",
                "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
            },
            "revenue": {"$sum": "$items.subtotal"},
        }},
        {"$sort": {"_id.year_month": 1}},
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "year_month": "$_id.year_month",
            "revenue": 1,
        }},
    ]
    results = list(db.transactions.aggregate(pipeline))
    client.close()
    return pd.DataFrame(results)


# Hardcoded from your actual Spark SQL run output (Query 4)
MONTHLY_REVENUE = pd.DataFrame({
    "month": ["2026-03", "2026-04", "2026-05", "2026-06"],
    "order_count": [1732, 6484, 6388, 4257],
    "unique_buyers": [1158, 1895, 1918, 1765],
    "revenue": [1554199.79, 5932224.38, 5695694.95, 3805411.86],
    "avg_order_value": [897.34, 914.90, 891.62, 893.92],
})

# Hardcoded from your actual CLV integration output
CLV_TIERS = pd.DataFrame({
    "clv_tier": ["Premium", "High Value", "Medium Value", "Low Value"],
    "user_count": [521, 497, 496, 486],
    "avg_clv_score": [64.68, 53.70, 46.89, 37.43],
    "avg_total_spent": [22159.04, 17168.33, 14070.70, 10059.69],
    "avg_orders": [19.4, 15.9, 13.9, 10.5],
    "avg_conversion_rate": [23.51, 21.27, 19.53, 17.18],
})

# Hardcoded from your actual Spark SQL run output (Query 3)
CONVERSION_DATA = pd.DataFrame({
    "device_type": ["mobile","desktop","mobile","mobile","desktop","desktop","desktop","mobile","tablet","desktop","tablet","tablet","mobile","tablet","tablet"],
    "referrer":    ["affiliate","direct","social","search_engine","search_engine","email","affiliate","email","email","social","search_engine","direct","direct","social","affiliate"],
    "conversion_rate_pct": [21.33,21.18,21.17,20.90,20.75,20.70,20.65,20.65,20.64,20.04,20.04,19.98,19.74,19.73,19.41],
})


# -----------------------------------------------------------------------
# Chart 1: Top 10 Products by Revenue
# -----------------------------------------------------------------------
def chart1_product_revenue():
    print("Generating Chart 1: Top 10 Products by Revenue...")
    df = get_mongo_product_popularity(10)

    # Shorten long product names for readability
    df["short_name"] = df["product_name"].apply(
        lambda x: (x[:28] + "...") if isinstance(x, str) and len(x) > 28 else x
    )
    df = df.sort_values("revenue", ascending=True)

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(df["short_name"], df["revenue"] / 1000,
                   color=sns.color_palette("Blues_d", len(df)))

    ax.set_xlabel("Revenue (USD thousands)", fontsize=12)
    ax.set_title("Top 10 Products by Total Revenue\n(MongoDB Aggregation Pipeline)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}K"))

    for bar, (_, row) in zip(bars, df.iterrows()):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"${row['revenue']/1000:.1f}K", va="center", fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart1_product_revenue.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# -----------------------------------------------------------------------
# Chart 2: Monthly Revenue Trend
# -----------------------------------------------------------------------
def chart2_monthly_revenue():
    print("Generating Chart 2: Monthly Revenue Trend...")
    df = MONTHLY_REVENUE.copy()

    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Revenue bars
    x = range(len(df))
    bars = ax1.bar(x, df["revenue"] / 1_000_000, color=ACCENT, alpha=0.75,
                   label="Total Revenue")
    ax1.set_ylabel("Revenue (USD millions)", fontsize=12, color=ACCENT)
    ax1.tick_params(axis="y", labelcolor=ACCENT)
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(df["month"], fontsize=11)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.1f}M"))

    # Unique buyers line on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(list(x), df["unique_buyers"], color="#FF5722", marker="o",
             linewidth=2.5, markersize=7, label="Unique Buyers")
    ax2.set_ylabel("Unique Buyers", fontsize=12, color="#FF5722")
    ax2.tick_params(axis="y", labelcolor="#FF5722")

    ax1.set_title("Monthly Revenue Trend with Unique Buyers\n(Spark SQL Analytics)",
                  fontsize=14, fontweight="bold", pad=15)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)

    # Annotate June as incomplete
    ax1.annotate("* June incomplete\n  (partial month)",
                 xy=(3, df["revenue"].iloc[3] / 1_000_000),
                 xytext=(2.3, df["revenue"].iloc[3] / 1_000_000 + 0.3),
                 fontsize=9, color="gray",
                 arrowprops=dict(arrowstyle="->", color="gray"))

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart2_monthly_revenue.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# -----------------------------------------------------------------------
# Chart 3: CLV Customer Segmentation
# -----------------------------------------------------------------------
def chart3_clv_segmentation():
    print("Generating Chart 3: CLV Customer Segmentation...")
    df = CLV_TIERS.copy()
    tier_colors = ["#1565C0", "#1E88E5", "#64B5F6", "#BBDEFB"]

    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    fig.suptitle("Customer Lifetime Value Segmentation\n(MongoDB + Spark Integration)",
                 fontsize=14, fontweight="bold", y=1.02)

    # Left: pie chart — user distribution
    wedges, texts, autotexts = axes[0].pie(
        df["user_count"],
        labels=df["clv_tier"],
        autopct="%1.1f%%",
        colors=tier_colors,
        startangle=140,
        pctdistance=0.82,
        wedgeprops=dict(edgecolor="white", linewidth=2),
    )
    for at in autotexts:
        at.set_fontsize(10)
    axes[0].set_title("User Distribution by Tier", fontsize=12, pad=10)

    # Right: grouped bar — avg spend vs avg conversion rate
    x = np.arange(len(df))
    w = 0.35
    b1 = axes[1].bar(x - w/2, df["avg_total_spent"] / 1000, w,
                     label="Avg Total Spent ($K)", color=tier_colors)
    ax_r = axes[1].twinx()
    b2 = ax_r.bar(x + w/2, df["avg_conversion_rate"], w,
                  label="Avg Conversion Rate (%)", color="#FF8F00", alpha=0.8)

    axes[1].set_xticks(list(x))
    axes[1].set_xticklabels(df["clv_tier"], fontsize=10)
    axes[1].set_ylabel("Avg Total Spent (USD thousands)", fontsize=10)
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.0f}K"))
    ax_r.set_ylabel("Avg Conversion Rate (%)", fontsize=10, color="#FF8F00")
    ax_r.tick_params(axis="y", labelcolor="#FF8F00")
    axes[1].set_title("Avg Spend & Conversion Rate by Tier", fontsize=12, pad=10)

    lines = [b1, b2]
    labels = ["Avg Total Spent ($K)", "Avg Conversion Rate (%)"]
    axes[1].legend(lines, labels, loc="upper right", fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart3_clv_segmentation.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# -----------------------------------------------------------------------
# Chart 4: Conversion Rate Heatmap by Device and Referrer
# -----------------------------------------------------------------------
def chart4_conversion_heatmap():
    print("Generating Chart 4: Conversion Rate Heatmap...")
    df = CONVERSION_DATA.copy()

    pivot = df.pivot_table(
        values="conversion_rate_pct",
        index="device_type",
        columns="referrer",
        aggfunc="mean"
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".2f",
        cmap="Blues",
        linewidths=0.5,
        linecolor="white",
        ax=ax,
        cbar_kws={"label": "Conversion Rate (%)"},
        vmin=19,
        vmax=22,
    )
    ax.set_title("Conversion Rate (%) by Device Type and Referrer\n(Spark SQL — Session Funnel Analysis)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xlabel("Referrer Source", fontsize=11)
    ax.set_ylabel("Device Type", fontsize=11)
    plt.xticks(rotation=30, ha="right")
    plt.yticks(rotation=0)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart4_conversion_heatmap.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------
def main():
    chart1_product_revenue()
    chart2_monthly_revenue()
    chart3_clv_segmentation()
    chart4_conversion_heatmap()
    print(f"\nAll 4 charts saved to: {OUTPUT_DIR}/")
    print("Files:")
    for f in ["chart1_product_revenue.png", "chart2_monthly_revenue.png",
              "chart3_clv_segmentation.png", "chart4_conversion_heatmap.png"]:
        path = os.path.join(OUTPUT_DIR, f)
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"  {f} ({size:,} bytes)")


if __name__ == "__main__":
    main()
