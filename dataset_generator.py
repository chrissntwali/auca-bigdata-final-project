"""
dataset_generator.py
---------------------
E-commerce synthetic dataset generator for AUCA Big Data Analytics Final Project.

This is a SCALED-DOWN version of the course-provided generator. The schema,
field names, and generation LOGIC are identical to the original script — only
the volume constants (NUM_USERS, NUM_PRODUCTS, NUM_SESSIONS, NUM_TRANSACTIONS)
are reduced so that the full pipeline (MongoDB load -> HBase load -> Spark jobs
-> visualizations) can be generated, loaded, and queried within a reasonable
time on a single laptop, while still demonstrating "big data" characteristics
(hundreds of thousands of nested documents/events).

If you have more time/compute, raise the constants below back toward the
assignment's original targets (NUM_USERS=10000, NUM_PRODUCTS=5000,
NUM_TRANSACTIONS=500000, NUM_SESSIONS=2000000) and simply re-run this script.

Run:
    pip install faker pandas numpy
    python dataset_generator.py
"""
import json
import random
import datetime
import uuid
import threading
import numpy as np
from faker import Faker

fake = Faker()

# --- Configuration (SCALED DOWN from assignment defaults for tractability) ---
NUM_USERS = 2000          # original: 10000
NUM_PRODUCTS = 800         # original: 5000
NUM_CATEGORIES = 25         # unchanged
NUM_TRANSACTIONS = 30000   # original: 500000
NUM_SESSIONS = 100000      # original: 2000000
TIMESPAN_DAYS = 90          # unchanged
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe

# --- Initialization ---
np.random.seed(42)
random.seed(42)
Faker.seed(42)

print("Initializing dataset generation...")


# --- ID Generators ---
def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"


def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"


# --- Inventory Management ---
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()  # For thread safety

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


# --- Helper Functions ---
def determine_page_type(position, previous_pages):
    """
    Determine page type based on position in user journey and previous pages viewed.
    """
    if position == 0:
        return random.choice(["home", "search", "category_listing"])

    if not previous_pages:
        return "home"

    prev_page = previous_pages[-1]["page_type"]

    if prev_page == "home":
        return random.choices(
            ["category_listing", "search", "product_detail"],
            weights=[0.5, 0.3, 0.2]
        )[0]
    elif prev_page == "category_listing":
        return random.choices(
            ["product_detail", "category_listing", "search", "home"],
            weights=[0.7, 0.1, 0.1, 0.1]
        )[0]
    elif prev_page == "search":
        return random.choices(
            ["product_detail", "search", "category_listing", "home"],
            weights=[0.6, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "product_detail":
        return random.choices(
            ["product_detail", "cart", "category_listing", "search", "home"],
            weights=[0.3, 0.3, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "cart":
        return random.choices(
            ["checkout", "product_detail", "category_listing", "home"],
            weights=[0.6, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "checkout":
        return random.choices(
            ["confirmation", "cart", "home"],
            weights=[0.8, 0.1, 0.1]
        )[0]
    elif prev_page == "confirmation":
        return random.choices(
            ["home", "product_detail", "category_listing"],
            weights=[0.6, 0.2, 0.2]
        )[0]
    else:
        return "home"


def get_page_content(page_type, products_list, categories_list, inventory):
    """
    Get appropriate product and category based on page type.
    """
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category_id = product["category_id"]
                category = next((c for c in categories_list if c["category_id"] == category_id), None)
                return product, category
            attempts += 1

        product = random.choice(products_list)
        category_id = product["category_id"]
        category = next((c for c in categories_list if c["category_id"] == category_id), None)
        return product, category

    elif page_type == "category_listing":
        category = random.choice(categories_list)
        return None, category

    else:
        return None, None


# --- Category Generation ---
categories = []
for cat_id in range(NUM_CATEGORIES):
    category = {
        "category_id": f"cat_{cat_id:03d}",
        "name": fake.company(),
        "subcategories": []
    }

    for sub_id in range(random.randint(3, 5)):
        subcategory = {
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)
        }
        category["subcategories"].append(subcategory)

    categories.append(category)

print(f"Generated {len(categories)} categories")

# --- Product Generation ---
products = []
product_creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)

for prod_id in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])

    base_price = round(random.uniform(5, 500), 2)
    price_history = []

    initial_date = fake.date_time_between(
        start_date=product_creation_start,
        end_date=product_creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3)
    )
    price_history.append({
        "price": base_price,
        "date": initial_date.isoformat()
    })

    for _ in range(random.randint(0, 2)):
        price_change_date = fake.date_time_between(
            start_date=initial_date,
            end_date="now"
        )
        new_price = round(base_price * random.uniform(0.8, 1.2), 2)
        price_history.append({
            "price": new_price,
            "date": price_change_date.isoformat()
        })
        initial_date = price_change_date

    price_history.sort(key=lambda x: x["date"])
    current_price = price_history[-1]["price"]

    products.append({
        "product_id": f"prod_{prod_id:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": current_price,
        "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": price_history,
        "creation_date": price_history[0]["date"]
    })

print(f"Generated {len(products)} products")

# --- User Generation ---
users = []
for user_id in range(NUM_USERS):
    reg_date = fake.date_time_between(
        start_date=f"-{TIMESPAN_DAYS * 3}d",
        end_date=f"-{TIMESPAN_DAYS}d"
    )

    users.append({
        "user_id": f"user_{user_id:06d}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": fake.country_code()
        },
        "registration_date": reg_date.isoformat(),
        "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat()
    })

print(f"Generated {len(users)} users")

# --- Session & Transaction Generation ---
inventory = InventoryManager(products)
sessions = []
transactions = []
transaction_counter = 0
session_counter = 0
iteration = 0

print("Generating sessions and transactions...")
while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
    iteration += 1

    if session_counter < NUM_SESSIONS:
        user = random.choice(users)
        session_id = generate_session_id()
        session_start = fake.date_time_between(
            start_date=f"-{TIMESPAN_DAYS}d",
            end_date="now"
        )
        session_duration = random.randint(30, 3600)

        page_views = []
        viewed_products = set()
        cart_contents = {}

        time_slots = sorted([0] + [random.randint(1, session_duration - 1) for _ in range(random.randint(3, 15))] + [session_duration])

        for i in range(len(time_slots) - 1):
            view_duration = time_slots[i + 1] - time_slots[i]
            page_type = determine_page_type(i, page_views)
            product, category = get_page_content(page_type, products, categories, inventory)

            if page_type == "product_detail" and product:
                product_id = product["product_id"]
                viewed_products.add(product_id)

                if random.random() < 0.3:
                    if product_id not in cart_contents:
                        cart_contents[product_id] = {
                            "quantity": 0,
                            "price": product["base_price"]
                        }

                    max_possible = min(3, inventory.get_product(product_id)["current_stock"] - cart_contents[product_id]["quantity"])
                    if max_possible > 0:
                        add_qty = random.randint(1, max_possible)
                        cart_contents[product_id]["quantity"] += add_qty

            page_views.append({
                "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": view_duration
            })

        converted = False
        if cart_contents and any(p["page_type"] in ["checkout", "confirmation"] for p in page_views):
            converted = random.random() < 0.7

        session_geo = user["geo_data"].copy()
        session_geo["ip_address"] = fake.ipv4()

        sessions.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])
            },
            "viewed_products": list(viewed_products),
            "page_views": page_views,
            "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
            "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])
        })

        session_counter += 1

        if converted and transaction_counter < NUM_TRANSACTIONS:
            transaction_items = []
            valid = True

            for prod_id, details in cart_contents.items():
                quantity = details["quantity"]
                if quantity > 0:
                    if inventory.update_stock(prod_id, quantity):
                        transaction_items.append({
                            "product_id": prod_id,
                            "quantity": quantity,
                            "unit_price": details["price"],
                            "subtotal": round(quantity * details["price"], 2)
                        })
                    else:
                        valid = False
                        break

            if valid and transaction_items:
                subtotal = sum(item["subtotal"] for item in transaction_items)
                discount = 0
                if random.random() < 0.2:
                    discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                    discount = round(subtotal * discount_rate, 2)

                total = round(subtotal - discount, 2)

                transactions.append({
                    "transaction_id": generate_transaction_id(),
                    "session_id": session_id,
                    "user_id": user["user_id"],
                    "timestamp": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
                    "items": transaction_items,
                    "subtotal": subtotal,
                    "discount": discount,
                    "total": total,
                    "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                    "status": "completed"
                })
                transaction_counter += 1

    if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
        user = random.choice(users)
        products_in_txn = random.sample(products, k=min(3, len(products)))

        transaction_items = []
        for product in products_in_txn:
            if product["is_active"]:
                quantity = random.randint(1, 3)
                if inventory.update_stock(product["product_id"], quantity):
                    transaction_items.append({
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": product["base_price"],
                        "subtotal": round(quantity * product["base_price"], 2)
                    })

        if transaction_items:
            subtotal = sum(item["subtotal"] for item in transaction_items)
            discount = 0
            if random.random() < 0.2:
                discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                discount = round(subtotal * discount_rate, 2)

            total = round(subtotal - discount, 2)

            transactions.append({
                "transaction_id": generate_transaction_id(),
                "session_id": None,
                "user_id": user["user_id"],
                "timestamp": fake.date_time_between(
                    start_date=f"-{TIMESPAN_DAYS}d",
                    end_date="now"
                ).isoformat(),
                "items": transaction_items,
                "subtotal": subtotal,
                "discount": discount,
                "total": total,
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                "status": random.choice(["completed", "processing", "shipped", "delivered"])
            })
            transaction_counter += 1

    if iteration % 10000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")


# --- Data Export ---
def json_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


print("Saving datasets...")

with open("users.json", "w") as f:
    json.dump(users, f, default=json_serializer)

with open("products.json", "w") as f:
    json.dump(list(inventory.products.values()), f, default=json_serializer)

with open("categories.json", "w") as f:
    json.dump(categories, f, default=json_serializer)

with open("transactions.json", "w") as f:
    json.dump(transactions, f, default=json_serializer)

CHUNK_SIZE = 50000
for i in range(0, len(sessions), CHUNK_SIZE):
    chunk = sessions[i:i + CHUNK_SIZE]
    with open(f"sessions_{i // CHUNK_SIZE}.json", "w") as f:
        json.dump(chunk, f, default=json_serializer)

print(f"""
Dataset generation complete!
- Users: {len(users):,}
- Products: {len(products):,}
- Categories: {len(categories):,}
- Sessions: {len(sessions):,} (target: {NUM_SESSIONS:,})
- Transactions: {len(transactions):,} (target: {NUM_TRANSACTIONS:,})
- Remaining stock units: {sum(p['current_stock'] for p in inventory.products.values()):,}
""")