# ============================================================
# generate_daily_files.py
# ============================================================

import os
import sys
import random
from datetime import datetime
import pandas as pd

from config import (
    TOTAL_CUSTOMERS,
    CUSTOMER_UPDATE_RATE,
    ORDERS_PER_DAY_MIN,
    ORDERS_PER_DAY_MAX,
    ORDER_UPDATE_RATE,
    DUPLICATE_RATE,
    BAD_RECORD_RATE
)

# ----------------------------
# GLOBAL CONFIG
# ----------------------------

BASE_DATA_PATH = "your_project_folder_path/data" # This is where the daily files will be generated
STATE_PATH = "your_project_folder_path/data_generator/state" # To hold the master file.

CUSTOMERS_MASTER_PATH = f"{STATE_PATH}/customers_master.csv"
ORDERS_MASTER_PATH = f"{STATE_PATH}/orders_master.csv"
RESTAURANTS_MASTER_PATH = f"{STATE_PATH}/restaurants_master.csv"
DELIVERY_MASTER_PATH = f"{STATE_PATH}/delivery_partners_master.csv"

CITIES = ["Delhi", "Mumbai", "Bangalore", "Hyderabad", "Pune", "Chennai"]
ORDER_STATUSES = ["PLACED", "COMPLETED", "CANCELLED"]
PAYMENT_STATUSES = ["PENDING", "SUCCESS", "FAILED"]
CUISINES = ["North Indian", "Chinese", "Italian", "Fast Food", "South Indian"]
MENU_ITEMS = ["Burger", "Pizza", "Pasta", "Biryani", "Noodles", "Sandwich", "Dosa", "Roll", "Fries", "Shake"]


# ============================================================
# UTILITIES
# ============================================================

def parse_run_date():
    # If date is passed → use it (manual runs)
    if len(sys.argv) == 2:
        return pd.to_datetime(sys.argv[1])

    # Otherwise → auto-detect next date
    if not os.path.exists(BASE_DATA_PATH):
        return pd.Timestamp.today().normalize()

    folders = [
        f for f in os.listdir(BASE_DATA_PATH)
        if os.path.isdir(os.path.join(BASE_DATA_PATH, f))
    ]

    if not folders:
        return pd.Timestamp.today().normalize()

    latest_date = max(pd.to_datetime(folders))
    next_date = latest_date + pd.Timedelta(days=1)

    return next_date

def set_seed(run_date):
    random.seed(run_date.toordinal())


def ensure_output_dir(run_date):
    path = f"{BASE_DATA_PATH}/{run_date.date()}"
    os.makedirs(path, exist_ok=True)
    return path


def enforce_customer_schema(df):
    df["customer_id"] = df["customer_id"].astype(str)
    df["customer_name"] = df["customer_name"].astype(str)
    df["city"] = df["city"].astype(str)
    df["phone"] = df["phone"].astype(str)
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", format="mixed")
    return df


def enforce_order_schema(df):
    df["order_id"] = df["order_id"].astype(str)
    df["customer_id"] = df["customer_id"].astype(str)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["amount"] = pd.to_numeric(df["amount"])
    df["payment_status"] = df["payment_status"].astype(str)
    df["order_status"] = df["order_status"].astype(str)
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", format="mixed")
    return df

def enforce_restaurant_schema(df):
    df["restaurant_id"] = df["restaurant_id"].astype(str)
    df["restaurant_name"] = df["restaurant_name"].astype(str)
    df["city"] = df["city"].astype(str)
    df["cuisine"] = df["cuisine"].astype(str)
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", format="mixed")
    return df


def enforce_delivery_schema(df):
    df["partner_id"] = df["partner_id"].astype(str)
    df["partner_name"] = df["partner_name"].astype(str)
    df["city"] = df["city"].astype(str)
    df["rating"] = pd.to_numeric(df["rating"])
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", format="mixed")
    return df

def load_master(path, schema_enforcer):
    if os.path.exists(path):
        df = pd.read_csv(path, low_memory = False)
        df = schema_enforcer(df)
        return df
    return None


def save_master(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

# ============================================================
# CUSTOMER LOGIC
# ============================================================

def generate_customer_id(i):
    return f"CUST_{i:05d}"


def bootstrap_customers(run_date):
    customers = []
    for i in range(TOTAL_CUSTOMERS):
        customers.append({
            "customer_id": generate_customer_id(i + 1),
            "customer_name": f"Customer_{i + 1}",
            "city": random.choice(CITIES),
            "phone": f"9{random.randint(100000000, 999999999)}",
            "updated_at": run_date
        })
    df = pd.DataFrame(customers)
    return enforce_customer_schema(df)


def generate_customer_updates(customers_master, run_date):
    num_updates = max(1, int(len(customers_master) * CUSTOMER_UPDATE_RATE))
    update_indices = random.sample(list(customers_master.index), num_updates)

    updated_rows = []

    for idx in update_indices:
        if random.choice(["city", "phone"]) == "city":
            customers_master.at[idx, "city"] = random.choice(CITIES)
        else:
            customers_master.at[idx, "phone"] = f"9{random.randint(100000000, 999999999)}"

        customers_master.at[idx, "updated_at"] = pd.Timestamp(run_date)
        updated_rows.append(customers_master.loc[idx])

    updated_df = pd.DataFrame(updated_rows)
    return customers_master, enforce_customer_schema(updated_df)

# ============================================================
# RESTAURANT LOGIC
# ============================================================

def generate_restaurant_id(i):
    return f"REST_{i:05d}"


def bootstrap_restaurants(run_date):
    restaurants = []
    for i in range(2000):
        restaurants.append({
            "restaurant_id": generate_restaurant_id(i + 1),
            "restaurant_name": f"Restaurant_{i+1}",
            "city": random.choice(CITIES),
            "cuisine": random.choice(CUISINES),
            "updated_at": run_date
        })

    return pd.DataFrame(restaurants)


def generate_restaurant_updates(rest_master, run_date):
    num_updates = max(1, int(len(rest_master) * CUSTOMER_UPDATE_RATE))
    indices = random.sample(list(rest_master.index), num_updates)

    updated = []

    for idx in indices:
        rest_master.at[idx, "cuisine"] = random.choice(CUISINES)
        rest_master.at[idx, "updated_at"] = run_date
        updated.append(rest_master.loc[idx])

    return rest_master, pd.DataFrame(updated)

# ============================================================
# DELIVERY PARTNER LOGIC
# ============================================================

def generate_partner_id(i):
    return f"DP_{i:05d}"


def bootstrap_delivery_partners(run_date):
    partners = []

    for i in range(5000):
        partners.append({
            "partner_id": generate_partner_id(i + 1),
            "partner_name": f"Rider_{i+1}",
            "city": random.choice(CITIES),
            "rating": round(random.uniform(3.0, 5.0), 2),
            "updated_at": run_date
        })

    return pd.DataFrame(partners)


def generate_partner_updates(master, run_date):
    num_updates = max(1, int(len(master) * CUSTOMER_UPDATE_RATE))
    indices = random.sample(list(master.index), num_updates)

    updated = []

    for idx in indices:
        master.at[idx, "rating"] = round(random.uniform(3.0, 5.0), 2)
        master.at[idx, "updated_at"] = run_date
        updated.append(master.loc[idx])

    return master, pd.DataFrame(updated)

# ============================================================
# ORDER LOGIC
# ============================================================

def generate_order_id(run_date, i):
    return f"ORD_{run_date.strftime('%Y%m%d')}_{i:06d}"


def generate_new_orders(run_date, customers_master, restaurants_master, delivery_master, is_day_1=False):
    restaurant_ids = restaurants_master["restaurant_id"].tolist()
    partner_ids = delivery_master["partner_id"].tolist()
    customer_ids = customers_master["customer_id"].tolist()

    num_orders = random.randint(ORDERS_PER_DAY_MIN, ORDERS_PER_DAY_MAX)

    orders = []

    future_days = random.sample([1, 2], k=1)  # only next 1–2 days

    future_orders_map = {}
    for d in future_days:
        future_orders_map[d] = random.randint(3, 8)  # max 10 (safe range)

    total_future_orders = sum(future_orders_map.values())
    future_order_counter = 0

    for i in range(num_orders):
        # ----------------------------
        # 🔥 FUTURE (CONTROLLED FIRST)
        # ----------------------------
        if future_order_counter < total_future_orders:
            # assign future date deterministically
            for d, count in future_orders_map.items():
                if count > 0:
                    order_date = run_date + pd.Timedelta(days=d)
                    future_orders_map[d] -= 1
                    future_order_counter += 1
                    break

        else:
            # ----------------------------
            # 🔥 NORMAL + LATE
            # ----------------------------
            p = random.random()

            late_prob = 0.05 if not is_day_1 else 0

            if p < late_prob:
                order_date = run_date - pd.Timedelta(
                    days=random.choices([1, 2], weights=[0.7, 0.3])[0]
                )
            else:
                order_date = run_date


        # ----------------------------
        # 🔥 STEP 2: Decide status based on order_date
        # ----------------------------
        if order_date == run_date:
            weights = [0.01, 0.96, 0.03]   # PLACED, COMPLETED, CANCELLED
        else:
            weights = [0.01, 0.98, 0.01]

        order_status = random.choices(ORDER_STATUSES, weights=weights)[0]


        # ----------------------------
        # 🔥 STEP 3: Remaining logic
        # ----------------------------
        restaurant_id = random.choice(restaurant_ids)

        partner_id = (
            random.choice(partner_ids)
            if order_status == "COMPLETED"
            else None
        )

        # 🔥 STATUS INCONSISTENCY
        payment_status = (
            "SUCCESS" if order_status == "COMPLETED"
            else random.choice(PAYMENT_STATUSES)
        )
        if random.random() < 0.03:
            payment_status = random.choice(["FAILED", "PENDING"])

        orders.append({
            "order_id": generate_order_id(run_date, i),
            "customer_id": random.choice(customer_ids),
            "order_date": order_date,
            "amount": round(random.uniform(100, 1500), 2),
            "payment_status": payment_status,
            "order_status": order_status,
            "updated_at": run_date,
            "restaurant_id": restaurant_id,
            "partner_id": partner_id
        })

    df = pd.DataFrame(orders)
    return enforce_order_schema(df)


def generate_order_updates(orders_master, run_date):
    if orders_master is None or orders_master.empty:
        return orders_master, pd.DataFrame()

    num_updates = max(1, int(len(orders_master) * ORDER_UPDATE_RATE))
    update_indices = random.sample(list(orders_master.index), num_updates)

    updated_orders = []
    

    for idx in update_indices:
        if orders_master.at[idx, "order_status"] == "PLACED":
            new_status = random.choice(["COMPLETED", "CANCELLED"])
            orders_master.at[idx, "order_status"] = new_status
            orders_master.at[idx, "payment_status"] = (
                "SUCCESS" if new_status == "COMPLETED" else "FAILED"
            )

            if orders_master.at[idx, "order_date"] > run_date:
                continue
            orders_master.at[idx, "updated_at"] = run_date
            updated_orders.append(orders_master.loc[idx])

    updated_df = pd.DataFrame(updated_orders)
    if not updated_df.empty:
        updated_df = enforce_order_schema(updated_df)

    return orders_master, updated_df


def inject_duplicates_and_bad_records(df):
    records = df.copy()

    # ----------------------------
    # 🔥 PARTIAL DUPLICATES (REALISTIC)
    # ----------------------------
    dup_count = int(len(records) * DUPLICATE_RATE)

    if dup_count > 0:
        dup_rows = records.sample(dup_count).copy()

        # only modify SOME columns (not full row)
        if "updated_at" in dup_rows.columns:
            dup_rows["updated_at"] = dup_rows["updated_at"].apply(lambda x: x + pd.Timedelta(minutes=random.randint(1, 60)))

        if "amount" in dup_rows.columns:
            dup_rows["amount"] = dup_rows["amount"].apply(lambda x: x * random.uniform(0.9, 1.1))

        records = pd.concat([records, dup_rows], ignore_index=True)

    # ----------------------------
    # 🔥 BAD RECORDS
    # ----------------------------
    bad_count = int(len(records) * BAD_RECORD_RATE)

    if bad_count > 0 and "amount" in records.columns:
        bad_indices = random.sample(list(records.index), bad_count)
        records.loc[bad_indices, "amount"] = -100

    return records

# ============================================================
# ORDER ITEMS LOGIC
# ============================================================

def generate_order_items(run_date, orders_df):
    items = []

    for _, order in orders_df.iterrows():
        num_items = random.randint(1, 5)

        for _ in range(num_items):
            price = round(random.uniform(50, 500), 2)
            qty = random.randint(1, 3)

            items.append({
                "order_item_id": f"ITEM_{random.randint(100000000,999999999)}",
                "order_id": order["order_id"],
                "restaurant_id": order["restaurant_id"],  # 🔥 FIXED realism
                "item_name": random.choice(MENU_ITEMS),
                "quantity": qty,
                "price": price,
                "updated_at": run_date
            })

    df = pd.DataFrame(items)

    # 🔥 REALISM: derive order amount
    order_totals = (
        df.groupby("order_id")
          .apply(lambda x: (x["price"] * x["quantity"]).sum())
          .reset_index(name="calc_amount")
    )

    return df, order_totals

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n========== DAILY DATA GENERATOR STARTED ==========")

    run_date = parse_run_date()
    set_seed(run_date)
    output_path = ensure_output_dir(run_date)

    print(f"[INFO] Run date          : {run_date.date()}")
    print(f"[INFO] Output directory : {output_path}")

    # ---------------- Customers ----------------
    print("\n--- CUSTOMER PROCESSING ---")

    customers_master = load_master(CUSTOMERS_MASTER_PATH, enforce_customer_schema)

    if customers_master is None:
        print("[INFO] Day 1 detected → bootstrapping customers")
        customers_master = bootstrap_customers(run_date)
        daily_customers = customers_master.copy()
    else:
        print("[INFO] Incremental customer updates")
        customers_master, daily_customers = generate_customer_updates(
            customers_master, run_date
        )

    customers_output = f"{output_path}/customers_{run_date.date()}.csv"
    daily_customers.to_csv(customers_output, index=False)
    save_master(customers_master, CUSTOMERS_MASTER_PATH)

    print(f"[SUCCESS] Customers written : {customers_output}")
    print(f"[INFO] Customer records   : {len(daily_customers)}")

    # ---------------- Restaurants ----------------
    print("\n--- RESTAURANT PROCESSING ---")

    restaurants_master = load_master(RESTAURANTS_MASTER_PATH, enforce_restaurant_schema)

    if restaurants_master is None:
        restaurants_master = bootstrap_restaurants(run_date)
        daily_restaurants = restaurants_master.copy()
    else:
        restaurants_master, daily_restaurants = generate_restaurant_updates(
            restaurants_master, run_date
        )

    daily_restaurants.to_csv(
        f"{output_path}/restaurants_{run_date.date()}.csv", index=False
    )

    save_master(restaurants_master, RESTAURANTS_MASTER_PATH)

    # ---------------- Delivery Partners ----------------
    print("\n--- DELIVERY PARTNER PROCESSING ---")

    delivery_master = load_master(DELIVERY_MASTER_PATH, enforce_delivery_schema)

    if delivery_master is None:
        delivery_master = bootstrap_delivery_partners(run_date)
        daily_delivery = delivery_master.copy()
    else:
        delivery_master, daily_delivery = generate_partner_updates(
            delivery_master, run_date
        )

    daily_delivery.to_csv(
        f"{output_path}/delivery_partners_{run_date.date()}.csv", index=False
    )

    save_master(delivery_master, DELIVERY_MASTER_PATH)

    # ---------------- Orders ----------------
    print("\n--- ORDER PROCESSING ---")

    orders_master = load_master(ORDERS_MASTER_PATH, enforce_order_schema)

    is_day_1 = orders_master is None

    new_orders = generate_new_orders(
        run_date,
        customers_master,
        restaurants_master,
        delivery_master,
        is_day_1=is_day_1
    )
    print(f"[INFO] New orders generated : {len(new_orders)}")

    orders_master, updated_orders = generate_order_updates(
        orders_master, run_date
    )
    print(f"[INFO] Order updates        : {len(updated_orders)}")

    clean_orders = pd.concat([new_orders, updated_orders], ignore_index=True)

    # generate items first
    order_items, order_totals = generate_order_items(run_date, clean_orders)

    # derive correct amount
    clean_orders = clean_orders.merge(order_totals, on="order_id", how="left")
    clean_orders["amount"] = clean_orders["calc_amount"]
    clean_orders.drop(columns=["calc_amount"], inplace=True)

    # 🔥 NOW inject chaos (after correctness)
    daily_orders = inject_duplicates_and_bad_records(clean_orders)
    order_items = inject_duplicates_and_bad_records(order_items)

    orders_output = f"{output_path}/orders_{run_date.date()}.csv"
    daily_orders.to_csv(orders_output, index=False)

    order_items.to_csv(
        f"{output_path}/order_items_{run_date.date()}.csv",
        index=False
    )

    if orders_master is None:
            orders_master = daily_orders.copy()
    else:
        orders_master = pd.concat(
            [orders_master, daily_orders],
            ignore_index=True
        )

    # 🔒 HARD TYPE ENFORCEMENT BEFORE SORT
    orders_master["updated_at"] = pd.to_datetime(
        orders_master["updated_at"],
        errors="coerce"
    )
    
    print("\nDEBUG updated_at types:")
    print(orders_master["updated_at"].apply(type).value_counts())

    orders_master = (
        orders_master
            .sort_values("updated_at")
            .drop_duplicates("order_id", keep="last")
    )


    save_master(orders_master, ORDERS_MASTER_PATH)

    print(f"[SUCCESS] Orders written    : {orders_output}")
    print(f"[INFO] Order records       : {len(daily_orders)}")

    print("\n========== DAILY DATA GENERATOR COMPLETED ==========\n")


if __name__ == "__main__":
    main()

