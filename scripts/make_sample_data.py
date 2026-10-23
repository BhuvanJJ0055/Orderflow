"""Generate synthetic sample data matching the Olist schema with intentional dirt.

Used for testing the pipeline when real Olist CSVs are not present.
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta


def generate_sample_data(output_dir: str = "data/raw", num_customers: int = 3000, num_orders: int = 8000) -> None:
    """Create the five Olist CSV files with synthetic data and deliberate quality defects."""
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)

    categories = [
        "beleza_saude",
        "relogios_presentes",
        "cama_mesa_banho",
        "esporte_lazer",
        "informatica_acessorios",
        "moveis_decoracao",
        "utilidades_domesticas",
        None,  # deliberate null category
    ]
    statuses = [
        "delivered",
        "shipped",
        "canceled",
        "invoiced",
        "processing",
        "DELIVERED ",  # deliberate uppercase + padding
        " unknown_status ",  # deliberate invalid status
    ]
    payment_types = ["credit_card", "boleto", "voucher", "debit_card"]

    # 1. Customers
    customer_ids: list[str] = []
    customer_rows = []
    for _ in range(num_customers):
        c_id = uuid.uuid4().hex
        u_id = uuid.uuid4().hex
        customer_ids.append(c_id)
        customer_rows.append({
            "customer_id": c_id,
            "customer_unique_id": u_id,
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city": random.choice(["sao paulo", "rio de janeiro", "belo horizonte", "curitiba"]),
            "customer_state": random.choice(["SP", "RJ", "MG", "PR", "sp"]),  # mixed case
        })

    # Add 10 duplicate customer rows
    for _ in range(10):
        customer_rows.append(random.choice(customer_rows).copy())

    # Add 5 invalid customer rows missing customer_id
    for _ in range(5):
        customer_rows.append({
            "customer_id": "",
            "customer_unique_id": uuid.uuid4().hex,
            "customer_zip_code_prefix": "12345",
            "customer_city": "sao paulo",
            "customer_state": "SP",
        })

    # 2. Products
    products = []
    product_ids: list[str] = []
    for _ in range(500):
        p_id = uuid.uuid4().hex
        product_ids.append(p_id)
        cat = random.choice(categories)
        products.append({
            "product_id": p_id,
            "product_category_name": cat if cat is not None else "",
            "product_name_lenght": random.randint(10, 60),
            "product_description_lenght": random.randint(50, 500),
            "product_photos_qty": random.randint(1, 5),
            "product_weight_g": random.randint(100, 10000),
            "product_length_cm": random.randint(10, 80),
            "product_height_cm": random.randint(5, 50),
            "product_width_cm": random.randint(10, 50),
        })

    # Add duplicate product rows
    for _ in range(5):
        products.append(random.choice(products).copy())

    # 3. Orders, Order Items, Payments
    orders = []
    order_items = []
    payments = []
    start_date = datetime(2017, 1, 1)

    for _ in range(num_orders):
        order_id = uuid.uuid4().hex
        cust_id = random.choice(customer_ids)
        delta_days = random.randint(0, 600)
        purchase_dt = start_date + timedelta(days=delta_days, seconds=random.randint(0, 86400))
        status = random.choice(statuses)

        # Deliberate dirt: 1% unparseable dates
        if random.random() < 0.01:
            purchase_str = "not-a-valid-date"
        else:
            purchase_str = purchase_dt.strftime("%Y-%m-%d %H:%M:%S")

        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_status": status,
            "order_purchase_timestamp": purchase_str,
            "order_approved_at": (purchase_dt + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_carrier_date": (purchase_dt + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_customer_date": (purchase_dt + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S"),
            "order_estimated_delivery_date": (purchase_dt + timedelta(days=12)).strftime("%Y-%m-%d %H:%M:%S"),
        })

        # Items for this order
        num_items = random.randint(1, 3)
        total_order_val = 0.0
        for item_seq in range(1, num_items + 1):
            p_id = random.choice(product_ids)
            # Deliberate dirt: occasional negative or zero price
            if random.random() < 0.01:
                price = -10.0
            else:
                price = round(random.uniform(10.0, 500.0), 2)
            freight = round(random.uniform(5.0, 50.0), 2)
            if price > 0:
                total_order_val += price + freight

            order_items.append({
                "order_id": order_id,
                "order_item_id": item_seq,
                "product_id": p_id,
                "seller_id": uuid.uuid4().hex,
                "shipping_limit_date": (purchase_dt + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "freight_value": freight,
            })

        # Payment for this order
        payments.append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": random.choice(payment_types),
            "payment_installments": random.randint(1, 6),
            "payment_value": round(max(total_order_val, 15.0), 2),
        })

    # Add duplicate orders and items
    for _ in range(10):
        orders.append(random.choice(orders).copy())
    for _ in range(15):
        order_items.append(random.choice(order_items).copy())
    for _ in range(10):
        payments.append(random.choice(payments).copy())

    # Write files
    def write_csv(filename: str, rows: list, fieldnames: list):
        path = os.path.join(output_dir, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    write_csv(
        "olist_customers_dataset.csv",
        customer_rows,
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    )
    write_csv(
        "olist_products_dataset.csv",
        products,
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    )
    write_csv(
        "olist_orders_dataset.csv",
        orders,
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    write_csv(
        "olist_order_items_dataset.csv",
        order_items,
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )
    write_csv(
        "olist_order_payments_dataset.csv",
        payments,
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    )


if __name__ == "__main__":
    generate_sample_data()
    print("Sample data generated successfully in data/raw")
