import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime


# -------------------------
# Normalization Helpers
# -------------------------
def normalize_date(row, date_col="date"):
    try:
        row[date_col] = datetime.strptime(row[date_col], "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        row[date_col] = None
    return row

def normalize_number(row, num_col="amount"):
    try:
        row[num_col] = float(str(row[num_col]).replace(",", ""))
    except Exception:
        row[num_col] = 0.0
    return row


# -------------------------
# Join Helpers
# -------------------------
def inner_join(element):
    cid, groups = element
    customers, orders = groups.get("customer", []), groups.get("order", [])
    for c in customers:
        for o in orders:
            yield {"customer_id": cid, "customer_name": c["name"], "order_id": o["order_id"], "amount": o["amount"]}

def left_join(element):
    cid, groups = element
    customers, orders = groups.get("customer", []), groups.get("order", [])
    if customers:
        for c in customers:
            if orders:  # match exists
                for o in orders:
                    yield {"customer_id": cid, "customer_name": c["name"], "order_id": o["order_id"], "amount": o["amount"]}
            else:  # no match → NULLs for order
                yield {"customer_id": cid, "customer_name": c["name"], "order_id": None, "amount": None}

def right_join(element):
    cid, groups = element
    customers, orders = groups.get("customer", []), groups.get("order", [])
    if orders:
        for o in orders:
            if customers:  # match exists
                for c in customers:
                    yield {"customer_id": cid, "customer_name": c["name"], "order_id": o["order_id"], "amount": o["amount"]}
            else:  # no match → NULLs for customer
                yield {"customer_id": cid, "customer_name": None, "order_id": o["order_id"], "amount": o["amount"]}


# -------------------------
# Pipeline
# -------------------------
def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:

        # Customers
        customers = (
            p
            | "Create Customers" >> beam.Create([
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
                {"customer_id": "C3", "name": "Charlie"},
            ])
            | "Map Customers by ID" >> beam.Map(lambda c: (c["customer_id"], c))
        )

        # Orders
        orders = (
            p
            | "Create Orders" >> beam.Create([
                {"order_id": "O1", "customer_id": "C1", "date": "08/20/2025", "amount": "1,000"},
                {"order_id": "O2", "customer_id": "C1", "date": "08/21/2025", "amount": "2,500"},
                {"order_id": "O3", "customer_id": "C2", "date": "08/22/2025", "amount": "3,000"},
                {"order_id": "O4", "customer_id": "C4", "date": "08/23/2025", "amount": "1,200"},
            ])
            | "Normalize Orders Date" >> beam.Map(normalize_date)
            | "Normalize Orders Amount" >> beam.Map(normalize_number)
            | "Map Orders by CustID" >> beam.Map(lambda o: (o["customer_id"], o))
        )

        # -------------------------
        # INNER JOIN
        # -------------------------
        inner = (
            {"customer": customers, "order": orders}
            | "Inner CoGroupByKey" >> beam.CoGroupByKey()
            | "Do Inner Join" >> beam.FlatMap(inner_join)
        )

        # -------------------------
        # LEFT JOIN
        # -------------------------
        left = (
            {"customer": customers, "order": orders}
            | "Left CoGroupByKey" >> beam.CoGroupByKey()
            | "Do Left Join" >> beam.FlatMap(left_join)
        )

        # -------------------------
        # RIGHT JOIN
        # -------------------------
        right = (
            {"customer": customers, "order": orders}
            | "Right CoGroupByKey" >> beam.CoGroupByKey()
            | "Do Right Join" >> beam.FlatMap(right_join)
        )

        # -------------------------
        # OUTPUT
        # -------------------------
        inner | "Print Inner Join" >> beam.Map(lambda x: print("INNER:", x))
        left | "Print Left Join" >> beam.Map(lambda x: print("LEFT:", x))
        right | "Print Right Join" >> beam.Map(lambda x: print("RIGHT:", x))


if __name__ == "__main__":
    run()
