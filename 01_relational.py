# Regional sales report: clean orders, join customers, and aggregate revenue.
# Run: $FLINK_HOME/bin/flink run -py 01_relational.py
#
# Expected client output (region, paid_orders, revenue_cents):
# ('North', 2, 15500)
# ('South', 2, 9000)

import pyflink.dataframe as pf


def main():
    pf.config.set("execution.runtime-mode", "batch")
    pf.config.set("parallelism.default", "1")

    orders = pf.from_records(
        [
            (1001, 10, 12000, 1000, "paid"),
            (1001, 10, 12000, 1000, "paid"),
            (1002, 20, 7000, None, "paid"),
            (1003, 10, 3000, 0, "cancelled"),
            (1004, 30, 5000, 500, "paid"),
            (1005, None, 9000, 0, "paid"),
            (1006, 20, 2000, 0, "paid"),
        ],
        schema=["order_id", "customer_id", "subtotal_cents", "discount_cents", "status"],
    )
    customers = pf.from_records(
        [(10, "North"), (20, "South"), (30, "North")],
        schema=["customer_id", "region"],
    )

    paid_orders = (
        orders.drop_duplicates()
        .drop_null(subset=["customer_id"])
        .fill_null(0, subset=["discount_cents"])
        .filter(pf.col("status") == "paid")
        .with_column("revenue_cents", pf.col("subtotal_cents") - pf.col("discount_cents"))
    )
    regional_sales = (
        paid_orders.join(customers, on="customer_id")
        .group_by("region")
        .agg(
            paid_orders=pf.col("order_id").count,
            revenue_cents=pf.col("revenue_cents").sum,
        )
        .sort("region")
    )

    for row in regional_sales.collect():
        print(tuple(row), flush=True)


if __name__ == "__main__":
    main()
