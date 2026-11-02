# Sales monitoring: price orders at purchase time, then total ten-second windows.
# Run: $FLINK_HOME/bin/flink run -py 04_windows_temporal.py
#
# Expected client output, in either order:
# 2024-01-01 00:00:00 -> 2024-01-01 00:00:10: 350 cents
# 2024-01-01 00:00:10 -> 2024-01-01 00:00:20: 450 cents
# The price changes from 100 to 150 cents at second 5. Orders at seconds 2, 6,
# and 12 cost 200, 150, and 450 cents. Both CSV inputs finish, closing the windows.
# The printed directory retains the input files.

from datetime import timedelta
from pathlib import Path
from tempfile import mkdtemp

import pyflink.dataframe as pf


def main():
    pf.config.set("execution.runtime-mode", "streaming")
    pf.config.set("parallelism.default", "1")
    pf.config.set("table.local-time-zone", "UTC")

    directory = Path(mkdtemp(prefix="priced-sales-"))
    print("Input files:", directory, flush=True)
    orders_path = directory / "orders.csv"
    orders_path.write_text(
        "1,2,2024-01-01 00:00:02\n"
        "1,1,2024-01-01 00:00:06\n"
        "1,3,2024-01-01 00:00:12\n",
        encoding="utf-8",
    )
    prices_path = directory / "prices.csv"
    prices_path.write_text(
        "1,100,2024-01-01 00:00:01\n"
        "1,150,2024-01-01 00:00:05\n",
        encoding="utf-8",
    )

    pf.get_or_create_table_environment().execute_sql(f"""
        CREATE TEMPORARY TABLE price_history (
            sku INT,
            price_cents BIGINT,
            changed_at TIMESTAMP(3),
            WATERMARK FOR changed_at AS changed_at - INTERVAL '1' SECOND,
            PRIMARY KEY (sku) NOT ENFORCED
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{prices_path.as_uri()}'
        )
    """)
    prices = pf.read_catalog_table("price_history")
    orders = pf.read_generic(
        "filesystem",
        schema={
            "sku": pf.DataType.int32(),
            "quantity": pf.DataType.int64(),
            "ordered_at": pf.DataType.timestamp(3),
        },
        options={"path": orders_path.as_uri(), "format": "csv"},
        watermark=("ordered_at", "ordered_at - INTERVAL '1' SECOND"),
    )

    priced_orders = orders.join_asof(prices, on="ordered_at", by="sku").with_column(
        "revenue_cents", pf.col("quantity") * pf.col("price_cents")
    )
    window_sales = (
        priced_orders.tumble(on="ordered_at", size=timedelta(seconds=10))
        .group_by("window_start", "window_end")
        .agg(revenue_cents=pf.col("revenue_cents").sum)
    )
    for start, end, revenue in window_sales.collect():
        print(f"{start} -> {end}: {revenue} cents", flush=True)


if __name__ == "__main__":
    main()
