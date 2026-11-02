# Order ingestion: publish valid CSV orders to Kafka and archive the consumed feed.
# Run: bash kafka.sh start
#      $FLINK_HOME/bin/flink run -py 06_connectors.py
#
# Expected client output (order_id, customer, amount_cents), in either order:
# (1001, 'Alice', 1200)
# (1003, 'Carol', 2500)
# The printed directory contains rejected/ (CSV: 1002,Bob,-50) and archive/
# (JSON containing exactly the two valid orders). Inspect the printed read-back
# and these files. Ignore generated filenames and hidden metadata files.
# Each run uses a fresh Kafka topic. The consumer stops at the completed
# producer's latest offsets, so the job finishes without manual cancellation.

from pathlib import Path
from tempfile import mkdtemp
from uuid import uuid4

import pyflink.dataframe as pf


def main():
    pf.config.set("execution.runtime-mode", "streaming")
    pf.config.set("execution.checkpointing.interval", "1 s")
    pf.config.set("parallelism.default", "1")

    directory = Path(mkdtemp(prefix="order-ingestion-"))
    topic = f"orders-{uuid4().hex}"
    print("Output directory:", directory, flush=True)
    print("Kafka topic:", topic, flush=True)
    input_path = directory / "orders.csv"
    input_path.write_text(
        "1001,Alice,1200\n1002,Bob,-50\n1003,Carol,2500\n", encoding="utf-8"
    )
    schema = {
        "order_id": pf.DataType.int64(),
        "customer": pf.DataType.string(),
        "amount_cents": pf.DataType.int64(),
    }
    orders = pf.read_generic(
        "filesystem",
        schema=schema,
        options={"path": input_path.as_uri(), "format": "csv"},
    )
    valid_orders = orders.filter(pf.col("amount_cents") > 0)
    rejected_orders = orders.filter(pf.col("amount_cents") <= 0)

    publication = pf.create_statement_set()
    valid_orders.write_kafka(
        "localhost:9092",
        topic=topic,
        format="json",
        delivery_guarantee="at-least-once",
        statement_set=publication,
    )
    rejected_orders.write_generic(
        "filesystem",
        options={"path": (directory / "rejected").as_uri(), "format": "csv"},
        statement_set=publication,
    )
    publication.execute().wait()

    received_orders = pf.read_kafka(
        "localhost:9092",
        topic=topic,
        schema=schema,
        format="json",
        group_id=f"archive-{uuid4().hex}",
        startup_mode="earliest-offset",
        bounded_mode="latest-offset",
        topic_partition_discovery_interval=None,
    )
    archival = pf.create_statement_set()
    received_orders.write_json(
        (directory / "archive").as_uri(), statement_set=archival
    )
    archival.execute().wait()

    archived_orders = pf.read_json(
        (directory / "archive").as_uri(), schema=schema
    )
    for row in archived_orders.collect():
        print(tuple(row), flush=True)


if __name__ == "__main__":
    main()
