# Support-ticket report: scalar SLA rules, tag expansion, and a pandas UDAF.
# Run: $FLINK_HOME/bin/flink run -py 02_python_functions.py
#
# Expected client output (issue, mean_minutes, missed_sla_percent):
# ('billing', 45.0, 0.0)
# ('delivery', 120.0, 0.0)
# ('login', 60.0, 50.0)

import pyflink.dataframe as pf


@pf.udf(return_dtype=pf.DataType.int64())
def missed_sla(minutes, priority):
    limit = 30 if priority == "urgent" else 120
    return int(minutes > limit)


@pf.udtf(return_dtype=pf.DataType.string())
def issue_tags(tags):
    for tag in tags.split(","):
        yield tag.strip()


@pf.udaf(return_dtype=pf.DataType.float64(), func_type="pandas")
def average(values):
    return float(values.mean())


def main():
    pf.config.set("execution.runtime-mode", "batch")
    pf.config.set("parallelism.default", "1")

    tickets = pf.from_records(
        [
            ("T1", "urgent", 30, "billing,login"),
            ("T2", "normal", 60, "billing"),
            ("T3", "urgent", 90, "login"),
            ("T4", "normal", 120, "delivery"),
        ],
        schema=["ticket_id", "priority", "minutes", "tags"],
    )
    tagged_tickets = tickets.with_column(
        "missed_sla", missed_sla(pf.col("minutes"), pf.col("priority"))
    ).join_lateral(issue_tags(pf.col("tags")).alias("issue"))

    report = (
        tagged_tickets.group_by("issue")
        .agg(
            mean_minutes=average(pf.col("minutes")),
            missed_sla_rate=average(pf.col("missed_sla")),
        )
        .select(
            "issue", "mean_minutes",
            missed_sla_percent=pf.col("missed_sla_rate") * 100,
        )
        .sort("issue")
    )
    for row in report.collect():
        print(tuple(row), flush=True)


if __name__ == "__main__":
    main()
