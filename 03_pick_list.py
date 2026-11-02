# Warehouse pick list: combine baskets, unpack bundles, normalize batches, and rank demand.
# Run: $FLINK_HOME/bin/flink run -py 03_pick_list.py
#
# Expected final top three (sku, required_units):
# ('TEA', 8)
# ('COFFEE', 7)
# ('MUG', 6)
# Each breakfast kit contains one coffee and two teas. Both coffee-only baskets
# count toward demand. BISCUIT has five units but falls outside the top three.
# Output is a changelog: +I/+U add a row; -U/-D remove a row. After applying
# all changes, the remaining rows must be the three above. Intermediate rows
# and their order may vary. The bounded baskets let the streaming job finish.

import pyflink.dataframe as pf


def normalize_items(batch):
    return {
        "sku": batch["sku"].str.strip().str.upper(),
        "units": batch["units"],
    }


def unpack_bundles(basket_item):
    sku, quantity = basket_item["item"].split(":")
    units = int(quantity)
    if sku.strip().upper() == "BREAKFAST-KIT":
        yield {"sku": "COFFEE", "units": units}
        yield {"sku": "TEA", "units": units * 2}
    else:
        yield {"sku": sku, "units": units}


def total_demand(items):
    return items.group_by("sku").agg(required_units=pf.col("units").sum)


def main():
    pf.config.set("execution.runtime-mode", "streaming")
    pf.config.set("parallelism.default", "1")

    web_baskets = pf.from_records(
        [
            (["coffee:2"],),
            ([" breakfast-kit:2 "],),
            (["tea:1", "mug:4"],),
        ],
        schema=["items"],
    )
    store_baskets = pf.from_records(
        [
            (["coffee:2"],),
            (["biscuit:5", "breakfast-kit:1"],),
            (["mug:2", "tea:1"],),
        ],
        schema=["items"],
    )
    item_type = pf.DataType.struct({
        "sku": pf.DataType.string(),
        "units": pf.DataType.int64(),
    })

    basket_items = web_baskets.union_all(store_baskets).explode(
        "items", output_column="item", ignore_empty_and_null=True
    )
    unpacked_items = basket_items.flat_map(unpack_bundles, return_dtype=item_type)
    normalized_items = unpacked_items.map_batches(
        normalize_items, batch_format="pandas", return_dtype=item_type
    )
    pick_list = (
        normalized_items.pipe(total_demand)
        .sort(["required_units", "sku"], descending=[True, False])
        .head(3)
    )
    with pick_list[["sku", "required_units"]].iter_rows(include_row_kind=True) as rows:
        for row in rows:
            print(row["__row_kind__"], (row["sku"], row["required_units"]), flush=True)


if __name__ == "__main__":
    main()
