# Review triage: classify customer sentiment with a real OpenAI-compatible service.
# Run: $FLINK_HOME/bin/flink run -py 05_openai.py
# First set OPENAI_ENDPOINT (full chat/completions URL), OPENAI_API_KEY, and
# OPENAI_MODEL as shown in README.md. This sends three billable review requests.
#
# Expected client output, in any order (review_id, sentiment):
# (1, 'POSITIVE')
# (2, 'NEGATIVE')
# (3, 'NEUTRAL')
# Judge the labels by meaning; harmless whitespace or punctuation may vary.

import os

import pyflink.dataframe as pf


def main():
    pf.config.set("execution.runtime-mode", "streaming")
    pf.config.set("parallelism.default", "1")
    pf.set_model_provider(
        pf.OpenAIProvider(
            os.environ["OPENAI_ENDPOINT"],
            os.environ["OPENAI_API_KEY"],
            model=os.environ["OPENAI_MODEL"],
            system_prompt=(
                "Classify the sentiment of the customer review. "
                "Reply with exactly one label: POSITIVE, NEGATIVE, or NEUTRAL."
            ),
            retry_num=0,
            error_handling_strategy="FAILOVER",
        )
    )

    reviews = pf.from_records(
        [
            (1, "Excellent headphones. I love the sound and would buy them again."),
            (2, "Terrible headphones. They broke immediately and I want a refund."),
            (3, "The headphones come in a box with a USB cable."),
        ],
        schema=["review_id", "review"],
    )
    triaged_reviews = reviews.llm.predict("review").select(
        "review_id", sentiment=pf.col("output")
    )
    for row in triaged_reviews.collect():
        print(tuple(row), flush=True)


if __name__ == "__main__":
    main()
