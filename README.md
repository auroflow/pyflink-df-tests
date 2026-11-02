# PyFlink DataFrame Test Jobs

The following build and job steps were verified on macOS 27.0.1 using
Bash, Temurin 17.0.19, Python 3.11.15, uv 0.11.19, Docker Engine 29.2.1, and
Kafka 3.9.1. macOS is not required; the `JAVA_HOME` lookup below is macOS-specific.

## Build Community Flink

Start in a clean Apache Flink community `release-2.4` checkout. The build below
was verified on November 2, 2026 at commit
`b5730a9166106bb6257a3e566fc9268845f2d974`, producing Flink `2.4-SNAPSHOT` and
both Python packages at `2.4.dev0`.
All six jobs were run on this build and matched their expected
output, including job 06's rejected CSV and archived JSON files.

```bash
export FLINK_SOURCE="$PWD"
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"

./mvnw clean package -DskipTests -Dfast -Pskip-webui-build -T4

uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python --group flink-python/pyproject.toml:dev
uv pip install --python .venv/bin/python --no-build-isolation \
  ./flink-python/apache-flink-libraries ./flink-python
uv pip check --python .venv/bin/python

export FLINK_HOME="$FLINK_SOURCE/flink-dist/target/flink-2.4-SNAPSHOT-bin/flink-2.4-SNAPSHOT"
source "$FLINK_SOURCE/.venv/bin/activate"
export PYFLINK_CLIENT_EXECUTABLE="$FLINK_SOURCE/.venv/bin/python"
unset PYTHONPATH
```

`PYFLINK_CLIENT_EXECUTABLE` selects the submitting Python interpreter; no
additional `python.client.executable` configuration is needed.

## Start Flink

Install the Kafka connector and the OpenAI model JAR before starting the cluster.
Neither is installed in the distribution's `lib` directory by the build above.

```bash
curl -fLo "$FLINK_HOME/lib/flink-sql-connector-kafka-5.0.0-2.2.jar" \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
cp "$FLINK_SOURCE/flink-models/flink-model-openai/target/flink-model-openai-2.4-SNAPSHOT.jar" \
  "$FLINK_HOME/lib/"
```

In the existing `$FLINK_HOME/conf/config.yaml`, change
`taskmanager.numberOfTaskSlots` to `4`.
Add the following, replacing the path with the absolute value of
`$FLINK_SOURCE/.venv/bin/python`:

```yaml
python:
  executable: /absolute/path/to/community-flink/.venv/bin/python
```

```bash
$FLINK_HOME/bin/start-cluster.sh
```

## Run The Jobs

Jobs 01 and 02 run in batch mode. Jobs 03 through 06 run in streaming mode
with bounded inputs, so they finish without manual cancellation.

Change to this jobs directory, keeping the same terminal environment, then run
the first four jobs:

```bash
$FLINK_HOME/bin/flink run -py 01_relational.py
$FLINK_HOME/bin/flink run -py 02_python_functions.py
$FLINK_HOME/bin/flink run -py 03_pick_list.py
$FLINK_HOME/bin/flink run -py 04_windows_temporal.py
```

For the fifth job, supply your OpenAI-compatible service's full
`/chat/completions` endpoint, a model, and an API key:

```bash
read -r -p "Chat completions endpoint: " OPENAI_ENDPOINT
read -r -p "Model: " OPENAI_MODEL
read -r -s -p "API key: " OPENAI_API_KEY
printf '\n'
export OPENAI_ENDPOINT OPENAI_MODEL OPENAI_API_KEY
$FLINK_HOME/bin/flink run -py 05_openai.py
unset OPENAI_API_KEY
```

For the sixth job, start Kafka first:

```bash
bash kafka.sh start
$FLINK_HOME/bin/flink run -py 06_connectors.py
```

Compare each job's printed results and output files with the expected output in
the comment at the top of its file.

## Stop

```bash
bash kafka.sh stop
$FLINK_HOME/bin/stop-cluster.sh
```

Stopping Kafka deletes its local test topics. The jobs' printed temporary
directories remain available for inspection.
