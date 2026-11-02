#!/usr/bin/env bash
# Local integration-test broker, not a production deployment.
# Usage: bash kafka.sh start|stop|status|topics [topic-name]
# Optional: KAFKA_PORT, KAFKA_ADVERTISED_HOST, KAFKA_BIND_HOST, KAFKA_CONTAINER.
# Default publication is loopback only. Docker-hosted Flink needs a reachable
# advertised address (for example host.docker.internal), not localhost.
set -euo pipefail

IMAGE=apache/kafka:3.9.1
NAME=${KAFKA_CONTAINER:-pyflink-df-kafka}
PORT=${KAFKA_PORT:-9092}
HOST=${KAFKA_ADVERTISED_HOST:-localhost}
BIND=${KAFKA_BIND_HOST:-127.0.0.1}
OWNER=pyflink-df-tests

command -v docker >/dev/null || { printf '%s\n' "Docker is required." >&2; exit 1; }

owned_container() {
    local owner
    owner=$(docker inspect --format '{{index .Config.Labels "testing.owner"}}' "$NAME")
    if [[ "$owner" != "$OWNER" ]]; then
        printf 'Refusing to operate on unowned container: %s\n' "$NAME" >&2
        exit 1
    fi
}

ready() {
    docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:19092 --list >/dev/null 2>&1
}

case "${1:-}" in
    start)
        if docker container inspect "$NAME" >/dev/null 2>&1; then
            owned_container
            docker start "$NAME" >/dev/null
        else
            docker run --detach --name "$NAME" --label "testing.owner=$OWNER" \
                --publish "$BIND:$PORT:9092" \
                --env KAFKA_NODE_ID=1 \
                --env KAFKA_PROCESS_ROLES=broker,controller \
                --env 'KAFKA_LISTENERS=INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093' \
                --env "KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19092,EXTERNAL://$HOST:$PORT" \
                --env 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT' \
                --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
                --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
                --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
                --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
                --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
                --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
                --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
                --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
                --env KAFKA_NUM_PARTITIONS=1 \
                "$IMAGE" >/dev/null
        fi
        for ((attempt = 0; attempt < 60; attempt++)); do
            if ready; then
                printf 'Kafka ready. export KAFKA_BOOTSTRAP_SERVERS=%s:%s\n' "$HOST" "$PORT"
                exit 0
            fi
            sleep 2
        done
        docker logs --tail 80 "$NAME" >&2
        printf '%s\n' "Kafka did not become ready. Container retained for diagnosis." >&2
        exit 1
        ;;
    stop)
        if docker container inspect "$NAME" >/dev/null 2>&1; then
            owned_container
            docker rm --force "$NAME"
        else
            printf 'No container named %s\n' "$NAME"
        fi
        ;;
    status)
        owned_container
        docker inspect --format '{{.State.Status}}' "$NAME"
        ready
        ;;
    topics)
        owned_container
        if [[ -n "${2:-}" ]]; then
            docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server localhost:19092 --create --if-not-exists \
                --topic "$2" --partitions 1 --replication-factor 1
        else
            docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server localhost:19092 --list
        fi
        ;;
    *)
        printf 'Usage: bash %s start|stop|status|topics [topic-name]\n' "$0" >&2
        exit 2
        ;;
esac
