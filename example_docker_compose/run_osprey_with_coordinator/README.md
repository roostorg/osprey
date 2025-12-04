While Osprey worker can stand on its own by directly ingesting data from Kafka, Osprey Coordinator provides an alternative that provides additional features such as load balancing and synchronous actions.

## Quick Start

The easiest way to run Osprey with the Coordinator is using the helper script from the repository root:

```bash
# Start with coordinator
./start.sh --with-coordinator

# Start in detached mode
./start.sh --with-coordinator up -d

# Start with test data producer
./start.sh --with-coordinator --profile coordinator_test_data up
```

Or manually using docker compose override files:

```bash
# From the repository root
docker compose -f docker-compose.yaml -f example_docker_compose/run_osprey_with_coordinator/docker-compose.coordinator.yaml up
```

## Overview

The **Osprey Coordinator** is a Rust-based service that acts as a central hub for distributing actions to Osprey Workers for rule evaluation. It provides two primary modes for receiving actions:

1. **Bidirectional gRPC Streaming** - Workers connect to the coordinator via persistent bidirectional streams
2. **Synchronous gRPC API** - External services send actions directly for immediate processing

The coordinator can consume actions from Kafka, Pubsub and/or receives them via gRPC, manages action distribution across connected workers, handles acknowledgments, and ensures reliable action processing.

## Architecture

### Components

```
┌───────────────────────────────┐
│   Kafka Topics and/or Pubsub  │
│  (actions_input)              │ 
└──────────┬────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────┐
│        Osprey Coordinator (Rust)            │
│  ┌─────────────────────────────────────┐    │
│  │  Priority Queue                     │    │
│  │  - Sync Actions (high priority)     │    │
│  │  - Async Actions (lower priority)   │    │
│  └─────────────────────────────────────┘    │
│                                             │
│  gRPC Services:                             │
│  - Bidirectional Stream (port 19950)        │
│  - Sync Action API (port 19951)             │
└──────────────┬──────────────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │  Osprey Workers      │
    │  (Python)            │
    │  - Process rules     │
    │  - Send verdicts     │
    └──────────────────────┘
```

## Configuration

The coordinator is configured via the `docker-compose.coordinator.yaml` override file in this directory (`example_docker_compose/run_osprey_with_coordinator/`). This file adds the coordinator service and modifies the worker configuration to connect to it.

### Environment Variables

Configure the coordinator via environment variables in `docker-compose.yaml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `OSPREY_COORDINATOR_BIDI_STREAM_PORT` | `19950` | Port for bidirectional streaming |
| `OSPREY_COORDINATOR_SYNC_ACTION_PORT` | `19951` | Port for synchronous action API |
| `SNOWFLAKE_API_ENDPOINT` | `http://snowflake-id-worker:8088` | Snowflake ID service endpoint |
| `ETCD_PEERS` | `http://etcd:2379` | etcd connection string |
| `OSPREY_KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `OSPREY_KAFKA_INPUT_STREAM_TOPIC` | `osprey.actions_input` | Kafka topic to consume |
| `OSPREY_KAFKA_GROUP_ID` | `osprey_coordinator_group` | Kafka consumer group ID |
| `OSPREY_COORDINATOR_CONSUMER_TYPE` | `kafka` | Consumer type: `kafka` or `pubsub` |
| `MAX_TIME_TO_SEND_TO_ASYNC_QUEUE_MS` | `500` | Max time to wait before queuing async actions |
| `MAX_ACKING_RECEIVER_WAIT_TIME_MS` | `60000` | Max time to wait for worker ack/nack |

### Example Configuration

To customize the coordinator, edit `example_docker_compose/run_osprey_with_coordinator/docker-compose.coordinator.yaml`:

```yaml
services:
  osprey-coordinator:
    environment:
      - RUST_LOG=info
      - ETCD_PEERS=http://etcd:2379
      - SNOWFLAKE_API_ENDPOINT=http://snowflake-id-worker:8088
      - OSPREY_COORDINATOR_CONSUMER_TYPE=kafka  # or 'pubsub'
      - OSPREY_COORDINATOR_BIDI_STREAM_PORT=19950
      - OSPREY_COORDINATOR_SYNC_ACTION_PORT=19951
```

## Using the Coordinator

**Worker Configuration:**

### Worker Connection

When using `docker-compose.coordinator.yaml`, workers are automatically configured to connect to the coordinator. The override file sets:

```yaml
osprey-worker:
  environment:
    - OSPREY_INPUT_STREAM_SOURCE=osprey_coordinator
    - OSPREY_COORDINATOR_SERVICE_NAME=osprey_coordinator
```

**How It Works:**

1. Worker connects to coordinator on port 19950
2. Worker sends initial connection request with client ID
3. Coordinator sends actions to worker via the bidirectional stream
4. Worker processes actions through rules
5. Worker sends ack/nack with optional verdicts back to coordinator
6. Connection automatically reconnects every 60-120 seconds for load balancing


### Direct Action Submission (Sync API)

External services can submit actions directly to the coordinator for synchronous processing.

**Using grpcurl:**

```bash
# Send a single action for immediate processing
grpcurl -plaintext \
  -d '{
    "action_id": 12345,
    "action_name": "user_login",
    "action_data_json": "{\"user_id\":\"user_123\",\"ip_address\":\"192.168.1.1\"}",
    "timestamp": "2024-11-25T10:30:00.000000000Z"
  }' \
  localhost:19951 \
  osprey.rpc.osprey_coordinator.sync_action.v1.OspreyCoordinatorSyncActionService/ProcessAction
```

or Use the test data producer:

```bash
./start.sh --with-coordinator --profile coordinator_test_data up
```

### Kafka/Pubsub Integration
The coordinator can automatically consume from either Kafka or PubSub (but not both simultaneously). Set `OSPREY_COORDINATOR_CONSUMER_TYPE` to choose:

- `kafka` (default) - Consume from Kafka
- `pubsub` - Consume from Google Cloud PubSub

Configure the appropriate environment variables for your chosen consumer in `example_docker_compose/run_osprey_with_coordinator/docker-compose.coordinator.yaml`:

```yaml
  osprey-coordinator:
    environment:
    # Consumer selection (kafka or pubsub)
    - OSPREY_COORDINATOR_CONSUMER_TYPE=kafka
    # Kafka configuration (when using kafka)
    - OSPREY_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    - OSPREY_KAFKA_INPUT_STREAM_TOPIC=osprey.actions_input
    - OSPREY_KAFKA_GROUP_ID=osprey_coordinator_group

    # Pubsub
    - OSPREY_COORDINATOR_SERVICE_ACCOUNT
    - PUBSUB_SUBSCRIPTION_PROJECT_ID
    - PUBSUB_SUBSCRIPTION_ID
    - PUBSUB_ENCRYPTION_KEY_URI
    # Optionally
    - PUBSUB_MAX_MESSAGES = 5000 # default
    - PUBSUB_MAX_PROCESSING_MESSAGES = 5000 # default

    # shared by both Kafka and Pubsub, optional
    - MAX_TIME_TO_SEND_TO_ASYNC_QUEUE_MS = 500 # default
    - MAX_ACKING_RECEIVER_WAIT_TIME_MS = 6000 # default
```


**Sending Actions via Kafka:**

Use the test data producer:

```bash
# Start the Kafka test data producer
./start.sh --with-coordinator --profile test_data up kafka-test-data-producer -d
```

