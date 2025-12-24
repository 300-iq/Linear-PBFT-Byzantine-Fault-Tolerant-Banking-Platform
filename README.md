# PBFT

This is a multimodule Java + Gradle project that implements PBFT using a cluster of 7 replicas and 10 clients.

Modules:
- modules/common – shared utilities, protobuf/gRPC bindings, key tools
- modules/replica – PBFT replica server
- modules/client – client CLI/driver

## Prerequisites
- Java 21


## Quick start guide

1) Generate keys (one-time)

- Generate all replica keys n1..n7 into `secrets/replicas/*`:
```
./gradlew :modules:common:keygenAll
```
- Generate all client keys A..J into `secrets/clients/*`:
```
./gradlew :modules:common:keygenClientsAll
```

2) Build everything
```
./gradlew clean build
```

3) Start replicas (one terminal per node)

Use ports and ids from `configs/cluster.json`. Example (7 nodes):
```
# Terminal 1
./gradlew :modules:replica:run --args="--id n1 --port 50051 --config configs/cluster.json --keys-dir secrets/replicas/n1"
# Terminal 2
./gradlew :modules:replica:run --args="--id n2 --port 50052 --config configs/cluster.json --keys-dir secrets/replicas/n2"
# Terminal 3
./gradlew :modules:replica:run --args="--id n3 --port 50053 --config configs/cluster.json --keys-dir secrets/replicas/n3"
# Terminal 4
./gradlew :modules:replica:run --args="--id n4 --port 50054 --config configs/cluster.json --keys-dir secrets/replicas/n4"
# Terminal 5
./gradlew :modules:replica:run --args="--id n5 --port 50055 --config configs/cluster.json --keys-dir secrets/replicas/n5"
# Terminal 6
./gradlew :modules:replica:run --args="--id n6 --port 50056 --config configs/cluster.json --keys-dir secrets/replicas/n6"
# Terminal 7
./gradlew :modules:replica:run --args="--id n7 --port 50057 --config configs/cluster.json --keys-dir secrets/replicas/n7"
```

4) Run the client in CSV mode
```
./gradlew :modules:client:run --args="--config configs/cluster.json --keys-dir secrets/clients --csv <path-to-tests.csv> --cmd run"
```
- On CLI:
  - `run` – start the current set.
  - `skip` – skip current set.
  - `goto <set>` – move to a specific set number.
  - `db [node]` – print DB table for all or a specific node (e.g., `db n1` or `db 1`).
  - `log [node]` – print phase log for all or one node.
  - `completelog [raw|phases] [node]` – prints the complete event log. `raw` shows the full event stream; `phases` shows a status (PP/P/C/E) summary.
  - `checkpoint [node]` – checkpoint state and window entries.
  - `view [node]` – view-change details for given test cases.
  - `status <seq> [view]` – per-seq status across nodes.
  - `help`, `quit` – help for commands, exit the CLI.

Notes:
- The client also has a minimal gRPC server to collect replies on the port from `clients[0]` in `cluster.json` (default 60051).


## Benchmark mode

The client has a benchmark with optional SmallBank workload. Benchmark commands require enabling a flag:

- To run a single shot benchmark
```
./gradlew :modules:client:run --args="--config configs/cluster.json --keys-dir secrets/clients --cmd bench --enable-bench"
```

- To run an interactive benchmark cli
```
./gradlew :modules:client:run --args="--config configs/cluster.json --keys-dir secrets/clients --cmd benchshell --enable-bench"
```
Inside `benchshell`, type `help` for commands (`benchmark`, `wait`, `info`, `ops`, `inprogress`, `db`, `log`, `checkpoint`, `view`, `quit`).


## Major important system level configurable properties



Replica (server):
- `-Dpbft.timers.base_ms=<ms>` – base timeout for replica timers
- `-Dpbft.db.csv_path=<path>` – override DB CSV path (default Logs/db/<nodeId>.csv)

Client (driver/CLI):
- `-Dpbft.client.base_ms=<ms>` – client base timeout
- `-Dpbft.client.backoff_factor=<x>` – client backoff factor
- `-Dpbft.client.max_attempts=<n>` – client max retransmit attempts

Benchmark:
- `-Dpbft.benchmark.enabled` – enable benchmark mode
- `-Dpbft.benchmark.smallbank.enabled` – enable SmallBank workload
- `-Dpbft.benchmark.smallbank.dc_amount`, `ts_amount`, `wc_amount`, `sp_amount` – operation amounts



## Configuration
- Cluster layout and ports: `configs/cluster.json`
  - Replica ids/hosts/ports and public keys for n1..n7
  - Client ids/hosts/ports and public keys for A..J
- Secrets/keys live under `secrets/replicas/<id>` and `secrets/clients/<id>` (generated via tasks above)

## Acknowledgements
I took help from ChatGPT for:
- Initial project scaffolding and protobuf/gRPC definitions
- Logback logger - to understand and implement logging
- Cryptos, keyGen, signatures etc as I was unfamiliar with these crypto tools.
- CSV parsing and CSV persistence on client side
- Small bank Benchmarking - To understand the benchmarking process and the workload.
- Client CLI
- Debugging and fixes issues in new view/view change, checkpointing.

## References
- M. Castro and B. Liskov, “Practical Byzantine Fault Tolerance.” OSDI 1999. https://pmg.csail.mit.edu/papers/osdi99.pdf

