# Section 3 - Problem 2: Food Ordering System using gRPC

Team 32 - Nityanand Gupta (2024101147), A V Aditya (2024111031)

## Architecture

A single central `FoodOrderingServicer` (in-memory state, thread-safe)
serves two kinds of clients over one gRPC service:

- `customer.py` - browse restaurants, place orders, check/track status, cancel.
- `restaurant.py` - view pending orders, advance an order's status.

State machine enforced server-side:

```
PLACED -> ACCEPTED -> PREPARING -> READY
PLACED -> CANCELLED
```

Every other transition is rejected with `FAILED_PRECONDITION`. Each
`Order` has its own `threading.Lock` (fine-grained locking instead of one
global lock around every request) plus a list of subscriber queues for
streaming; the servicer keeps a single global lock only around the
shared `orders` dict / id counter.

`SubscribeToOrderUpdates` is a server-streaming RPC: the customer opens
one call per order, immediately receives the current status, then
receives a new `OrderUpdate` every time `UpdateOrderStatus`/`CancelOrder`
changes it. The stream closes once the order reaches a terminal state
(`READY` or `CANCELLED`).

## Files

| File | Purpose |
|---|---|
| `food_ordering.proto` | Service + message definitions |
| `food_ordering_pb2.py`, `food_ordering_pb2_grpc.py` | Generated stubs (regenerate with the command below if the proto changes) |
| `server.py` | Servicer implementation, menu data, concurrency + exception handling |
| `customer.py` | Customer CLI |
| `restaurant.py` | Restaurant CLI |
| `test_food_ordering.py` | Automated correctness/exception/concurrency test (no manual interaction needed) |

Regenerating stubs after editing the `.proto`:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto
```

## Setup

```bash
pip install --user grpcio grpcio-tools
```

Already verified installed under this account both locally and on the
RCE cluster (Python 3.12.5, grpcio 1.84.0).

## Running locally

Terminal 1 (server):
```bash
python3 server.py localhost:50051
```

Terminal 2 (customer):
```bash
python3 customer.py localhost:50051 cust1
```

Terminal 3 (restaurant):
```bash
python3 restaurant.py localhost:50051 "Pizza House"
```

## Automated correctness test

```bash
python3 test_food_ordering.py
```

Starts the server itself, drives it purely through the stub, and checks:
restaurant listing, order placement + total calculation, streamed
real-time updates arriving in the correct order, all valid state
transitions, the required exceptional cases (non-existent restaurant,
unavailable item, non-existent order, invalid transition, cross-restaurant
update attempt, cancelling an already-processed order), and 20 concurrent
`PlaceOrder` calls all getting unique order IDs.

All 18 checks pass:
```
18 passed, 0 failed
```

## Demonstration walkthrough (matches assignment's required demo)

1. `python3 customer.py localhost:50051 cust1` -> option 1 (List Restaurants).
2. Option 2 (Place Order) -> restaurant "Pizza House", items "Margherita Pizza" x1, "Garlic Bread" x2 -> total 550, status PLACED.
3. `python3 restaurant.py localhost:50051 "Pizza House"` -> option 1 (View Pending Orders) shows the new order.
4. In the customer client, option 4 (Track Order) with that order ID -> background thread starts printing updates.
5. In the restaurant client: option 2 (Accept), option 3 (Start Preparing), option 4 (Mark Ready) -> the customer's tracking thread prints each `[Update] Order O101 : ...` live, without polling.
6. Run two customer clients placing orders concurrently, and two restaurant clients (or the same one) processing them, to show simultaneous requests are handled safely (also covered automatically by `test_food_ordering.py`'s 20-thread test).
7. Exceptional cases: try `restaurant.py "Burger Point"` calling option 3 (Start Preparing) on an order that belongs to "Pizza House" -> `PERMISSION_DENIED`; try `restaurant.py "Pizza House"` calling option 4 (Mark Ready) before "Accept"/"Start Preparing" -> `FAILED_PRECONDITION`.

## Running on the RCE cluster

Verified on `rce.iiit.ac.in` under account `cs3401.32`:

- Login node Python is 3.6.8 (too old); use `module load python/3.12.5`.
- The `gRPC` module (`module load gRPC/1.74.1`) is the **C++** gRPC
  library, not Python bindings - it does not provide the `grpc` Python
  package.
- `grpcio`/`grpcio-tools` install fine for the user via pip once
  `python/3.12.5` is loaded (confirmed `grpcio 1.84.0`), but **modules
  can only be loaded on compute nodes**, not the login node - run the
  install inside an `srun`/`salloc` allocation:

```bash
srun --nodes=1 --ntasks=1 --time=00:10:00 \
  bash -c "module load python/3.12.5; python3 -m pip install --user grpcio grpcio-tools"
```

Then follow `rce_grpc_execution_guide.pdf`: allocate 3 nodes, run
`server.py` on node01, `customer.py`/`restaurant.py` on node02/node03,
connecting to `node01:<port>` (not `localhost`). Remember to
`module load python/3.12.5` in every session/`srun` command before
running the Python scripts, since the default login-node Python (3.6)
doesn't have `grpcio` installed.

```bash
salloc --nodes=3 --ntasks-per-node=1
scontrol show hostnames $SLURM_JOB_NODELIST

# on node01
module load python/3.12.5
python3 server.py 0.0.0.0:50051

# on node02
module load python/3.12.5
python3 customer.py node01:50051 cust1

# on node03
module load python/3.12.5
python3 restaurant.py node01:50051 "Pizza House"
```

### Verified on the actual RCE cluster

Two things were actually executed on `rce.iiit.ac.in` (account `cs3401.32`),
not just tested locally:

1. **`test_food_ordering.py`** (all 18 automated checks) run inside a
   `salloc` allocation with `module load python/3.12.5` - all passed on
   a real compute node (`node06`).
2. **True cross-node communication**: `rce_multinode_demo.sh` requests 2
   nodes, starts `server.py` on the first (`node06`) via `srun`, and runs
   `remote_client_probe.py` on the *second* node (`node07`), connecting
   over the cluster network to `node06:<port>` (not `localhost`). Output:
   ```
   Server node: node06
   Client node: node07
   [Probe] Connecting to node06:50151 ...
   [Probe] Restaurants: ['Pizza House', 'Burger Point']
   [Probe] Order placed: O101, total=250, status=PLACED
   [Probe] UpdateOrderStatus ack: success=True message=Order O101 -> ACCEPTED.
   [Probe] Confirmed status via GetOrderStatus: ACCEPTED
   [Probe] Cross-node gRPC communication verified OK.
   ```
   Run it yourself with:
   ```bash
   cd ~/HW3/Section3_gRPC/FoodOrdering
   salloc --nodes=2 --ntasks-per-node=1 --time=00:06:00 bash rce_multinode_demo.sh
   ```
