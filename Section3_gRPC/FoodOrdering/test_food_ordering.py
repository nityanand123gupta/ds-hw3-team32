#!/usr/bin/env python3
"""
Automated correctness/exception/concurrency test for the Food Ordering
gRPC service. Starts the server as a subprocess, drives it purely via
the generated stub (no manual CLI interaction needed), and asserts the
required behaviors from the assignment:

  - restaurant listing
  - order placement + status tracking
  - real-time streaming updates (SubscribeToOrderUpdates)
  - valid/invalid state transitions
  - ownership checks (restaurant updating another restaurant's order)
  - cancellation rules
  - concurrent order placement safety
  - gRPC status codes for exceptional cases

Usage:
    python3 test_food_ordering.py
"""
import queue
import subprocess
import sys
import threading
import time

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

ADDRESS = "localhost:50151"
PASS = 0
FAIL = 0


def check(name, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}")


def expect_rpc_error(name, code, fn):
    try:
        fn()
        check(name, False)
    except grpc.RpcError as e:
        check(f"{name} (got {e.code().name})", e.code() == code)


def main():
    server_proc = subprocess.Popen(
        [sys.executable, "server.py", ADDRESS],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    time.sleep(1.5)  # let the server bind

    try:
        channel = grpc.insecure_channel(ADDRESS)
        stub = pb2_grpc.FoodOrderingServiceStub(channel)

        print("== Restaurant listing ==")
        resp = stub.ListRestaurants(pb2.Empty())
        check("at least 2 restaurants returned", len(resp.restaurants) >= 2)
        check("Pizza House present", any(r.name == "Pizza House" for r in resp.restaurants))

        print("== Order placement ==")
        order_resp = stub.PlaceOrder(pb2.OrderRequest(
            customer_id="cust1",
            restaurant_name="Pizza House",
            items=[
                pb2.OrderItemRequest(item_name="Margherita Pizza", quantity=1),
                pb2.OrderItemRequest(item_name="Garlic Bread", quantity=2),
            ],
        ))
        check("order total correct (250 + 2*150 = 550)", order_resp.total == 550)
        check("initial status PLACED", order_resp.status == "PLACED")
        order_id = order_resp.order_id

        print("== Restaurant processes order + real-time streaming ==")
        updates = queue.Queue()

        def stream_updates():
            for u in stub.SubscribeToOrderUpdates(
                pb2.OrderIdRequest(order_id=order_id, requester_id="cust1")
            ):
                updates.put(u.status)

        t = threading.Thread(target=stream_updates, daemon=True)
        t.start()
        time.sleep(0.3)

        for status in ("ACCEPTED", "PREPARING", "READY"):
            ack = stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
                order_id=order_id, restaurant_name="Pizza House", new_status=status,
            ))
            check(f"UpdateOrderStatus -> {status} succeeds", ack.success)
            time.sleep(0.2)

        t.join(timeout=3)
        received = []
        while not updates.empty():
            received.append(updates.get())
        check("customer received streamed updates in order",
              received == ["PLACED", "ACCEPTED", "PREPARING", "READY"])

        final = stub.GetOrderStatus(pb2.OrderIdRequest(order_id=order_id, requester_id="cust1"))
        check("final status is READY", final.status == "READY")

        print("== Exception handling ==")
        expect_rpc_error(
            "order from non-existent restaurant", grpc.StatusCode.NOT_FOUND,
            lambda: stub.PlaceOrder(pb2.OrderRequest(
                customer_id="cust1", restaurant_name="Nonexistent Place",
                items=[pb2.OrderItemRequest(item_name="X", quantity=1)],
            )),
        )
        expect_rpc_error(
            "order for unavailable item", grpc.StatusCode.NOT_FOUND,
            lambda: stub.PlaceOrder(pb2.OrderRequest(
                customer_id="cust1", restaurant_name="Pizza House",
                items=[pb2.OrderItemRequest(item_name="Sushi", quantity=1)],
            )),
        )
        expect_rpc_error(
            "query non-existent order", grpc.StatusCode.NOT_FOUND,
            lambda: stub.GetOrderStatus(pb2.OrderIdRequest(order_id="O_MISSING", requester_id="cust1")),
        )
        expect_rpc_error(
            "invalid transition READY -> PREPARING", grpc.StatusCode.FAILED_PRECONDITION,
            lambda: stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
                order_id=order_id, restaurant_name="Pizza House", new_status="PREPARING",
            )),
        )
        expect_rpc_error(
            "restaurant updates another restaurant's order", grpc.StatusCode.PERMISSION_DENIED,
            lambda: stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
                order_id=order_id, restaurant_name="Burger Point", new_status="ACCEPTED",
            )),
        )

        print("== Cancellation rules ==")
        order2 = stub.PlaceOrder(pb2.OrderRequest(
            customer_id="cust2", restaurant_name="Burger Point",
            items=[pb2.OrderItemRequest(item_name="Veg Burger", quantity=1)],
        ))
        cancel_ack = stub.CancelOrder(pb2.OrderIdRequest(order_id=order2.order_id, requester_id="cust2"))
        check("cancel a freshly PLACED order succeeds", cancel_ack.success)
        expect_rpc_error(
            "cancel an already-cancelled order fails", grpc.StatusCode.FAILED_PRECONDITION,
            lambda: stub.CancelOrder(pb2.OrderIdRequest(order_id=order2.order_id, requester_id="cust2")),
        )
        expect_rpc_error(
            "cancel an accepted order fails", grpc.StatusCode.FAILED_PRECONDITION,
            lambda: stub.CancelOrder(pb2.OrderIdRequest(order_id=order_id, requester_id="cust1")),
        )

        print("== Concurrency: simultaneous order placement ==")
        results = []
        results_lock = threading.Lock()

        def place_concurrent(i):
            r = stub.PlaceOrder(pb2.OrderRequest(
                customer_id=f"concurrent-{i}",
                restaurant_name="Burger Point",
                items=[pb2.OrderItemRequest(item_name="French Fries", quantity=1)],
            ))
            with results_lock:
                results.append(r.order_id)

        threads = [threading.Thread(target=place_concurrent, args=(i,)) for i in range(20)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        check("20 concurrent orders all got unique IDs", len(set(results)) == 20)

        print(f"\n{PASS} passed, {FAIL} failed")
        sys.exit(1 if FAIL else 0)

    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()


if __name__ == "__main__":
    main()
