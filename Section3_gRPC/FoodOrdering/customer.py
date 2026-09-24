#!/usr/bin/env python3
"""
Food Ordering - Customer CLI Client.

Usage:
    python3 customer.py <server_host:port> [customer_id]
    python3 customer.py localhost:50051 cust1
"""
import sys
import threading
import uuid

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU = """
1. List Restaurants
2. Place Order
3. Check Order Status
4. Track Order (real-time updates)
5. Cancel Order
6. Exit
"""


def list_restaurants(stub):
    resp = stub.ListRestaurants(pb2.Empty())
    if not resp.restaurants:
        print("[Client] No restaurants available.")
        return
    for i, r in enumerate(resp.restaurants, 1):
        print(f"{i}. {r.name}")
        for item in r.items:
            print(f"   - {item.name} : {item.price}")


def place_order(stub, customer_id):
    restaurant_name = input("Restaurant name: ").strip()
    items = []
    print("Enter items one at a time. Leave item name blank to finish.")
    while True:
        item_name = input("  Item name: ").strip()
        if not item_name:
            break
        qty_str = input("  Quantity: ").strip()
        try:
            qty = int(qty_str)
        except ValueError:
            print("  Invalid quantity, skipping item.")
            continue
        items.append(pb2.OrderItemRequest(item_name=item_name, quantity=qty))

    if not items:
        print("[Client] No items entered, order not placed.")
        return

    try:
        resp = stub.PlaceOrder(pb2.OrderRequest(
            customer_id=customer_id,
            restaurant_name=restaurant_name,
            items=items,
        ))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return

    print("[Client] Order placed successfully.")
    print(f"[Client] Order ID: {resp.order_id}")
    print(f"[Client] Total: {resp.total}")
    print(f"[Client] Status: {resp.status}")


def check_order_status(stub, customer_id):
    order_id = input("Order ID: ").strip()
    try:
        resp = stub.GetOrderStatus(pb2.OrderIdRequest(order_id=order_id, requester_id=customer_id))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    print(f"[Client] Order {resp.order_id} ({resp.restaurant_name}) : {resp.status}")
    for item in resp.items:
        print(f"   - {item.item_name} x{item.quantity}")
    print(f"   Total: {resp.total}")


def track_order(stub, customer_id):
    order_id = input("Order ID to track: ").strip()

    def _stream():
        try:
            for update in stub.SubscribeToOrderUpdates(
                pb2.OrderIdRequest(order_id=order_id, requester_id=customer_id)
            ):
                print(f"\n[Update] Order {update.order_id} : {update.status}\n> ", end="", flush=True)
        except grpc.RpcError as e:
            print(f"\n[Error] tracking {order_id}: {e.code().name}: {e.details()}\n> ", end="", flush=True)

    print(f"[Client] Tracking order {order_id} in the background...")
    t = threading.Thread(target=_stream, daemon=True)
    t.start()


def cancel_order(stub, customer_id):
    order_id = input("Order ID to cancel: ").strip()
    try:
        resp = stub.CancelOrder(pb2.OrderIdRequest(order_id=order_id, requester_id=customer_id))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    print(f"[Client] {resp.message}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 customer.py <server_host:port> [customer_id]")
        sys.exit(1)

    server_addr = sys.argv[1]
    customer_id = sys.argv[2] if len(sys.argv) > 2 else f"cust-{uuid.uuid4().hex[:6]}"

    channel = grpc.insecure_channel(server_addr)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)

    print(f"[Client] Connected to {server_addr} as customer '{customer_id}'")

    actions = {
        "1": lambda: list_restaurants(stub),
        "2": lambda: place_order(stub, customer_id),
        "3": lambda: check_order_status(stub, customer_id),
        "4": lambda: track_order(stub, customer_id),
        "5": lambda: cancel_order(stub, customer_id),
    }

    while True:
        print(MENU)
        choice = input("> ").strip()
        if choice == "6":
            break
        action = actions.get(choice)
        if action is None:
            print("Invalid choice.")
            continue
        action()


if __name__ == "__main__":
    main()
