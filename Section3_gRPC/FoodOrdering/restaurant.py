#!/usr/bin/env python3
"""
Food Ordering - Restaurant CLI Client.

Usage:
    python3 restaurant.py <server_host:port> "<restaurant name>"
    python3 restaurant.py localhost:50051 "Pizza House"
"""
import sys

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU = """
1. View Pending Orders
2. Accept Order
3. Start Preparing
4. Mark Ready
5. Exit
"""


def view_pending_orders(stub, restaurant_name):
    try:
        resp = stub.ViewPendingOrders(pb2.RestaurantNameRequest(restaurant_name=restaurant_name))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    if not resp.orders:
        print("[Restaurant] No pending orders.")
        return
    for order in resp.orders:
        items_str = ", ".join(f"{it.item_name} x{it.quantity}" for it in order.items)
        print(f"Order {order.order_id} : {order.status}  [{items_str}]  Total: {order.total}")


def _update_status(stub, restaurant_name, new_status):
    order_id = input("Order ID: ").strip()
    try:
        resp = stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
            order_id=order_id,
            restaurant_name=restaurant_name,
            new_status=new_status,
        ))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    print(f"[Restaurant] {resp.message}")


def main():
    if len(sys.argv) < 3:
        print('Usage: python3 restaurant.py <server_host:port> "<restaurant name>"')
        sys.exit(1)

    server_addr = sys.argv[1]
    restaurant_name = sys.argv[2]

    channel = grpc.insecure_channel(server_addr)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)

    print(f"[Restaurant] Connected to {server_addr} as '{restaurant_name}'")

    actions = {
        "1": lambda: view_pending_orders(stub, restaurant_name),
        "2": lambda: _update_status(stub, restaurant_name, "ACCEPTED"),
        "3": lambda: _update_status(stub, restaurant_name, "PREPARING"),
        "4": lambda: _update_status(stub, restaurant_name, "READY"),
    }

    while True:
        print(MENU)
        choice = input("> ").strip()
        if choice == "5":
            break
        action = actions.get(choice)
        if action is None:
            print("Invalid choice.")
            continue
        action()


if __name__ == "__main__":
    main()
