#!/usr/bin/env python3
"""
Food Ordering gRPC Server.

Usage:
    python3 server.py <host:port>
    python3 server.py localhost:50051
"""
import itertools
import queue
import sys
import threading
from concurrent import futures

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

# ------------------------------------------------------------------
# Static restaurant / menu data
# ------------------------------------------------------------------
RESTAURANTS = {
    "Pizza House": {
        "Margherita Pizza": 250,
        "Farmhouse Pizza": 350,
        "Garlic Bread": 150,
    },
    "Burger Point": {
        "Veg Burger": 180,
        "Cheese Burger": 220,
        "French Fries": 120,
    },
}

# Order status state machine
VALID_TRANSITIONS = {
    "PLACED": {"ACCEPTED", "CANCELLED"},
    "ACCEPTED": {"PREPARING"},
    "PREPARING": {"READY"},
    "READY": set(),
    "CANCELLED": set(),
}
TERMINAL_STATES = {"READY", "CANCELLED"}


class Order:
    def __init__(self, order_id, customer_id, restaurant_name, items, total):
        self.order_id = order_id
        self.customer_id = customer_id
        self.restaurant_name = restaurant_name
        self.items = items  # list of (item_name, quantity)
        self.total = total
        self.status = "PLACED"
        self.lock = threading.Lock()
        self.subscribers = []  # list of queue.Queue, one per active subscriber

    def to_status_response(self):
        return pb2.OrderStatusResponse(
            order_id=self.order_id,
            restaurant_name=self.restaurant_name,
            customer_id=self.customer_id,
            items=[pb2.OrderItemRequest(item_name=n, quantity=q) for n, q in self.items],
            total=self.total,
            status=self.status,
        )


class FoodOrderingServicer(pb2_grpc.FoodOrderingServiceServicer):
    def __init__(self):
        self._orders = {}
        self._order_counter = itertools.count(101)
        self._global_lock = threading.Lock()

    # ---------------- ListRestaurants ----------------
    def ListRestaurants(self, request, context):
        restaurants = []
        for name, menu in RESTAURANTS.items():
            items = [pb2.FoodItem(name=n, price=p) for n, p in menu.items()]
            restaurants.append(pb2.Restaurant(name=name, items=items))
        return pb2.RestaurantResponse(restaurants=restaurants)

    # ---------------- PlaceOrder ----------------
    def PlaceOrder(self, request, context):
        restaurant_name = request.restaurant_name
        if restaurant_name not in RESTAURANTS:
            context.abort(grpc.StatusCode.NOT_FOUND,
                           f"Restaurant '{restaurant_name}' does not exist.")

        menu = RESTAURANTS[restaurant_name]
        if len(request.items) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                           "Order must contain at least one item.")

        total = 0
        items = []
        for item in request.items:
            if item.item_name not in menu:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Item '{item.item_name}' is not available at '{restaurant_name}'.",
                )
            if item.quantity <= 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                               f"Invalid quantity for '{item.item_name}'.")
            total += menu[item.item_name] * item.quantity
            items.append((item.item_name, item.quantity))

        with self._global_lock:
            order_id = f"O{next(self._order_counter)}"
            order = Order(order_id, request.customer_id, restaurant_name, items, total)
            self._orders[order_id] = order

        return pb2.OrderResponse(order_id=order_id, total=total, status=order.status)

    # ---------------- GetOrderStatus ----------------
    def GetOrderStatus(self, request, context):
        order = self._get_order_or_abort(request.order_id, context)
        with order.lock:
            return order.to_status_response()

    # ---------------- CancelOrder ----------------
    def CancelOrder(self, request, context):
        order = self._get_order_or_abort(request.order_id, context)
        with order.lock:
            if order.status != "PLACED":
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"Order {order.order_id} cannot be cancelled: "
                    f"already {order.status}.",
                )
            order.status = "CANCELLED"
            self._notify_subscribers(order)
        return pb2.Acknowledge(success=True, message=f"Order {order.order_id} cancelled.")

    # ---------------- ViewPendingOrders ----------------
    def ViewPendingOrders(self, request, context):
        restaurant_name = request.restaurant_name
        if restaurant_name not in RESTAURANTS:
            context.abort(grpc.StatusCode.NOT_FOUND,
                           f"Restaurant '{restaurant_name}' does not exist.")

        pending = []
        with self._global_lock:
            candidates = [o for o in self._orders.values()
                          if o.restaurant_name == restaurant_name]
        for order in candidates:
            with order.lock:
                if order.status not in TERMINAL_STATES:
                    pending.append(order.to_status_response())
        return pb2.PendingOrdersResponse(orders=pending)

    # ---------------- UpdateOrderStatus ----------------
    def UpdateOrderStatus(self, request, context):
        order = self._get_order_or_abort(request.order_id, context)

        if request.restaurant_name != order.restaurant_name:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"'{request.restaurant_name}' may not update an order "
                f"belonging to '{order.restaurant_name}'.",
            )

        new_status = request.new_status
        with order.lock:
            allowed = VALID_TRANSITIONS.get(order.status, set())
            if new_status not in allowed:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"Invalid order state transition: {order.status} -> {new_status}.",
                )
            order.status = new_status
            self._notify_subscribers(order)

        return pb2.Acknowledge(success=True,
                                message=f"Order {order.order_id} -> {new_status}.")

    # ---------------- SubscribeToOrderUpdates (server streaming) ----------------
    def SubscribeToOrderUpdates(self, request, context):
        order = self._get_order_or_abort(request.order_id, context)

        q = queue.Queue()
        with order.lock:
            q.put(order.status)  # immediately report current status
            order.subscribers.append(q)

        try:
            while context.is_active():
                try:
                    status = q.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield pb2.OrderUpdate(order_id=order.order_id, status=status)
                if status in TERMINAL_STATES:
                    return
        finally:
            with order.lock:
                if q in order.subscribers:
                    order.subscribers.remove(q)

    # ---------------- helpers ----------------
    def _get_order_or_abort(self, order_id, context):
        with self._global_lock:
            order = self._orders.get(order_id)
        if order is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Order '{order_id}' does not exist.")
        return order

    @staticmethod
    def _notify_subscribers(order):
        # caller already holds order.lock
        for q in order.subscribers:
            q.put(order.status)


def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_FoodOrderingServiceServicer_to_server(FoodOrderingServicer(), server)
    server.add_insecure_port(address)
    server.start()
    print(f"[Server] Food Ordering gRPC server listening on {address}")
    server.wait_for_termination()


if __name__ == "__main__":
    addr = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    serve(addr)
