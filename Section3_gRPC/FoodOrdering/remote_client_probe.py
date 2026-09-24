#!/usr/bin/env python3
"""
Non-interactive probe used to validate real cross-node gRPC communication
on the RCE cluster (server on one node, this probe run on another via
srun, connecting over the network rather than localhost).
"""
import sys

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

addr = sys.argv[1]
print(f"[Probe] Connecting to {addr} ...")
channel = grpc.insecure_channel(addr)
stub = pb2_grpc.FoodOrderingServiceStub(channel)

r = stub.ListRestaurants(pb2.Empty())
print("[Probe] Restaurants:", [x.name for x in r.restaurants])

o = stub.PlaceOrder(pb2.OrderRequest(
    customer_id="rce_demo",
    restaurant_name="Pizza House",
    items=[pb2.OrderItemRequest(item_name="Margherita Pizza", quantity=1)],
))
print(f"[Probe] Order placed: {o.order_id}, total={o.total}, status={o.status}")

ack = stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
    order_id=o.order_id, restaurant_name="Pizza House", new_status="ACCEPTED",
))
print(f"[Probe] UpdateOrderStatus ack: success={ack.success} message={ack.message}")

st = stub.GetOrderStatus(pb2.OrderIdRequest(order_id=o.order_id, requester_id="rce_demo"))
print(f"[Probe] Confirmed status via GetOrderStatus: {st.status}")

print("[Probe] Cross-node gRPC communication verified OK.")
