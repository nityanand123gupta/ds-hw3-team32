#!/usr/bin/env python3
"""
Non-interactive single-shot version of dashboard.py, used to validate
cross-node querying in automated/RCE demos (dashboard.py itself loops
forever for interactive use).
"""
import sys

import grpc

import log_analytics_pb2 as pb2
import log_analytics_pb2_grpc as pb2_grpc
from dashboard import render

addr = sys.argv[1]
channel = grpc.insecure_channel(addr)
stub = pb2_grpc.LogAnalyticsServiceStub(channel)
snap = stub.GetAnalytics(pb2.Empty())
render(snap)
print(f"[Probe] Queried {addr} from a different node than the server -- cross-node gRPC OK.")
