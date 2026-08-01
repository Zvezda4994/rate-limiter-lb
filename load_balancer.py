"""
A reverse-proxy load balancer built on raw TCP sockets.

Features:
  - Round-robin and least-connections routing
  - Active health checks (background thread probes each backend)
  - Automatic failover: if a chosen backend refuses/drops mid-request,
    the request is retried on the next healthy backend before giving up
  - Per-client-IP rate limiting (token bucket), independent of routing
"""

import socket
import threading
import time
import itertools
import argparse


class Backend:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.healthy = True
        self.active_connections = 0
        self.lock = threading.Lock()

    @property
    def address(self):
        return (self.host, self.port)

    def __repr__(self):
        return f"{self.host}:{self.port}"


class LoadBalancer:
    def __init__(self, backends, algorithm="round_robin",
                 rate_limit_capacity=20, rate_limit_refill=10):
        from rate_limiter import RateLimiterRegistry

        self.backends = backends
        self.algorithm = algorithm
        self._rr_cycle = itertools.cycle(backends)
        self._rr_lock = threading.Lock()
        self.rate_limiter = RateLimiterRegistry(
            algorithm="token_bucket",
            capacity=rate_limit_capacity,
            refill_rate=rate_limit_refill,
        )
        self.stats_lock = threading.Lock()
        self.stats = {"forwarded": 0, "rate_limited": 0, "bad_gateway": 0}

    def healthy_backends(self):
        return [b for b in self.backends if b.healthy]

    def pick_backend(self, exclude=()):
        healthy = [b for b in self.healthy_backends() if b not in exclude]
        if not healthy:
            return None
        if self.algorithm == "least_connections":
            return min(healthy, key=lambda b: b.active_connections)
        # round robin, skipping unhealthy/excluded backends
        with self._rr_lock:
            for _ in range(len(self.backends)):
                b = next(self._rr_cycle)
                if b.healthy and b not in exclude:
                    return b
        return None

    def health_check_loop(self, interval=0.5):
        while True:
            for b in self.backends:
                ok = self._probe(b)
                if ok != b.healthy:
                    print(f"[lb] backend {b} -> {'UP' if ok else 'DOWN'}", flush=True)
                b.healthy = ok
            time.sleep(interval)

    def _probe(self, backend, timeout=0.4):
        try:
            with socket.create_connection(backend.address, timeout=timeout):
                return True
        except OSError:
            return False

    def _bump(self, key):
        with self.stats_lock:
            self.stats[key] += 1

    def forward(self, client_conn, client_addr, raw_request):
        client_key = client_addr[0]
        if not self.rate_limiter.allow(client_key):
            client_conn.sendall(
                b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            self._bump("rate_limited")
            return

        tried = set()
        for _ in range(len(self.backends)):
            backend = self.pick_backend(exclude=tried)
            if backend is None:
                break
            tried.add(backend)
            with backend.lock:
                backend.active_connections += 1
            try:
                with socket.create_connection(backend.address, timeout=2) as be_conn:
                    be_conn.sendall(raw_request)
                    be_conn.settimeout(2)
                    response = b""
                    while True:
                        chunk = be_conn.recv(4096)
                        if not chunk:
                            break
                        response += chunk
                if not response:
                    raise OSError("empty response (backend dropped connection)")
                client_conn.sendall(response)
                self._bump("forwarded")
                return
            except OSError:
                backend.healthy = False
                print(f"[lb] backend {backend} failed mid-request, failing over", flush=True)
                continue
            finally:
                with backend.lock:
                    backend.active_connections -= 1

        client_conn.sendall(
            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        self._bump("bad_gateway")

    def handle_client(self, conn, addr):
        try:
            conn.settimeout(3)
            data = conn.recv(8192)
            if not data:
                return
            self.forward(conn, addr, data)
        except OSError:
            pass
        finally:
            conn.close()

    def serve(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(256)
        print(f"[lb] listening on {host}:{port}, algorithm={self.algorithm}", flush=True)

        threading.Thread(target=self.health_check_loop, daemon=True).start()

        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()


def parse_backends(spec):
    backends = []
    for item in spec.split(","):
        host, port = item.split(":")
        backends.append(Backend(host, int(port)))
    return backends


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--backends", type=str, required=True, help="host:port,host:port,...")
    parser.add_argument("--algorithm", choices=["round_robin", "least_connections"], default="round_robin")
    parser.add_argument("--rate-capacity", type=int, default=20)
    parser.add_argument("--rate-refill", type=float, default=10)
    args = parser.parse_args()

    backends = parse_backends(args.backends)
    lb = LoadBalancer(
        backends,
        algorithm=args.algorithm,
        rate_limit_capacity=args.rate_capacity,
        rate_limit_refill=args.rate_refill,
    )
    lb.serve("127.0.0.1", args.port)


if __name__ == "__main__":
    main()
