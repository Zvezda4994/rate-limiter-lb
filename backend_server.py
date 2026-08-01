"""
A minimal HTTP server built directly on raw TCP sockets (no http.server,
no frameworks). Simulates a real backend doing a bit of work per request,
and can optionally be "flaky" (randomly drop requests) to exercise the
load balancer's failover logic.
"""

import socket
import random
import argparse
import threading
import time


def handle_client(conn, delay_range, flaky):
    try:
        conn.settimeout(5)
        data = conn.recv(4096)
        if not data:
            return

        if flaky and random.random() < flaky:
            # Simulate a broken backend: drop the connection with no response.
            conn.close()
            return

        time.sleep(random.uniform(*delay_range))
        body = b"OK"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        conn.sendall(response)
    except OSError:
        pass
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--min-delay", type=float, default=0.005)
    parser.add_argument("--max-delay", type=float, default=0.03)
    parser.add_argument("--flaky", type=float, default=0.0,
                         help="probability [0-1] of silently dropping a request")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", args.port))
    sock.listen(128)
    print(f"[backend:{args.port}] listening (flaky={args.flaky})", flush=True)

    while True:
        conn, _ = sock.accept()
        threading.Thread(
            target=handle_client,
            args=(conn, (args.min_delay, args.max_delay), args.flaky),
            daemon=True,
        ).start()


if __name__ == "__main__":
    main()
