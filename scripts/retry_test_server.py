#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar


class RetryTestHandler(BaseHTTPRequestHandler):
    counter: ClassVar[int] = 0
    fail_rate: ClassVar[float] = 0.5
    lock: ClassVar[threading.Lock] = threading.Lock()

    def do_GET(self) -> None:
        self._reply()

    def do_POST(self) -> None:
        self._reply()

    def do_PUT(self) -> None:
        self._reply()

    def do_PATCH(self) -> None:
        self._reply()

    def do_DELETE(self) -> None:
        self._reply()

    def _reply(self) -> None:
        with self.lock:
            type(self).counter += 1
            count = type(self).counter

        fixed_error = self.path.split("?", 1)[0] == "/error"
        should_fail = fixed_error or random.random() < type(self).fail_rate
        status = 500 if should_fail else 200
        body = {
            "count": count,
            "status": status,
            "message": "fixed 500 error" if fixed_error else "random 500 error" if should_fail else "ok",
        }
        payload = json.dumps(body).encode()

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        print(f"{self.command} {self.path} -> {format % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Randomly return HTTP 500 for retry testing.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--fail-rate", type=float, default=0.5, help="Probability of HTTP 500, from 0 to 1.")
    args = parser.parse_args()
    if not 0 <= args.fail_rate <= 1:
        raise ValueError("--fail-rate must be between 0 and 1")

    RetryTestHandler.fail_rate = args.fail_rate
    server = ThreadingHTTPServer((args.host, args.port), RetryTestHandler)
    print(f"Retry test server listening on http://{args.host}:{args.port} fail_rate={args.fail_rate}")
    server.serve_forever()


if __name__ == "__main__":
    main()
