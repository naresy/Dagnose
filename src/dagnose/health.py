"""A tiny HTTP health endpoint for platforms that probe containers (Cloud Run, Azure
Container Apps, Kubernetes liveness/readiness). Started by `dagnose worker` when PORT
(Cloud Run's convention) or DAGNOSE_HEALTH_PORT is set. It reports only queue counts:
no failure data, no settings, no secrets.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from dagnose import __version__
from dagnose.queue import JobQueue


def start_health_server(port: int, queue_path: Path, host: str = "0.0.0.0") -> ThreadingHTTPServer:  # noqa: S104 - container port
    started = time.time()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server API
            if self.path.split("?")[0] not in ("/", "/healthz"):
                self.send_response(404)
                self.end_headers()
                return
            try:
                counts = JobQueue(queue_path).counts()
                status, body = (
                    200,
                    {
                        "status": "ok",
                        "version": __version__,
                        "uptime_s": int(time.time() - started),
                        "jobs": counts,
                    },
                )
            except Exception as exc:  # the queue is unreadable: report unhealthy
                status, body = 503, {"status": "error", "error": type(exc).__name__}
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True, name="dagnose-health").start()
    return server
