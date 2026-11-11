"""Starts an AgentCore runtime's entry point with its port 8080 mapped to a local port.

AgentCore runs each runtime in its own microVM, where the code serves /ping and /invocations on port 8080. Here
runtimes share the host's network, so this launcher maps a bind to port 8080 onto the loopback port that moto
gave the runtime, as a container's port mapping would. It runs as a standalone script and must not import moto.

Usage: python runtime_launcher.py <port> <entry point> [<argument> ...]
"""

import os
import runpy
import shutil
import socket
import sys
from typing import Any

AGENTCORE_PORT = 8080


def main() -> None:
    port = int(sys.argv[1])
    entry_point = sys.argv[2:]
    bind = socket.socket.bind

    def mapped_bind(self: socket.socket, address: Any) -> None:
        if isinstance(address, tuple) and len(address) >= 2:
            if address[1] == AGENTCORE_PORT:
                loopback = "::1" if self.family == socket.AF_INET6 else "127.0.0.1"
                address = (loopback, port, *address[2:])
        bind(self, address)

    socket.socket.bind = mapped_bind  # type: ignore[method-assign,assignment]
    script = entry_point[0]
    if not os.path.exists(script):
        script = shutil.which(script) or script
    sys.argv = entry_point
    # As `python <script>` would: the script's directory first, not this launcher's
    sys.path[0] = os.path.dirname(os.path.abspath(script))
    runpy.run_path(script, run_name="__main__")


if __name__ == "__main__":
    main()
