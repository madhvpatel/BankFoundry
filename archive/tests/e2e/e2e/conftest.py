import os
import random
import socket
import subprocess
import time
from contextlib import closing

import pytest


def _pick_free_port() -> int:
    for _ in range(50):
        port = random.randint(18000, 24000)
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("Could not find a free port")


@pytest.fixture(scope="session")
def streamlit_url():
    """Start the Streamlit app once for the e2e suite."""
    port = _pick_free_port()
    url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env.setdefault("STREAMLIT_SERVER_HEADLESS", "true")

    # Keep stdout visible for debugging when tests fail.
    cmd = [
        "streamlit",
        "run",
        os.path.join(os.getcwd(), "main.py"),
        "--server.headless",
        "true",
        "--server.port",
        str(port),
        "--server.address",
        "127.0.0.1",
    ]

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        # Wait for readiness.
        deadline = time.time() + 60
        ready = False
        output = []
        while time.time() < deadline:
            line = proc.stdout.readline() if proc.stdout else ""
            if line:
                output.append(line)
                if "You can now view your Streamlit app" in line or "Local URL" in line:
                    ready = True
                    break
            else:
                time.sleep(0.1)
        if not ready:
            raise RuntimeError("Streamlit did not become ready. Output:\n" + "".join(output[-200:]))

        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()
