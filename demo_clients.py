"""Create two live local sessions and sample traffic for the dashboard demo."""

import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
from reliable import ReliableSocket

SERVER = (os.environ.get("SERVER", "127.0.0.1"), int(os.environ.get("PORT", "9000")))


def main():
    alice = ReliableSocket.connect(SERVER)
    bob = ReliableSocket.connect(SERVER)
    connections = [("Alice", alice), ("Bob", bob)]
    try:
        for name, connection in connections:
            connection.send({"type": "AUTH_REQ", "name": name})
        for name, connection in connections:
            packet, _ = connection.recv()
            if packet.get("type") != "AUTH_OK":
                raise RuntimeError(f"{name} login failed: {packet}")

        def drain(connection):
            while True:
                try:
                    connection.recv()
                except (OSError, ConnectionError, ValueError):
                    return

        for _name, connection in connections:
            threading.Thread(target=drain, args=(connection,), daemon=True).start()

        alice.send({
            "type": "MSG_BROADCAST",
            "text": "Live backend demo: Alice sent a broadcast message.",
        })
        bob.send({
            "type": "MSG_UNICAST",
            "to": "Alice",
            "text": "Live backend demo: Bob sent Alice a private message.",
        })
        print("Alice and Bob are connected. Sample broadcast and private messages were sent.")
        print("Leave this process running to keep both users visible; press Ctrl+C to disconnect.")

        while True:
            time.sleep(10)
            for _name, connection in connections:
                connection.send({"type": "PING"}, reliable=False)
    except KeyboardInterrupt:
        print("Disconnecting demo clients...")
    finally:
        for _name, connection in connections:
            try:
                connection.send({"type": "BYE"})
                time.sleep(0.05)
            except (OSError, ConnectionError):
                pass
            connection.close()


if __name__ == "__main__":
    main()
