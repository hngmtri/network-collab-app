"""Local encrypted TCP delivery test; TCP itself handles packet loss/reordering."""

import os
import sys
import threading
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))
os.environ.setdefault("APP_SECRET", "unit-test-secret-at-least-16-chars")

from reliable import ReliableSocket


class ReliableDeliveryTests(unittest.TestCase):
    def test_encrypted_message_is_delivered_and_acknowledged(self):
        listener = ReliableSocket(bind=("127.0.0.1", 0))
        listener.sock.listen(1)
        address = listener.sock.getsockname()
        accepted = []
        accept_thread = threading.Thread(target=lambda: accepted.append(listener.accept()[0]), daemon=True)
        accept_thread.start()

        client = ReliableSocket.connect(address)
        accept_thread.join(timeout=3)
        self.assertEqual(len(accepted), 1)
        server_side = accepted[0]
        try:
            client.send({"type": "MSG_BROADCAST", "text": "delivery check"})
            received, _ = server_side.recv()
            self.assertEqual(received["text"], "delivery check")

            server_side.send({"type": "MSG", "from": "server", "text": "reply"})
            response, _ = client.recv()
            self.assertEqual(response["text"], "reply")
        finally:
            client.close()
            server_side.close()
            listener.sock.close()


if __name__ == "__main__":
    unittest.main()
