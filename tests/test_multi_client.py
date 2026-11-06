"""Unit tests for packet validation and concurrent user registration."""

import os
import sys
import threading
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from protocol import validate_packet
from users import UserRegistry
from filetransfer import FileTransferReceiver, FileTransferRelay


class ProtocolTests(unittest.TestCase):
    def test_valid_chat_packet_is_returned(self):
        packet = {"type": "MSG_BROADCAST", "text": "hello"}
        self.assertIs(validate_packet(packet), packet)

    def test_empty_login_name_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_packet({"type": "AUTH_REQ", "name": "   "})

    def test_oversized_message_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_packet({"type": "MSG_BROADCAST", "text": "x" * 16001})


class MultiClientRegistryTests(unittest.TestCase):
    def test_registry_accepts_concurrent_distinct_clients(self):
        registry = UserRegistry()
        results = []
        result_lock = threading.Lock()

        def add_client(index):
            result = registry.add(f"user-{index}", object(), ("127.0.0.1", 10000 + index))
            with result_lock:
                results.append(result[0])

        threads = [threading.Thread(target=add_client, args=(i,)) for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 20)
        self.assertTrue(all(results))
        self.assertEqual(len(registry.names()), 20)

    def test_duplicate_name_is_rejected_for_another_connection(self):
        registry = UserRegistry()
        self.assertTrue(registry.add("alice", object(), ("127.0.0.1", 10001))[0])
        accepted, reason = registry.add("alice", object(), ("127.0.0.1", 10002))
        self.assertFalse(accepted)
        self.assertEqual(reason, "Name is already in use")


class FileTransferTests(unittest.TestCase):
    class FakeConnection:
        def __init__(self):
            self.packets = []

        def send(self, packet, reliable=True):
            self.packets.append(packet)

    def test_file_relay_forwards_chunks_and_receiver_checks_digest(self):
        payload = b"A small file sent in several chunks."
        checksum = hashlib.sha256(payload).hexdigest()
        transfer_id = "test-transfer-1"
        sender = self.FakeConnection()
        recipient = self.FakeConnection()
        registry = UserRegistry()
        registry.add("Bob", recipient, ("127.0.0.1", 20002))
        progress = []
        relay = FileTransferRelay(registry, on_progress=lambda name, value: progress.append(value))

        relay.handle("Alice", sender, {
            "type": "FILE_START", "transfer_id": transfer_id,
            "to": "Bob", "name": "sample.txt", "size": len(payload),
            "sha256": checksum,
        })
        first, second = payload[:12], payload[12:]
        for index, data in enumerate((first, second)):
            relay.handle("Alice", sender, {
                "type": "FILE_CHUNK", "transfer_id": transfer_id,
                "index": index, "data": base64.b64encode(data).decode("ascii"),
            })
        relay.handle("Alice", sender, {"type": "FILE_END", "transfer_id": transfer_id})

        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as output_dir:
            receiver = FileTransferReceiver(output_dir)
            for packet in recipient.packets:
                receiver.handle(packet)
            with open(os.path.join(output_dir, "sample.txt"), "rb") as received:
                self.assertEqual(received.read(), payload)

        self.assertEqual(sender.packets[-1]["type"], "FILE_DONE")
        self.assertEqual(progress[-1], 100)


if __name__ == "__main__":
    unittest.main()
