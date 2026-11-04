import base64
import hashlib
import json
import os
import socket
import struct
import threading
import time
import uuid

RTO = 0.5
MAX_RETRIES = 8
MAX_FRAME_SIZE = 8 * 1024 * 1024


def _load_fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "Encryption requires the 'cryptography' package. Install it with: "
            "python -m pip install cryptography"
        ) from exc

    secret = os.environ.get("APP_SECRET")
    if not secret:
        raise RuntimeError(
            "Set APP_SECRET to the same strong secret on the server and all clients."
        )
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def _send_frame(sock, payload):
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if len(data) > MAX_FRAME_SIZE:
        raise ValueError("Frame exceeds the maximum allowed size")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _recv_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("Peer closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)


def _recv_frame(sock):
    header = _recv_exact(sock, 4)
    (length,) = struct.unpack("!I", header)
    if length <= 0 or length > MAX_FRAME_SIZE:
        raise ValueError("Invalid frame length")
    data = _recv_exact(sock, length)
    packet = json.loads(data.decode("utf-8"))
    if not isinstance(packet, dict):
        raise ValueError("Frame must contain a JSON object")
    return packet


class ReliableSocket:
    """One TCP connection with encrypted frames and a background receiver."""

    def __init__(self, sock=None, bind=None, on_giveup=None):
        self.sock = sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.on_giveup = on_giveup
        self.fernet = _load_fernet()
        self.send_lock = threading.Lock()
        self.pending_lock = threading.Lock()
        self.pending = {}
        self.received_ids = set()
        self.received_order = []
        self.incoming = __import__("queue").Queue()
        self.closed = threading.Event()
        self.peer = None

        if bind:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(bind)

    @classmethod
    def connect(cls, address, timeout=10, on_giveup=None):
        sock = socket.create_connection(address, timeout=timeout)
        sock.settimeout(None)
        instance = cls(sock=sock, on_giveup=on_giveup)
        instance.peer = address
        instance._start_receiver()
        return instance

    def accept(self):
        self.sock.listen()
        conn, address = self.sock.accept()
        conn.settimeout(None)
        instance = ReliableSocket(sock=conn, on_giveup=self.on_giveup)
        instance.peer = address
        instance._start_receiver()
        return instance, address

    def _start_receiver(self, conn=None):
        self.connection = conn or self.sock
        self.receiver_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.receiver_thread.start()
        self.retransmit_thread = threading.Thread(target=self._retransmit_loop, daemon=True)
        self.retransmit_thread.start()

    def send(self, address, packet=None, reliable=True):
        """Send a packet. Address is accepted for compatibility with old call sites."""
        if packet is None:
            packet = address
        if self.closed.is_set():
            raise ConnectionError("Connection is closed")

        message_id = uuid.uuid4().hex
        envelope = {"kind": "DATA", "id": message_id, "payload": packet}
        if reliable:
            with self.pending_lock:
                self.pending[message_id] = {
                    "packet": packet,
                    "sent_at": time.monotonic(),
                    "retries": 0,
                }
        self._write_encrypted(envelope)

    def _write_encrypted(self, envelope):
        token = self.fernet.encrypt(json.dumps(envelope).encode("utf-8"))
        with self.send_lock:
            _send_frame(self.connection, {"ciphertext": token.decode("ascii")})

    def _reader_loop(self):
        try:
            while not self.closed.is_set():
                outer = _recv_frame(self.connection)
                token = outer.get("ciphertext")
                if not isinstance(token, str):
                    continue
                try:
                    envelope = json.loads(self.fernet.decrypt(token.encode("ascii")).decode("utf-8"))
                except Exception:
                    continue
                if not isinstance(envelope, dict):
                    continue

                kind = envelope.get("kind")
                if kind == "ACK":
                    with self.pending_lock:
                        self.pending.pop(envelope.get("id"), None)
                elif kind == "DATA":
                    message_id = envelope.get("id")
                    self._write_encrypted({"kind": "ACK", "id": message_id})
                    if message_id not in self.received_ids:
                        self.received_ids.add(message_id)
                        self.received_order.append(message_id)
                        if len(self.received_order) > 10000:
                            expired = self.received_order.pop(0)
                            self.received_ids.discard(expired)
                        self.incoming.put(envelope.get("payload", {}))
        except (OSError, ConnectionError, ValueError, json.JSONDecodeError) as exc:
            if not self.closed.is_set():
                self.incoming.put(exc)
                self.closed.set()

    def _retransmit_loop(self):
        while not self.closed.wait(0.1):
            now = time.monotonic()
            failed = []
            with self.pending_lock:
                for message_id, item in list(self.pending.items()):
                    if now - item["sent_at"] < RTO:
                        continue
                    if item["retries"] >= MAX_RETRIES:
                        failed.append((message_id, item["packet"]))
                        del self.pending[message_id]
                        continue
                    item["sent_at"] = now
                    item["retries"] += 1
                    self._write_encrypted({
                        "kind": "DATA",
                        "id": message_id,
                        "payload": item["packet"],
                    })
            for _message_id, packet in failed:
                if self.on_giveup:
                    self.on_giveup(self.peer, packet)

    def recv(self):
        packet = self.incoming.get()
        if isinstance(packet, Exception):
            raise packet
        return packet, self.peer

    def close(self):
        if self.closed.is_set():
            return
        self.closed.set()
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except (AttributeError, OSError):
            pass
        try:
            self.connection.close()
        except (AttributeError, OSError):
            pass
