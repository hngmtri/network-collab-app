"""Encrypted, stop-and-wait reliable file upload over UDP.

Each datagram is a Fernet-encrypted JSON packet. The sender waits for an ACK
for every packet and retransmits it on timeout. Small chunks avoid IP
fragmentation; the server reassembles and verifies the complete SHA-256 digest.
"""

import base64
import hashlib
import json
import os
import socket
import uuid

from crypto import decrypt_bytes, encrypt_bytes
from protocol import MAX_FILE_SIZE

CHUNK_SIZE = 400  # Keeps encrypted UDP datagrams comfortably below common MTUs.
ACK_TIMEOUT = 0.7
MAX_RETRIES = 12


def _encode(packet):
    return encrypt_bytes(json.dumps(packet, separators=(",", ":")).encode("utf-8"))


def _decode(datagram):
    return json.loads(decrypt_bytes(datagram).decode("utf-8"))


def _exchange(sock, server, packet, expected, transfer_id, index=None):
    datagram = _encode(packet)
    for _ in range(MAX_RETRIES):
        sock.sendto(datagram, server)
        deadline = socket.getdefaulttimeout()
        sock.settimeout(ACK_TIMEOUT)
        try:
            while True:
                response, address = sock.recvfrom(4096)
                if address != server:
                    continue
                try:
                    ack = _decode(response)
                except Exception:
                    continue
                if ack.get("transfer_id") != transfer_id:
                    continue
                if ack.get("type") == "UDP_FILE_ERROR":
                    raise OSError(ack.get("reason", "UDP file transfer failed"))
                if ack.get("type") == "UDP_FILE_ACK" and ack.get("ack") == expected:
                    if index is None or ack.get("index") == index:
                        return ack
        except socket.timeout:
            pass
        finally:
            sock.settimeout(deadline)
    raise TimeoutError(f"No UDP acknowledgement for {expected}; transfer stopped")


def send_file_udp(path, sender, recipient, server_host, server_port, progress=None):
    """Upload one file reliably to the server, which relays it to recipient."""
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        raise ValueError("Maximum supported file size is 100 MiB")
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    transfer_id = uuid.uuid4().hex
    server = (socket.gethostbyname(server_host), server_port)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
        udp.settimeout(ACK_TIMEOUT)
        _exchange(udp, server, {
            "type": "UDP_FILE_START", "transfer_id": transfer_id,
            "sender": sender, "to": recipient, "name": os.path.basename(path), "size": size,
            "sha256": digest.hexdigest(),
        }, "START", transfer_id)
        sent = 0
        with open(path, "rb") as source:
            index = 0
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                _exchange(udp, server, {
                    "type": "UDP_FILE_CHUNK", "transfer_id": transfer_id,
                    "index": index, "data": base64.b64encode(chunk).decode("ascii"),
                }, "CHUNK", transfer_id, index)
                sent += len(chunk)
                index += 1
                if progress:
                    progress(100 if size == 0 else int(sent * 100 / size))
        _exchange(udp, server, {
            "type": "UDP_FILE_END", "transfer_id": transfer_id,
        }, "COMPLETE", transfer_id)
    return transfer_id


class UDPFileUploadReceiver:
    """Server-side UDP reassembly, duplicate handling, and integrity checks."""

    def __init__(self, registry, output_dir="received_files"):
        self.registry = registry
        self.output_dir = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        self.transfers = {}
        self.completed = set()

    def handle(self, datagram, address, udp_socket):
        try:
            try:
                packet = _decode(datagram)
            except Exception:  # Ignore malformed or unauthenticated datagrams.
                return
            kind = packet.get("type")
            transfer_id = str(packet.get("transfer_id", ""))
            if not transfer_id:
                return
            if transfer_id in self.completed and kind == "UDP_FILE_END":
                self._reply(udp_socket, address, transfer_id, "COMPLETE")
                return
            if kind == "UDP_FILE_START":
                sender = str(packet.get("sender", "")).strip()
                recipient = str(packet.get("to", "")).strip()
                name = os.path.basename(str(packet.get("name", "")))
                size = packet.get("size")
                digest = str(packet.get("sha256", ""))
                if not self.registry.get(sender):
                    self._error(udp_socket, address, transfer_id, "Sender is not authenticated")
                    return
                target = self.registry.get(recipient)
                if not target:
                    self._error(udp_socket, address, transfer_id, "Recipient is not online")
                    return
                if not name or not isinstance(size, int) or size < 0 or size > MAX_FILE_SIZE or len(digest) != 64:
                    self._error(udp_socket, address, transfer_id, "Invalid file metadata")
                    return
                existing = self.transfers.get(transfer_id)
                if existing:
                    if existing["address"] != address or existing["sender"] != sender:
                        self._error(udp_socket, address, transfer_id, "Transfer ID is already in use")
                        return
                    self._reply(udp_socket, address, transfer_id, "START")
                    return
                temp_path = os.path.join(self.output_dir, f".{transfer_id}.udp-part")
                stream = open(temp_path, "wb")
                self.transfers[transfer_id] = {
                    "sender": sender, "recipient": recipient, "connection": target["connection"],
                    "name": name, "size": size, "sha256": digest.lower(), "path": temp_path,
                    "stream": stream, "received": 0, "next_index": 0,
                    "digest": hashlib.sha256(), "address": address,
                }
                self._reply(udp_socket, address, transfer_id, "START")
                return

            state = self.transfers.get(transfer_id)
            if not state or state["address"] != address:
                return
            if kind == "UDP_FILE_CHUNK":
                index = packet.get("index")
                if not isinstance(index, int) or index < 0:
                    return
                if index < state["next_index"]:  # Retransmission: ACK again without duplicating bytes.
                    self._reply(udp_socket, address, transfer_id, "CHUNK", index)
                    return
                if index != state["next_index"]:
                    return
                try:
                    data = base64.b64decode(packet.get("data", ""), validate=True)
                except (ValueError, TypeError):
                    return
                if len(data) > CHUNK_SIZE or state["received"] + len(data) > state["size"]:
                    self._error(udp_socket, address, transfer_id, "Invalid chunk size")
                    return
                state["stream"].write(data)
                state["digest"].update(data)
                state["received"] += len(data)
                state["next_index"] += 1
                self._reply(udp_socket, address, transfer_id, "CHUNK", index)
                return

            if kind == "UDP_FILE_END":
                state["stream"].close()
                if state["received"] != state["size"] or state["digest"].hexdigest() != state["sha256"]:
                    self._discard(transfer_id, state)
                    self._error(udp_socket, address, transfer_id, "File integrity check failed")
                    return
                try:
                    self._relay_to_recipient(state, transfer_id)
                except (OSError, ConnectionError):
                    self._discard(transfer_id, state)
                    self._error(udp_socket, address, transfer_id, "Recipient disconnected during transfer")
                    return
                self._discard(transfer_id, state)
                self.completed.add(transfer_id)
                if len(self.completed) > 2000:
                    self.completed.clear()
                self._reply(udp_socket, address, transfer_id, "COMPLETE")
        except (ValueError, OSError, KeyError, TypeError):
            return

    @staticmethod
    def _relay_to_recipient(state, transfer_id):
        connection = state["connection"]
        connection.send({"type": "FILE_START", "transfer_id": transfer_id,
                         "from": state["sender"], "name": state["name"],
                         "size": state["size"], "sha256": state["sha256"]})
        with open(state["path"], "rb") as source:
            index = 0
            while True:
                data = source.read(48 * 1024)
                if not data:
                    break
                connection.send({"type": "FILE_CHUNK", "transfer_id": transfer_id,
                                 "index": index, "data": base64.b64encode(data).decode("ascii")})
                index += 1
        connection.send({"type": "FILE_END", "transfer_id": transfer_id,
                         "size": state["size"], "sha256": state["sha256"],
                         "name": state["name"], "from": state["sender"]})

    def _discard(self, transfer_id, state):
        try:
            if not state["stream"].closed:
                state["stream"].close()
        finally:
            try:
                os.remove(state["path"])
            except FileNotFoundError:
                pass
            self.transfers.pop(transfer_id, None)

    @staticmethod
    def _reply(sock, address, transfer_id, ack, index=None):
        packet = {"type": "UDP_FILE_ACK", "transfer_id": transfer_id, "ack": ack}
        if index is not None:
            packet["index"] = index
        sock.sendto(_encode(packet), address)

    @staticmethod
    def _error(sock, address, transfer_id, reason):
        sock.sendto(_encode({"type": "UDP_FILE_ERROR", "transfer_id": transfer_id,
                             "reason": reason}), address)
