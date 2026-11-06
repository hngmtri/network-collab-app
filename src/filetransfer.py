"""Chunked, checksummed file delivery over the reliable encrypted TCP channel."""

import base64
import hashlib
import os
import uuid

from protocol import MAX_CHUNK_SIZE, MAX_FILE_SIZE, validate_packet


def describe_file(path):
    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        raise ValueError("Maximum supported file size is 100 MiB")
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return size, digest.hexdigest()


def send_file(connection, recipient, path, progress=None):
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    size, digest = describe_file(path)
    transfer_id = uuid.uuid4().hex
    connection.send({
        "type": "FILE_START", "transfer_id": transfer_id,
        "to": recipient, "name": os.path.basename(path),
        "size": size, "sha256": digest,
    })
    sent = 0
    with open(path, "rb") as source:
        index = 0
        while True:
            chunk = source.read(MAX_CHUNK_SIZE)
            if not chunk:
                break
            connection.send({
                "type": "FILE_CHUNK", "transfer_id": transfer_id,
                "index": index, "data": base64.b64encode(chunk).decode("ascii"),
            })
            sent += len(chunk)
            index += 1
            if progress:
                progress(100 if size == 0 else int(sent * 100 / size))
    connection.send({"type": "FILE_END", "transfer_id": transfer_id})
    return transfer_id


class FileTransferRelay:
    """Validate transfer ownership and relay chunks between authenticated peers."""

    def __init__(self, registry, on_progress=None):
        self.registry = registry
        self.on_progress = on_progress
        self.transfers = {}

    def handle(self, sender_name, connection, packet):
        validate_packet(packet)
        kind = packet["type"]
        transfer_id = packet.get("transfer_id")

        if kind == "FILE_START":
            recipient_name = str(packet.get("to", "")).strip()
            recipient = self.registry.get(recipient_name)
            if not recipient:
                connection.send({"type": "ERROR", "reason": "File recipient is not online"})
                return
            state = {
                "sender": sender_name,
                "recipient": recipient_name,
                "sender_connection": connection,
                "recipient_connection": recipient["connection"],
                "name": os.path.basename(str(packet["name"])),
                "size": packet["size"],
                "sha256": packet["sha256"],
                "received": 0,
                "next_index": 0,
            }
            self.transfers[transfer_id] = state
            recipient["connection"].send({
                "type": "FILE_START", "transfer_id": transfer_id,
                "from": sender_name, "name": state["name"],
                "size": state["size"], "sha256": state["sha256"],
            })
            if self.on_progress:
                self.on_progress(state["name"], 0)
            return

        state = self.transfers.get(transfer_id)
        if not state or state["sender_connection"] is not connection:
            connection.send({"type": "ERROR", "reason": "Unknown file transfer"})
            return

        if kind == "FILE_CHUNK":
            if packet["index"] != state["next_index"]:
                connection.send({"type": "ERROR", "reason": "File chunk order is invalid"})
                return
            raw = base64.b64decode(packet["data"], validate=True)
            if len(raw) > MAX_CHUNK_SIZE or state["received"] + len(raw) > state["size"]:
                connection.send({"type": "ERROR", "reason": "File chunk size is invalid"})
                return
            state["received"] += len(raw)
            state["next_index"] += 1
            state["recipient_connection"].send(packet)
            progress = 100 if state["size"] == 0 else int(state["received"] * 100 / state["size"])
            state["sender_connection"].send({"type": "FILE_PROGRESS", "transfer_id": transfer_id, "progress": progress})
            if self.on_progress:
                self.on_progress(state["name"], progress)
            return

        if kind == "FILE_END":
            if state["received"] != state["size"]:
                connection.send({"type": "ERROR", "reason": "File transfer ended before all bytes arrived"})
                return
            state["recipient_connection"].send({
                "type": "FILE_END", "transfer_id": transfer_id,
                "size": state["size"], "sha256": state["sha256"],
                "name": state["name"], "from": sender_name,
            })
            connection.send({"type": "FILE_DONE", "transfer_id": transfer_id})
            if self.on_progress:
                self.on_progress(state["name"], 100)
            self.transfers.pop(transfer_id, None)


class FileTransferReceiver:
    """Write incoming chunks safely and verify the completed file digest."""

    def __init__(self, output_dir="received_files", on_progress=None):
        self.output_dir = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        self.on_progress = on_progress
        self.active = {}

    def handle(self, packet):
        kind = packet.get("type")
        transfer_id = packet.get("transfer_id")
        if kind == "FILE_START":
            validate_packet(packet)
            name = os.path.basename(packet["name"])
            temp_path = os.path.join(self.output_dir, f".{transfer_id}.part")
            stream = open(temp_path, "wb")
            self.active[transfer_id] = {
                "name": name, "size": packet["size"], "sha256": packet["sha256"],
                "path": temp_path, "stream": stream, "received": 0, "next_index": 0,
                "digest": hashlib.sha256(),
            }
            print(f"Receiving {name} from {packet.get('from', 'peer')} ({packet['size']} bytes)")
            return True
        state = self.active.get(transfer_id)
        if not state:
            return False
        if kind == "FILE_CHUNK":
            validate_packet(packet)
            if packet["index"] != state["next_index"]:
                raise ValueError("Received file chunks out of order")
            data = base64.b64decode(packet["data"], validate=True)
            state["stream"].write(data)
            state["digest"].update(data)
            state["received"] += len(data)
            state["next_index"] += 1
            if self.on_progress:
                self.on_progress(state["name"], int(state["received"] * 100 / max(1, state["size"])))
            return True
        if kind == "FILE_END":
            state["stream"].close()
            if state["received"] != state["size"] or state["digest"].hexdigest() != state["sha256"]:
                os.remove(state["path"])
                self.active.pop(transfer_id, None)
                raise ValueError("File integrity check failed; incomplete file was discarded")
            final_path = os.path.join(self.output_dir, state["name"])
            if os.path.exists(final_path):
                stem, ext = os.path.splitext(state["name"])
                final_path = os.path.join(self.output_dir, f"{stem}_{transfer_id[:8]}{ext}")
            os.replace(state["path"], final_path)
            self.active.pop(transfer_id, None)
            print(f"File received and verified: {final_path}")
            return True
        return False
