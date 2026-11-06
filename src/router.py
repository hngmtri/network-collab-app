"""Route application messages and file transfer packets to authenticated peers."""

from filetransfer import FileTransferRelay
from protocol import validate_packet


class MessageRouter:
    def __init__(self, registry, on_message=None, on_file_progress=None):
        self.registry = registry
        self.on_message = on_message
        self.file_relay = FileTransferRelay(registry, on_progress=on_file_progress)

    def route(self, sender_name, connection, packet):
        validate_packet(packet)
        kind = packet["type"]
        if kind == "USER_LIST":
            connection.send({"type": "USER_LIST", "users": self.registry.names()})
        elif kind == "MSG_BROADCAST":
            text = packet.get("text", "")
            if self.on_message:
                self.on_message({"sender": sender_name, "text": text, "is_private": False})
            outgoing = {"type": "MSG", "from": sender_name, "text": text}
            for _name, recipient in self.registry.recipients(exclude=sender_name):
                try:
                    recipient.send(outgoing)
                except (OSError, ConnectionError):
                    pass
        elif kind == "MSG_UNICAST":
            recipient_name = str(packet.get("to", "")).strip()
            recipient = self.registry.get(recipient_name)
            if not recipient:
                connection.send({"type": "ERROR", "reason": "Recipient is not online"})
                return
            text = packet.get("text", "")
            recipient["connection"].send({"type": "MSG", "from": sender_name, "text": text})
            if self.on_message:
                self.on_message({
                    "sender": sender_name,
                    "to": recipient_name,
                    "text": text,
                    "is_private": True,
                })
        elif kind in {"FILE_START", "FILE_CHUNK", "FILE_END"}:
            self.file_relay.handle(sender_name, connection, packet)
        elif kind == "PING":
            connection.send({"type": "PONG"}, reliable=False)
        elif kind == "BYE":
            connection.send({"type": "BYE_OK"})
        else:
            connection.send({"type": "ERROR", "reason": f"Unsupported message type: {kind}"})
