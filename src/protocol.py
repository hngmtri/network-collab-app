"""Application message types and packet validation."""

MAX_TEXT_LENGTH = 16_000
MAX_FILENAME_LENGTH = 255
MAX_FILE_SIZE = 100 * 1024 * 1024
MAX_CHUNK_SIZE = 48 * 1024

MESSAGE_TYPES = {
    "AUTH_REQ", "AUTH_OK", "AUTH_FAIL", "ERROR", "PING", "PONG",
    "USER_LIST", "MSG_BROADCAST", "MSG_UNICAST", "MSG", "BYE", "BYE_OK",
    "FILE_START", "FILE_CHUNK", "FILE_END", "FILE_PROGRESS", "FILE_DONE",
}


def validate_packet(packet):
    if not isinstance(packet, dict):
        raise ValueError("Packet must be a JSON object")
    message_type = packet.get("type")
    if message_type not in MESSAGE_TYPES:
        raise ValueError("Unknown packet type")

    if message_type == "AUTH_REQ":
        if not str(packet.get("name", "")).strip():
            raise ValueError("A non-empty name is required")
    elif message_type in {"MSG_BROADCAST", "MSG_UNICAST"}:
        if not isinstance(packet.get("text", ""), str):
            raise ValueError("Message text must be a string")
        if len(packet.get("text", "")) > MAX_TEXT_LENGTH:
            raise ValueError("Message is too long")
        if message_type == "MSG_UNICAST" and not str(packet.get("to", "")).strip():
            raise ValueError("A recipient name is required")
    elif message_type == "FILE_START":
        if not packet.get("transfer_id") or not packet.get("name"):
            raise ValueError("File transfer ID and name are required")
        size = packet.get("size")
        if not isinstance(size, int) or size < 0 or size > MAX_FILE_SIZE:
            raise ValueError("File size is invalid or exceeds 100 MiB")
        digest = packet.get("sha256", "")
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest.lower()):
            raise ValueError("A SHA-256 digest is required")
        if len(str(packet.get("name"))) > MAX_FILENAME_LENGTH:
            raise ValueError("Filename is too long")
    elif message_type == "FILE_CHUNK":
        if not packet.get("transfer_id") or not isinstance(packet.get("index"), int):
            raise ValueError("File chunk ID and index are required")
        if not isinstance(packet.get("data"), str) or len(packet["data"]) > MAX_CHUNK_SIZE * 2:
            raise ValueError("File chunk is invalid or too large")
    elif message_type in {"FILE_END", "FILE_PROGRESS", "FILE_DONE"}:
        if not packet.get("transfer_id"):
            raise ValueError("File transfer ID is required")
    return packet
