"""Thread-safe in-memory registry for authenticated client sessions."""

import threading
import time


class UserRegistry:
    def __init__(self):
        self._users = {}
        self.lock = threading.RLock()

    def add(self, name, connection, address):
        name = str(name).strip()
        if not name:
            return False, "Name cannot be empty"
        with self.lock:
            current = self._users.get(name)
            if current and current["connection"] is not connection:
                return False, "Name is already in use"
            self._users[name] = {
                "connection": connection,
                "address": address,
                "last_seen": time.monotonic(),
            }
        return True, name

    def get(self, name):
        with self.lock:
            return self._users.get(name)

    def touch(self, name, connection):
        with self.lock:
            user = self._users.get(name)
            if not user or user["connection"] is not connection:
                return False
            user["last_seen"] = time.monotonic()
            return True

    def names(self):
        with self.lock:
            return sorted(self._users)

    def recipients(self, exclude=None):
        with self.lock:
            return [(name, user["connection"]) for name, user in self._users.items() if name != exclude]

    def remove_connection(self, connection):
        with self.lock:
            removed = [name for name, user in self._users.items() if user["connection"] is connection]
            for name in removed:
                self._users.pop(name, None)
            return removed

    def expire(self, timeout_seconds=30):
        now = time.monotonic()
        with self.lock:
            removed = [
                (name, user["connection"])
                for name, user in self._users.items()
                if now - user["last_seen"] > timeout_seconds
            ]
            for name, _connection in removed:
                self._users.pop(name, None)
            return removed

    def ui_snapshot(self):
        with self.lock:
            return [
                f"{name} ({user['address'][0]}:{user['address'][1]})"
                for name, user in sorted(self._users.items())
            ]
