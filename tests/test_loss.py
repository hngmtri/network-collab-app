
import os
import re
import subprocess
import sys
import time

PORT = "19100"
UDP_PORT = "19101"
TEST_SECRET = "local-test-secret-change-this-before-deployment"


def terminate(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def main():
    env = dict(
        os.environ,
        PORT=PORT,
        UDP_PORT=UDP_PORT,
        APP_SECRET=TEST_SECRET,
        PYTHONUNBUFFERED="1",
    )
    server = subprocess.Popen(
        [sys.executable, "server.py"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    clients = []
    try:
        time.sleep(1)
        for name in ("an", "binh"):
            process = subprocess.Popen(
                [sys.executable, "client.py"],
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            process.stdin.write(name + "\n")
            process.stdin.flush()
            clients.append(process)

        time.sleep(1)
        for index in range(1, 21):
            clients[0].stdin.write(f"/to binh message-{index}\n")
            clients[0].stdin.flush()
        time.sleep(2)

        clients[0].stdin.write("/quit\n")
        clients[0].stdin.flush()
        clients[1].stdin.write("/quit\n")
        clients[1].stdin.flush()
        outputs = [client.communicate(timeout=8)[0] for client in clients]
        received = [
            int(value)
            for value in re.findall(r"'text': 'message-(\d+)'", outputs[1])
        ]

        expected = set(range(1, 21))
        print(f"Received {len(received)} messages: {sorted(received)}")
        print(f"All messages received once: {set(received) == expected and len(received) == 20}")
    finally:
        for client in clients:
            terminate(client)
        terminate(server)


if __name__ == "__main__":
    main()
