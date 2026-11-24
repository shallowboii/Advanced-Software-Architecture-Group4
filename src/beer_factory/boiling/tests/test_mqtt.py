"""Check a machine's MQTT heartbeats with a protocol peer on localhost:1883.

Stop any local broker before running. The heartbeat payload uses the shared
library's native pair of integers: Unix timestamp and machine index.
"""
import argparse
import os
import signal
import socket
import struct
import subprocess
import time


def read_exact(connection, count):
    data = bytearray()
    while len(data) < count:
        chunk = connection.recv(count - len(data))
        if not chunk:
            raise AssertionError("Unexpected end of MQTT connection")
        data.extend(chunk)
    return bytes(data)


def read_packet(connection):
    header = read_exact(connection, 1)[0]
    remaining = 0
    for index in range(4):
        byte = read_exact(connection, 1)[0]
        remaining += (byte & 127) << (7 * index)
        if byte < 128:
            return header, read_exact(connection, remaining)
    raise AssertionError("Invalid MQTT remaining length")


def run(binary, machine="boiling", disconnect=False):
    expected_id = (machine.upper() + "-7").encode()
    expected_topic = machine.encode()
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 1883))
        listener.listen(1)
        listener.settimeout(8)
        process = subprocess.Popen(
            [binary, "7"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env={**os.environ, "MQTT_HOST": "127.0.0.1", "MQTT_PORT": "1883",
                 "MQTT_USERNAME": "admin", "MQTT_PASSWORD": "admin123"},
        )
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(8)
                header, body = read_packet(connection)
                assert header == 0x10
                assert body[:7] == b"\x00\x04MQTT\x04"
                assert body[7] == 0xC2  # Clean session, username and password.
                assert struct.unpack("!H", body[8:10])[0] == 60
                offset = 10
                for expected in [expected_id, b"admin", b"admin123"]:
                    size = struct.unpack("!H", body[offset:offset + 2])[0]
                    offset += 2
                    assert body[offset:offset + size] == expected
                    offset += size
                assert offset == len(body)
                connection.sendall(b"\x20\x02\x00\x00")

                arrivals = []
                for _ in range(1 if disconnect else 2):
                    header, body = read_packet(connection)
                    assert header == 0x32  # PUBLISH, QoS 1, no retain.
                    topic_size = struct.unpack("!H", body[:2])[0]
                    assert body[2:2 + topic_size] == expected_topic
                    offset = 2 + topic_size
                    packet_id = body[offset:offset + 2]
                    timestamp, machine_idx = struct.unpack("@ii", body[offset + 2:])
                    assert abs(timestamp - time.time()) < 5
                    assert machine_idx == 7
                    arrivals.append(time.monotonic())
                    connection.sendall(b"\x40\x02" + packet_id)

                if disconnect:
                    connection.shutdown(socket.SHUT_RDWR)
                else:
                    for before, after in zip(arrivals, arrivals[1:]):
                        assert 4.0 <= after - before <= 7.0
                    process.send_signal(signal.SIGTERM)

            if disconnect:
                time.sleep(2)
                assert process.poll() is None, "Broker loss stopped the machine"
                process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == -signal.SIGTERM, stderr
            # The hourly processes must not finish during this short MQTT test.
            if machine in {"boiling", "fermentation_maturation"}:
                assert "complete" not in stdout
            # SIGTERM uses the default handler; stdout may still be buffered.
            if not disconnect:
                assert stderr == "", stderr
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", help="Path to the machine executable")
    parser.add_argument(
        "--machine", default="boiling",
        choices=["boiling", "fermentation_maturation", "mashing", "lautering",
                 "filtering", "whirlpooling"],
        help="Machine type used for the client ID and topic (default: boiling)",
    )
    args = parser.parse_args()
    run(args.binary, args.machine)
    run(args.binary, args.machine, disconnect=True)
    for invalid_args in [[], ["1", "2"]]:
        invalid = subprocess.run(
            [args.binary, *invalid_args], capture_output=True, text=True, timeout=5,
        )
        assert invalid.returncode != 0 and "<machine-number>" in invalid.stderr
    print(f"{args.machine}: MQTT integration tests passed.")
