"""Exercise the controller against a small local MQTT protocol test peer."""
import signal
import socket
import struct
import subprocess
import sys
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


def run(binary, disconnect=False):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(8)
        process = subprocess.Popen(
            [binary, "127.0.0.1", str(listener.getsockname()[1]), "test-boiling"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(8)
                header, body = read_packet(connection)
                assert header == 0x10
                assert body[:7] == b"\x00\x04MQTT\x04"
                assert body[7] == 2  # Clean session, no will or authentication.
                assert struct.unpack("!H", body[8:10])[0] == 60
                assert body[12:] == b"test-boiling"
                connection.sendall(b"\x20\x02\x00\x00")

                arrivals = []
                for _ in range(1 if disconnect else 3):
                    header, body = read_packet(connection)
                    assert header == 0x32  # PUBLISH, QoS 1, no retain.
                    topic_size = struct.unpack("!H", body[:2])[0]
                    assert body[2:2 + topic_size] == b"heartbeat"
                    offset = 2 + topic_size
                    packet_id = body[offset:offset + 2]
                    timestamp, name = struct.unpack("@i32s", body[offset + 2:])
                    assert abs(timestamp - time.time()) < 5
                    assert name.rstrip(b"\0") == b"test-boiling"
                    arrivals.append(time.monotonic())
                    connection.sendall(b"\x40\x02" + packet_id)

                if disconnect:
                    connection.shutdown(socket.SHUT_RDWR)
                else:
                    for before, after in zip(arrivals, arrivals[1:]):
                        assert 1.7 <= after - before <= 3.0
                    process.send_signal(signal.SIGTERM)
                    header, body = read_packet(connection)
                    assert header == 0xE0 and body == b""

            if disconnect:
                time.sleep(0.5)
                assert process.poll() is None, "Broker loss stopped boiling"
                process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
            assert "Boiling started." in stdout
            assert "Boiling complete" not in stdout  # No acceleration of the hour.
            if disconnect:
                assert "boiling continues" in stderr
            else:
                assert stderr == "", stderr
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


if __name__ == "__main__":
    run(sys.argv[1])
    run(sys.argv[1], disconnect=True)
    invalid = subprocess.run(
        [sys.argv[1], "127.0.0.1", "1883", "x" * 32],
        capture_output=True, text=True, timeout=5,
    )
    assert invalid.returncode != 0 and "1 to 31 bytes" in invalid.stderr
    print("Boiling MQTT integration tests passed.")
