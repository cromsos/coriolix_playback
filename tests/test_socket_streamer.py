"""
Tests for socket streaming behaviour of TimeseriesReader.

The tests start a temporary TCP server on localhost and verify that the
TimeseriesReader.stream_data method sends JSON lines for each record.
"""
import socket
import threading
import json
import time

from timeseries_data_reader.timeseries_reader import TimeseriesReader


def _run_server_and_collect(port, messages, ready_event):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", port))
    server_sock.listen(1)
    # signal ready
    ready_event.set()
    conn, _ = server_sock.accept()
    with conn:
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
    server_sock.close()
    # split received data into lines and parse JSON
    for line in data.splitlines():
        if line.strip():
            messages.append(json.loads(line.decode("utf-8")))


def test_can_stream_data_via_socket(sample_csv_file):
    # prepare server
    messages = []
    ready = threading.Event()

    # bind to an ephemeral port by passing 0, then get the port
    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    temp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    temp_sock.bind(("127.0.0.1", 0))
    port = temp_sock.getsockname()[1]
    temp_sock.close()

    server_thread = threading.Thread(target=_run_server_and_collect, args=(port, messages, ready), daemon=True)
    server_thread.start()

    # wait until server thread signals it's ready
    ready.wait(timeout=1.0)

    # call stream_data which should connect and send JSON-lines
    reader = TimeseriesReader(str(sample_csv_file))
    # use interval=0 to send immediately for the test
    reader.stream_data(host="127.0.0.1", port=port, interval=0)

    # give a small moment for server to process
    server_thread.join(timeout=2.0)

    # check that we received the same number of records as in the CSV
    assert len(messages) == 4
    assert messages[0]["sensor_id"] == "sensor_1"
    assert float(messages[0]["value"]) == 23.5
