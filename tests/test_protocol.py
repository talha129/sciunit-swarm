"""Tests for socket protocol helpers."""

import socket
import threading

import pytest

from sciunit_swarm.protocol import send_json, recv_json, recv_exact


def socket_pair():
    """Return a connected (client, server) socket pair."""
    srv = socket.socket()
    srv.bind(('127.0.0.1', 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    client = socket.create_connection(('127.0.0.1', port))
    server, _ = srv.accept()
    srv.close()
    return client, server


def test_send_recv_json_roundtrip():
    c, s = socket_pair()
    send_json(c, {'type': 'REGISTER', 'node_id': 'abc'})
    msg = recv_json(s)
    assert msg == {'type': 'REGISTER', 'node_id': 'abc'}
    c.close(); s.close()


def test_send_recv_json_various_types():
    c, s = socket_pair()
    payload = {'list': [1, 2, 3], 'nested': {'a': True}, 'num': 42}
    send_json(c, payload)
    assert recv_json(s) == payload
    c.close(); s.close()


def test_recv_exact_full():
    c, s = socket_pair()
    c.sendall(b'hello world')
    data = recv_exact(s, 11)
    assert data == b'hello world'
    c.close(); s.close()


def test_recv_exact_partial_sends():
    c, s = socket_pair()

    def sender():
        c.sendall(b'abc')
        c.sendall(b'def')

    t = threading.Thread(target=sender)
    t.start()
    data = recv_exact(s, 6)
    t.join()
    assert data == b'abcdef'
    c.close(); s.close()


def test_recv_json_multiple_messages():
    c, s = socket_pair()
    send_json(c, {'seq': 1})
    send_json(c, {'seq': 2})
    assert recv_json(s)['seq'] == 1
    assert recv_json(s)['seq'] == 2
    c.close(); s.close()
