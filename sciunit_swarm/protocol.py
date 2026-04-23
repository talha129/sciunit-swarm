import json


def send_json(sock, obj):
    data = json.dumps(obj).encode()
    sock.sendall(len(data).to_bytes(4, 'big') + data)


def recv_json(sock):
    n = int.from_bytes(_recv_exact(sock, 4), 'big')
    return json.loads(_recv_exact(sock, n))


def recv_exact(sock, n):
    return _recv_exact(sock, n)


def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('socket closed')
        buf += chunk
    return buf
