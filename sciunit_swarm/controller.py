#!/usr/bin/env python3
"""
Controller — runs on head node.

exec phase:
  - Accept REGISTER from workers (any count, any time)
  - Launch manager command
  - When manager exits → KILL all registered workers
  - Collect CONTAINER tarballs → build unified container → store as workflow_id

repeat phase:
  - Accept REGISTER(workflow_id) from workers
  - Push CONTAINER tarball to each worker
  - Launch manager command
  - When manager exits → KILL all registered workers
"""

import io
import os
import shutil
import socket
import subprocess
import tarfile
import threading
import time
import uuid

from sciunit_swarm.merge import build_unified_container
from sciunit_swarm.protocol import send_json, recv_json, recv_exact


class Controller:
    def __init__(self, manager_cmd, port=9000, output_dir='./swarm-packages'):
        self.manager_cmd = manager_cmd
        self.port        = port
        self.output_dir  = output_dir
        self.host        = socket.gethostname()

        self.workers        = {}        # node_id → (host, port)  exec workers
        self.repeat_workers = {}        # node_id → (host, port)  repeat workers
        self.workers_lock   = threading.Lock()
        self.stop_event     = threading.Event()

    def run(self):
        workflow_id = str(uuid.uuid4())[:8]
        print(f"[controller] workflow_id    = {workflow_id}")
        print(f"[controller] listening port = {self.port}")
        print(f"[controller] manager cmd    = {self.manager_cmd}")

        reg_thread = threading.Thread(target=self._registry_server, daemon=True)
        reg_thread.start()

        self._run_manager()

        self.stop_event.set()
        pkg_dirs = self._kill_and_collect(workflow_id)

        if pkg_dirs:
            out     = os.path.join(self.output_dir, workflow_id, 'unified')
            tarball = os.path.join(self.output_dir, workflow_id, 'unified.tar.gz')
            t0 = time.time()
            build_unified_container(pkg_dirs, out)
            print(f"[controller] merge done ({time.time() - t0:.2f}s)")

            print(f"[controller] compressing unified container...")
            t0 = time.time()
            with tarfile.open(tarball, mode='w:gz') as t:
                t.add(out, arcname='cde-package')
            tarball_mb = os.path.getsize(tarball) / 1_048_576
            print(f"[controller] compress done ({tarball_mb:.1f} MB, {time.time() - t0:.2f}s)")

            shutil.rmtree(out, ignore_errors=True)
            for pkg_dir in pkg_dirs:
                shutil.rmtree(os.path.dirname(pkg_dir), ignore_errors=True)
            print(f"[controller] unified container → {tarball}")
            print(f"[controller] replay: sciunit-swarm repeat "
                  f"--controller-ip {self.host} "
                  f"--controller-port {self.port} {workflow_id}")
        else:
            print("[controller] no containers collected")

    # ------------------------------------------------------------------
    # Registry server
    # ------------------------------------------------------------------

    def _registry_server(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('', self.port))
        srv.listen(50)
        srv.settimeout(1)
        while not self.stop_event.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(
                target=self._handle_connect, args=(conn,), daemon=True
            ).start()
        srv.close()

    def _handle_connect(self, conn):
        try:
            msg = recv_json(conn)
            if msg['type'] == 'REGISTER':
                mode = msg.get('mode', 'exec')
                if mode == 'repeat':
                    self._handle_repeat_register(conn, msg)
                else:
                    with self.workers_lock:
                        self.workers[msg['node_id']] = (msg['host'], msg['port'])
                    print(f"[controller] worker {msg['node_id']} registered "
                          f"@ {msg['host']}:{msg['port']}")
                    conn.close()
        except Exception as e:
            print(f"[controller] connection error: {e}")
            conn.close()

    def _handle_repeat_register(self, conn, msg):
        workflow_id = msg.get('workflow_id')
        node_id     = msg['node_id']
        host        = msg['host']
        port        = msg['port']
        print(f"[controller] repeat worker {node_id} registered (workflow={workflow_id})")

        tarball = os.path.join(self.output_dir, workflow_id, 'unified.tar.gz')
        if not os.path.isfile(tarball):
            print(f"[controller] workflow {workflow_id} not found")
            conn.close()
            return

        with open(tarball, 'rb') as f:
            payload = f.read()

        t0 = time.time()
        send_json(conn, {'type': 'CONTAINER', 'size': len(payload)})
        conn.sendall(payload)
        conn.close()
        elapsed = time.time() - t0
        print(f"[controller] sent container to repeat worker {node_id} "
              f"({len(payload)} B, {elapsed:.2f}s)")

        with self.workers_lock:
            self.repeat_workers[node_id] = (host, port)

    # ------------------------------------------------------------------
    # Manager
    # ------------------------------------------------------------------

    def _run_manager(self):
        env = os.environ.copy()
        env['MANAGER_HOST'] = self.host
        proc = subprocess.run(self.manager_cmd, shell=True, env=env)
        print(f"[controller] manager exited (code {proc.returncode})")

    # ------------------------------------------------------------------
    # Kill + collect
    # ------------------------------------------------------------------

    def _kill_and_collect(self, workflow_id):
        os.makedirs(self.output_dir, exist_ok=True)
        pkg_dirs = []
        pkg_dirs_lock = threading.Lock()

        with self.workers_lock:
            snapshot = dict(self.workers)

        if snapshot:
            collect_srv = socket.socket()
            collect_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            collect_srv.bind(('', 0))
            collect_srv.listen(len(snapshot))
            collect_srv.settimeout(300)
            collect_port = collect_srv.getsockname()[1]

            def send_kill(node_id, host, port):
                print(f"[controller] killing exec worker {node_id}")
                try:
                    conn = socket.create_connection((host, port), timeout=10)
                    send_json(conn, {
                        'type':         'KILL',
                        'collect_host': self.host,
                        'collect_port': collect_port,
                    })
                    conn.close()
                except Exception as e:
                    print(f"[controller] ERROR sending KILL to {node_id}: {e}")

            t_collect_start = time.time()
            kill_threads = [
                threading.Thread(target=send_kill, args=(nid, h, p))
                for nid, (h, p) in snapshot.items()
            ]
            for t in kill_threads:
                t.start()
            for t in kill_threads:
                t.join()

            def accept_container():
                try:
                    conn, _ = collect_srv.accept()
                    conn.settimeout(None)
                    t0 = time.time()
                    msg = recv_json(conn)
                    assert msg['type'] == 'CONTAINER'
                    node_id = msg['node_id']
                    payload = recv_exact(conn, msg['size'])
                    conn.close()
                    elapsed = time.time() - t0
                    print(f"[controller] received container from {node_id} "
                          f"({len(payload)} B, {elapsed:.2f}s)")

                    dest = os.path.join(self.output_dir, workflow_id, f'worker_{node_id}')
                    os.makedirs(dest, exist_ok=True)
                    with tarfile.open(fileobj=io.BytesIO(payload), mode='r:') as t:
                        t.extractall(dest)

                    pkg_dir = os.path.join(dest, 'cde-package')
                    with pkg_dirs_lock:
                        pkg_dirs.append(pkg_dir)
                except Exception as e:
                    print(f"[controller] ERROR receiving container: {e}")

            accept_threads = [
                threading.Thread(target=accept_container)
                for _ in snapshot
            ]
            for t in accept_threads:
                t.start()
            for t in accept_threads:
                t.join()
            collect_srv.close()
            print(f"[controller] all containers collected "
                  f"({len(pkg_dirs)}/{len(snapshot)} workers, "
                  f"{time.time() - t_collect_start:.2f}s total)")

        with self.workers_lock:
            repeat_snapshot = dict(self.repeat_workers)

        def kill_repeat(node_id, host, port):
            print(f"[controller] killing repeat worker {node_id}")
            try:
                conn = socket.create_connection((host, port), timeout=10)
                send_json(conn, {'type': 'KILL'})
                conn.close()
            except Exception as e:
                print(f"[controller] ERROR killing repeat worker {node_id}: {e}")

        threads = [
            threading.Thread(target=kill_repeat, args=(nid, h, p))
            for nid, (h, p) in repeat_snapshot.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return pkg_dirs
