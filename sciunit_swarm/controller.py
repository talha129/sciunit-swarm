#!/usr/bin/env python3
"""
Controller — runs on head node.

exec phase:
  - Launch manager under PTU capture
  - Accept REGISTER from workers (any count, any time)
  - When manager exits → KILL all registered workers
  - Collect CONTAINER tarballs → build worker.tar.gz
  - Tar manager capture → manager.tar.gz

repeat phase:
  - Accept REGISTER(workflow_id) from workers → push worker.tar.gz to each
  - Replay manager from manager.tar.gz via cde-exec
  - When manager exits → KILL all repeat workers
"""

import io
import json
import os
import shlex
import shutil
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid

import sciunit2.libexec
from sciunit_swarm.merge import build_unified_container
from sciunit_swarm.protocol import send_json, recv_json, recv_exact


class Controller:
    def __init__(self, port=9000, output_dir='./swarm-packages',
                 manager_cmd=None,
                 workflow_id=None, repeat_args=None):
        self.manager_cmd  = manager_cmd
        self.port         = port
        self.output_dir   = output_dir
        self.workflow_id  = workflow_id
        self.repeat_args  = repeat_args
        self.host         = socket.gethostname()
        self.ptu_bin      = sciunit2.libexec.ptu.which

        self.workers        = {}
        self.repeat_workers = {}
        self.workers_lock   = threading.Lock()
        self.stop_event     = threading.Event()

    # ------------------------------------------------------------------
    # Exec phase
    # ------------------------------------------------------------------

    def run(self):
        workflow_id = str(uuid.uuid4())[:8]
        print(f"[controller] workflow_id    = {workflow_id}")
        print(f"[controller] listening port = {self.port}")
        print(f"[controller] manager cmd    = {self.manager_cmd}")

        reg_thread = threading.Thread(target=self._registry_server, daemon=True)
        reg_thread.start()

        mgr_work_dir, mgr_pkg_dir = self._run_manager_captured()

        self.stop_event.set()
        pkg_dirs = self._kill_and_collect(workflow_id)

        os.makedirs(os.path.join(self.output_dir, workflow_id), exist_ok=True)

        if pkg_dirs:
            out            = os.path.join(self.output_dir, workflow_id, 'unified')
            worker_tarball = os.path.join(self.output_dir, workflow_id, 'worker.tar.gz')

            t0 = time.time()
            build_unified_container(pkg_dirs, out)
            print(f"[controller] merge done ({time.time() - t0:.2f}s)")

            t0 = time.time()
            with tarfile.open(worker_tarball, 'w:gz') as t:
                t.add(out, arcname='cde-package')
            mb = os.path.getsize(worker_tarball) / 1_048_576
            print(f"[controller] worker container → {worker_tarball} "
                  f"({mb:.1f} MB, {time.time() - t0:.2f}s)")

            shutil.rmtree(out, ignore_errors=True)
            for pkg_dir in pkg_dirs:
                shutil.rmtree(os.path.dirname(pkg_dir), ignore_errors=True)
        else:
            print("[controller] no worker containers collected")

        manager_tarball = os.path.join(self.output_dir, workflow_id, 'manager.tar.gz')
        t0 = time.time()
        with tarfile.open(manager_tarball, 'w:gz') as t:
            t.add(mgr_pkg_dir, arcname='cde-package')
        mb = os.path.getsize(manager_tarball) / 1_048_576
        print(f"[controller] manager container → {manager_tarball} "
              f"({mb:.1f} MB, {time.time() - t0:.2f}s)")
        shutil.rmtree(mgr_work_dir, ignore_errors=True)

        print(f"[controller] replay:")
        print(f"  sciunit-swarm controller repeat {workflow_id} "
              f"--port {self.port} --output {self.output_dir}")
        print(f"  sciunit-swarm repeat "
              f"--controller-ip {self.host} "
              f"--controller-port {self.port} {workflow_id}")

    # ------------------------------------------------------------------
    # Repeat phase
    # ------------------------------------------------------------------

    def repeat(self):
        workflow_id = self.workflow_id
        print(f"[controller] repeat workflow_id = {workflow_id}")
        print(f"[controller] listening port     = {self.port}")

        worker_tarball  = os.path.join(self.output_dir, workflow_id, 'worker.tar.gz')
        manager_tarball = os.path.join(self.output_dir, workflow_id, 'manager.tar.gz')

        for path, label in ((worker_tarball, 'worker'), (manager_tarball, 'manager')):
            if not os.path.isfile(path):
                print(f"[controller] {label}.tar.gz not found at {path}")
                return

        reg_thread = threading.Thread(target=self._registry_server, daemon=True)
        reg_thread.start()

        self._run_manager_replay()

        self.stop_event.set()
        self._kill_and_collect(workflow_id)

    # ------------------------------------------------------------------
    # Manager helpers
    # ------------------------------------------------------------------

    def _run_manager_captured(self):
        work_dir   = tempfile.mkdtemp(prefix='swarm_mgr_exec_')
        pkg_dir    = os.path.join(work_dir, 'cde-package')
        invoke_dir = os.getcwd()
        os.makedirs(pkg_dir, exist_ok=True)

        with open(os.path.join(pkg_dir, 'swarm.cmd'), 'w') as f:
            json.dump({'cmd': shlex.split(self.manager_cmd), 'cwd': invoke_dir}, f)

        env = os.environ.copy()
        env['MANAGER_HOST'] = self.host

        ptu_cmd = f"{self.ptu_bin} -o {pkg_dir} -- {self.manager_cmd}"
        print(f"[controller] manager (PTU): {ptu_cmd}")
        proc = subprocess.run(shlex.split(ptu_cmd), env=env, cwd=invoke_dir)
        print(f"[controller] manager exited (code {proc.returncode})")
        return work_dir, pkg_dir

    def _run_manager_replay(self):
        work_dir        = tempfile.mkdtemp(prefix='swarm_mgr_repeat_')
        manager_tarball = os.path.join(self.output_dir, self.workflow_id, 'manager.tar.gz')

        with tarfile.open(manager_tarball, 'r:gz') as t:
            t.extractall(work_dir)

        pkg_dir   = os.path.join(work_dir, 'cde-package')
        cde_exec  = os.path.join(pkg_dir, 'cde-exec')
        cde_root  = os.path.join(pkg_dir, 'cde-root')
        swarm_cmd = os.path.join(pkg_dir, 'swarm.cmd')

        if not os.path.exists(swarm_cmd):
            print(f"[controller] no swarm.cmd in manager package")
            shutil.rmtree(work_dir, ignore_errors=True)
            return
        if not os.path.exists(cde_exec):
            print(f"[controller] no cde-exec in manager package")
            shutil.rmtree(work_dir, ignore_errors=True)
            return

        with open(swarm_cmd) as f:
            data = json.load(f)

        if isinstance(data, dict):
            orig       = data['cmd']
            replay_cwd = data.get('cwd', cde_root)
        else:
            orig       = data          # backward compat: old format was bare list
            replay_cwd = cde_root

        args = orig[:1] + self.repeat_args if self.repeat_args else orig
        cmd  = [cde_exec, '-o', pkg_dir, '--'] + args

        env = os.environ.copy()
        env['MANAGER_HOST'] = self.host

        print(f"[controller] manager replay: {shlex.join(cmd)}")
        print(f"[controller] manager cwd:    {replay_cwd}")
        proc = subprocess.run(cmd, cwd=replay_cwd, env=env)
        print(f"[controller] manager exited (code {proc.returncode})")
        shutil.rmtree(work_dir, ignore_errors=True)

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

        tarball = os.path.join(self.output_dir, workflow_id, 'worker.tar.gz')
        if not os.path.isfile(tarball):
            print(f"[controller] worker.tar.gz not found for {workflow_id}")
            conn.close()
            return

        with open(tarball, 'rb') as f:
            payload = f.read()

        t0 = time.time()
        send_json(conn, {'type': 'CONTAINER', 'size': len(payload)})
        conn.sendall(payload)
        conn.close()
        print(f"[controller] sent worker container to {node_id} "
              f"({len(payload)} B, {time.time() - t0:.2f}s)")

        with self.workers_lock:
            self.repeat_workers[node_id] = (host, port)

    # ------------------------------------------------------------------
    # Kill + collect
    # ------------------------------------------------------------------

    def _kill_and_collect(self, workflow_id):
        os.makedirs(self.output_dir, exist_ok=True)
        pkg_dirs      = []
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
                    t0  = time.time()
                    msg = recv_json(conn)
                    assert msg['type'] == 'CONTAINER'
                    node_id = msg['node_id']
                    payload = recv_exact(conn, msg['size'])
                    conn.close()
                    print(f"[controller] received container from {node_id} "
                          f"({len(payload)} B, {time.time() - t0:.2f}s)")

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
