#!/usr/bin/env python3
"""
WorkerAgent — runs on compute node.

exec mode:
  - REGISTER with controller
  - Run worker command under PTU capture
  - Wait for KILL signal → send cde-package tarball → exit

repeat mode:
  - REGISTER(workflow_id) with controller
  - Receive unified container tarball from controller
  - Extract and run via cde.log (replay)
  - Wait for KILL signal → exit
"""

import io
import os
import select
import shlex
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import time
import uuid

import sciunit2.libexec
from sciunit_swarm.protocol import send_json, recv_json, recv_exact


class WorkerAgent:
    def __init__(self, controller_ip, controller_port,
                 worker_cmd=None, workflow_id=None, mode='exec',
                 work_dir=None, listen_port=0):
        self.controller_ip   = controller_ip
        self.controller_port = controller_port
        self.worker_cmd      = worker_cmd
        self.workflow_id     = workflow_id
        self.mode            = mode
        self.work_dir        = work_dir
        self.ptu_bin         = sciunit2.libexec.ptu.which
        self.node_id         = str(uuid.uuid4())[:8]
        self.host            = socket.gethostname()
        self.listen_port     = listen_port

    def run(self):
        if self.mode == 'exec':
            self._exec_phase()
        else:
            self._repeat_phase()

    # ------------------------------------------------------------------
    # Exec phase
    # ------------------------------------------------------------------

    def _exec_phase(self):
        work_dir = self.work_dir or tempfile.mkdtemp(prefix='swarm_exec_')
        os.makedirs(work_dir, exist_ok=True)
        pkg_dir  = os.path.join(work_dir, 'cde-package')

        cmd = self.worker_cmd.format(
            manager_host=self.controller_ip,
            manager_port=os.environ.get('MANAGER_PORT', ''),
        )

        srv, port = self._start_listener()
        self._register(port, mode='exec')

        print(f"[worker {self.node_id}] exec: {cmd}")
        ptu_cmd = f"{self.ptu_bin} -o {pkg_dir} -- {cmd}"
        proc = subprocess.Popen(shlex.split(ptu_cmd), start_new_session=True, cwd=work_dir)

        self._wait_for_kill_and_send(srv, pkg_dir, proc)
        shutil.rmtree(work_dir, ignore_errors=True)
        print(f"[worker {self.node_id}] work dir cleaned up")

    # ------------------------------------------------------------------
    # Repeat phase
    # ------------------------------------------------------------------

    def _repeat_phase(self):
        work_dir = self.work_dir or tempfile.mkdtemp(prefix='swarm_repeat_')
        os.makedirs(work_dir, exist_ok=True)

        srv, port = self._start_listener()

        conn = self._connect_to_controller()
        conn.settimeout(None)
        send_json(conn, {
            'type':        'REGISTER',
            'node_id':     self.node_id,
            'host':        self.host,
            'port':        port,
            'workflow_id': self.workflow_id,
            'mode':        'repeat',
        })
        msg = recv_json(conn)
        assert msg['type'] == 'CONTAINER'
        payload = recv_exact(conn, msg['size'])
        conn.close()
        print(f"[worker {self.node_id}] container received ({len(payload)} B)")

        with tarfile.open(fileobj=io.BytesIO(payload), mode='r:gz') as t:
            t.extractall(work_dir)

        pkg_dir = os.path.join(work_dir, 'cde-package')
        proc = self._start_cde_log(pkg_dir)

        self._wait_for_kill(srv, proc)
        shutil.rmtree(work_dir, ignore_errors=True)
        print(f"[worker {self.node_id}] work dir cleaned up")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _connect_to_controller(self):
        deadline = time.time() + self.RECONNECT_TIMEOUT
        attempt  = 0
        while True:
            try:
                conn = socket.create_connection(
                    (self.controller_ip, self.controller_port), timeout=5)
                if attempt > 0:
                    print(f"[worker {self.node_id}] connected to controller")
                return conn
            except OSError:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise RuntimeError(
                        f"[worker {self.node_id}] could not reach controller "
                        f"after {self.RECONNECT_TIMEOUT}s, giving up")
                attempt += 1
                wait = min(5, remaining)
                print(f"[worker {self.node_id}] controller not available, "
                      f"retrying in {wait:.0f}s ({remaining:.0f}s remaining)...")
                time.sleep(wait)

    def _register(self, port, mode='exec'):
        conn = self._connect_to_controller()
        send_json(conn, {
            'type':    'REGISTER',
            'node_id': self.node_id,
            'host':    self.host,
            'port':    port,
            'mode':    mode,
        })
        conn.close()
        print(f"[worker {self.node_id}] registered with controller")

    def _start_listener(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('', self.listen_port))
        srv.listen(1)
        port = srv.getsockname()[1]
        return srv, port

    RECONNECT_TIMEOUT = 120  # seconds before giving up if no KILL and proc exits

    def _wait_for_kill_and_send(self, srv, pkg_dir, proc):
        print(f"[worker {self.node_id}] waiting for KILL signal...")
        kill_msg = self._wait_for_signal(srv, proc)

        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        proc.wait()
        print(f"[worker {self.node_id}] PTU stopped")
        srv.close()

        if kill_msg is None:
            return

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode='w:') as t:
            t.add(pkg_dir, arcname='cde-package')
        payload = buf.getvalue()

        collect_host = kill_msg['collect_host']
        collect_port = kill_msg['collect_port']
        conn = socket.create_connection((collect_host, collect_port), timeout=30)
        conn.settimeout(None)
        send_json(conn, {'type': 'CONTAINER', 'size': len(payload), 'node_id': self.node_id})
        conn.sendall(payload)
        conn.close()
        print(f"[worker {self.node_id}] container sent ({len(payload) // 1_048_576} MB)")

    def _start_cde_log(self, pkg_dir):
        cde_log = os.path.join(pkg_dir, 'cde.log')
        if not os.path.exists(cde_log):
            print(f"[worker {self.node_id}] no cde.log in {pkg_dir}")
            return None
        with open(cde_log) as f:
            cmd = f.read().strip()
        print(f"[worker {self.node_id}] replay: {cmd}")
        return subprocess.Popen(cmd, shell=True, cwd=pkg_dir, start_new_session=True)

    def _wait_for_kill(self, srv, proc):
        print(f"[worker {self.node_id}] waiting for KILL signal...")
        self._wait_for_signal(srv, proc)
        srv.close()

        if proc and proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait()
        print(f"[worker {self.node_id}] replay stopped")

    def _wait_for_signal(self, srv, proc):
        """
        Wait for KILL on srv, or for proc to exit, or for RECONNECT_TIMEOUT.
        Returns KILL message dict if signal received, None on timeout.
        """
        while True:
            ready, _, _ = select.select([srv], [], [], 5)
            if ready:
                conn, _ = srv.accept()
                msg = recv_json(conn)
                conn.close()
                assert msg['type'] == 'KILL'
                return msg
            if proc and proc.poll() is not None:
                print(f"[worker {self.node_id}] subprocess exited (code {proc.returncode}), "
                      f"waiting up to {self.RECONNECT_TIMEOUT}s for KILL...")
                ready, _, _ = select.select([srv], [], [], self.RECONNECT_TIMEOUT)
                if ready:
                    conn, _ = srv.accept()
                    msg = recv_json(conn)
                    conn.close()
                    assert msg['type'] == 'KILL'
                    return msg
                print(f"[worker {self.node_id}] timeout reached, exiting")
                return None
