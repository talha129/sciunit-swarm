#!/usr/bin/env python3
"""
sciunit-swarm — distributed workflow capture and replay.

Usage:
  sciunit-swarm controller run --manager <cmd> [--port <port>] [--output <dir>]
  sciunit-swarm exec --controller-ip <ip> --controller-port <port> <worker-cmd>
  sciunit-swarm repeat --controller-ip <ip> --controller-port <port> <workflow-id> [args...]
  sciunit-swarm push <workflow-id> [--output <dir>]
  sciunit-swarm pull <url> [--output <dir>]
"""

import argparse
import sys


def cmd_controller(args):
    from sciunit_swarm.controller import Controller
    c = Controller(
        manager_cmd=args.manager,
        port=args.port,
        output_dir=args.output,
    )
    c.run()


def cmd_exec(args):
    if args.batch_type == 'slurm':
        from sciunit_swarm.batch import submit_slurm
        worker_args = ['--controller-ip', args.controller_ip,
                       '--controller-port', str(args.controller_port)]
        if args.work_dir:
            worker_args += ['--work-dir', args.work_dir]
        worker_args.append(args.worker_cmd)
        submit_slurm('exec', worker_args, args.batch_options or '')
    else:
        from sciunit_swarm.worker import WorkerAgent
        WorkerAgent(
            controller_ip=args.controller_ip,
            controller_port=args.controller_port,
            worker_cmd=args.worker_cmd,
            work_dir=args.work_dir,
            mode='exec',
        ).run()


def cmd_repeat(args):
    if args.batch_type == 'slurm':
        from sciunit_swarm.batch import submit_slurm
        worker_args = ['--controller-ip', args.controller_ip,
                       '--controller-port', str(args.controller_port)]
        if args.work_dir:
            worker_args += ['--work-dir', args.work_dir]
        worker_args.append(args.workflow_id)
        if args.repeat_args:
            worker_args += args.repeat_args
        submit_slurm('repeat', worker_args, args.batch_options or '')
    else:
        from sciunit_swarm.worker import WorkerAgent
        WorkerAgent(
            controller_ip=args.controller_ip,
            controller_port=args.controller_port,
            workflow_id=args.workflow_id,
            work_dir=args.work_dir,
            mode='repeat',
            repeat_args=args.repeat_args or None,
        ).run()


def cmd_push(args):
    import os
    from sciunit_swarm.s3 import push
    tarball = os.path.join(args.output, args.workflow_id, 'unified.tar.gz')
    if not os.path.isfile(tarball):
        print(f'[push] error: {tarball} not found', file=sys.stderr)
        sys.exit(1)
    print(f'[push] uploading {tarball}...')
    url = push(tarball, args.workflow_id)
    print(f'[push] url: {url}')


def cmd_pull(args):
    import os
    from sciunit_swarm.s3 import pull
    print(f'[pull] downloading {args.url}...')
    workflow_id = pull(args.url, args.output)
    dest = os.path.join(args.output, workflow_id, 'unified.tar.gz')
    print(f'[pull] workflow_id: {workflow_id}')
    print(f'[pull] stored in:   {dest}')
    print(f'[pull] repeat with:')
    print(f'  sciunit-swarm repeat '
          f'--controller-ip <host> --controller-port <port> '
          f'{workflow_id} [new-args...]')


def main():
    parser = argparse.ArgumentParser(prog='sciunit-swarm')
    sub = parser.add_subparsers(dest='command')

    # --- controller ---
    ctl = sub.add_parser('controller', help='Run on head node')
    ctl_sub = ctl.add_subparsers(dest='ctl_command')
    ctl_run = ctl_sub.add_parser('run', help='Launch manager and manage worker lifecycle')
    ctl_run.add_argument('--manager', required=True, metavar='CMD',
                         help='Manager command (e.g. "python3 manager.py")')
    ctl_run.add_argument('--port', type=int, default=9000, metavar='PORT',
                         help='Controller listen port (default: 9000)')
    ctl_run.add_argument('--output', default='./swarm-packages', metavar='DIR',
                         help='Directory to store captured packages (default: ./swarm-packages)')
    ctl_run.set_defaults(func=cmd_controller)

    # --- exec ---
    exc = sub.add_parser('exec', help='Run on compute node: capture worker under PTU')
    exc.add_argument('--controller-ip',   required=True)
    exc.add_argument('--controller-port', required=True, type=int)
    exc.add_argument('--work-dir', default=None, metavar='DIR',
                     help='Directory for PTU capture (default: auto temp dir, cleaned after)')
    exc.add_argument('--batch-type', default=None, choices=['slurm'], metavar='TYPE',
                     help='Submit workers via batch scheduler (slurm)')
    exc.add_argument('--batch-options', default=None, metavar='OPTS',
                     help='Extra options passed to sbatch, e.g. "--nodes=3 --time=01:00:00"')
    exc.add_argument('worker_cmd', metavar='WORKER_CMD',
                     help='Worker command; supports {manager_host} template')
    exc.set_defaults(func=cmd_exec)

    # --- repeat ---
    rep = sub.add_parser('repeat', help='Run on compute node: replay captured workflow')
    rep.add_argument('--controller-ip',   required=True)
    rep.add_argument('--controller-port', required=True, type=int)
    rep.add_argument('--work-dir', default=None, metavar='DIR',
                     help='Directory to extract container (default: auto temp dir, cleaned after)')
    rep.add_argument('--batch-type', default=None, choices=['slurm'], metavar='TYPE',
                     help='Submit workers via batch scheduler (slurm)')
    rep.add_argument('--batch-options', default=None, metavar='OPTS',
                     help='Extra options passed to sbatch, e.g. "--nodes=3 --time=01:00:00"')
    rep.add_argument('workflow_id', metavar='WORKFLOW_ID')
    rep.add_argument('repeat_args', nargs='*', metavar='ARG',
                     help='New args to substitute (keeps executable, replaces rest)')
    rep.set_defaults(func=cmd_repeat)

    # --- push ---
    psh = sub.add_parser('push', help='Upload unified container to S3, print URL')
    psh.add_argument('workflow_id', metavar='WORKFLOW_ID')
    psh.add_argument('--output', default='./swarm-packages', metavar='DIR',
                     help='Directory where packages are stored (default: ./swarm-packages)')
    psh.set_defaults(func=cmd_push)

    # --- pull ---
    pll = sub.add_parser('pull', help='Download unified container from S3 URL')
    pll.add_argument('url', metavar='URL')
    pll.add_argument('--output', default='./swarm-packages', metavar='DIR',
                     help='Directory to store downloaded package (default: ./swarm-packages)')
    pll.set_defaults(func=cmd_pull)

    args = parser.parse_args()
    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == '__main__':
    main()
