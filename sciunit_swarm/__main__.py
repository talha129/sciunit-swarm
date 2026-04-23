#!/usr/bin/env python3
"""
sciunit-swarm — distributed workflow capture and replay.

Usage:
  sciunit-swarm controller run --manager <cmd> [--port <port>] [--output <dir>]
  sciunit-swarm exec --controller-ip <ip> --controller-port <port> <worker-cmd>
  sciunit-swarm repeat --controller-ip <ip> --controller-port <port> <workflow-id>
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
        submit_slurm('repeat', worker_args, args.batch_options or '')
    else:
        from sciunit_swarm.worker import WorkerAgent
        WorkerAgent(
            controller_ip=args.controller_ip,
            controller_port=args.controller_port,
            workflow_id=args.workflow_id,
            work_dir=args.work_dir,
            mode='repeat',
        ).run()


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
    rep.set_defaults(func=cmd_repeat)

    args = parser.parse_args()
    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == '__main__':
    main()
