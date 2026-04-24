# sciunit-swarm

Distributed workflow capture and replay for HPC clusters. Captures the full execution environment of a distributed workflow — both manager and workers — into reproducible containers that can be replayed on any compatible node.

## Architecture

```
Head node                          Compute nodes
──────────────────────────         ─────────────────────
sciunit-swarm controller exec      sciunit-swarm exec (workers)
  └─ PTU captures manager            └─ PTU captures vine_worker
  └─ collects worker containers      └─ sends raw container to controller
  └─ builds manager.tar.gz
  └─ builds worker.tar.gz
```

**Exec phase** — manager and workers both run under PTU capture. Workers send containers to controller on exit. Controller merges into two containers.

**Repeat phase** — controller replays captured manager; workers replay from merged container.

## Output

```
swarm-packages/
└── <workflow-id>/
    ├── manager.tar.gz    # captured manager environment
    └── worker.tar.gz     # merged worker environment (all workers unified)
```

## Installation

```bash
git clone <repo>
cd sciunit-swarm
pip install -e .
```

Requires `sciunit2` (provides the PTU binary).

## Usage

### Exec Phase

**Terminal 1 — controller (head node):**
```bash
sciunit-swarm controller exec \
  --manager "python3 examples/montecarlo_manager.py --tasks 20 --samples 1000000" \
  --port 9000 \
  --output ./swarm-packages
```

**Terminal 2 — workers (single-node):**
```bash
sciunit-swarm exec \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  "vine_worker {manager_host} 9123"
```

**Terminal 2 — workers (SLURM):**
```bash
sciunit-swarm exec \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  --batch-type slurm \
  --batch-options "--nodes=4 --ntasks-per-node=2 --time=01:00:00 --export=ALL,CONDA_ENV=my_env" \
  "vine_worker {manager_host} 9123"
```

`{manager_host}` is substituted at runtime with the controller's hostname.

After exec completes the controller prints:
```
[controller] replay:
  sciunit-swarm controller repeat <workflow-id> --port 9000 --output ./swarm-packages
  sciunit-swarm repeat --controller-ip <host> --controller-port 9000 <workflow-id>
```

### Repeat Phase

**Terminal 1 — controller repeat (head node):**
```bash
sciunit-swarm controller repeat <workflow-id> \
  --port 9000 \
  --output ./swarm-packages
```

Replays the captured manager from `manager.tar.gz` via `cde-exec`.

**Terminal 2 — workers repeat (single-node):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  <workflow-id>
```

**Terminal 2 — workers repeat (new cluster, substitute args):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  <workflow-id> <new-manager-host> <new-manager-port>
```

Trailing args replace the original worker arguments (keeps executable, replaces rest). For example if original exec used `vine_worker old-host 9123`, providing `new-host 9123` replays as `vine_worker new-host 9123`. Same substitution applies to the manager via `controller repeat [new-manager-args...]`.

**Terminal 2 — workers repeat (SLURM):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  --batch-type slurm \
  --batch-options "--nodes=4 --ntasks-per-node=2 --time=01:00:00 --export=ALL,CONDA_ENV=my_env" \
  <workflow-id> [new-args...]
```

### Sharing Workflows via S3

```bash
# Push both containers as a single workflow archive
sciunit-swarm push <workflow-id> --output ./swarm-packages
# prints: [push] base url: https://d3okuktvxs1y4w.cloudfront.net/projects/.../workflow.tar.gz

# On another machine: pull and extract both containers
sciunit-swarm pull https://d3okuktvxs1y4w.cloudfront.net/projects/.../workflow.tar.gz
# prints: workflow_id, paths, and ready-to-use replay commands for both controller and workers
```

Uses sciunit's S3 bucket and credentials. Stored under `projects/` prefix so the same lifecycle policies apply.

## CLI Reference

```
sciunit-swarm controller exec
  --manager CMD       Manager command to capture (required)
  --port PORT         Controller listen port (default: 9000)
  --output DIR        Directory to store packages (default: ./swarm-packages)

sciunit-swarm controller repeat
  WORKFLOW_ID         Workflow captured during exec
  --port PORT         Controller listen port (default: 9000)
  --output DIR        Directory where packages are stored (default: ./swarm-packages)
  [ARG ...]           New args to substitute into manager command

sciunit-swarm exec
  --controller-ip IP        Controller hostname/IP (required)
  --controller-port PORT    Controller port (required)
  --work-dir DIR            PTU capture directory (default: auto temp, cleaned after)
  --batch-type {slurm}      Submit via batch scheduler
  --batch-options OPTS      Options passed to sbatch
  WORKER_CMD                Command to capture under PTU; supports {manager_host} template

sciunit-swarm repeat
  --controller-ip IP        Controller hostname/IP (required)
  --controller-port PORT    Controller port (required)
  --work-dir DIR            Container extraction directory (default: auto temp, cleaned after)
  --batch-type {slurm}      Submit via batch scheduler
  --batch-options OPTS      Options passed to sbatch
  WORKFLOW_ID               Workflow ID printed by controller after exec
  [ARG ...]                 New args to substitute (keeps executable, replaces rest)

sciunit-swarm push
  WORKFLOW_ID               Workflow to upload
  --output DIR              Directory where packages are stored (default: ./swarm-packages)

sciunit-swarm pull
  URL                       CloudFront URL printed by push
  --output DIR              Directory to store downloaded packages (default: ./swarm-packages)
```

## Notes

- Workers retry connecting to controller for 120 seconds on startup — safe to start workers before controller
- Workers on the same node run in separate temp directories automatically (avoids PTU `cde.options` race)
- Worker containers are transferred uncompressed for speed; controller compresses once
- SLURM jobs inherit the submitting shell's environment; use `--export=ALL,CONDA_ENV=<env>` to activate conda on compute nodes
- Use `--oversubscribe` in `--batch-options` if requesting more tasks than CPUs per node
- `--output` must be consistent across `controller exec`, `push`, and `controller repeat` — use an absolute path to avoid confusion
