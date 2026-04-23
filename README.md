# sciunit-swarm

Distributed workflow capture and replay for HPC clusters. Runs worker processes under PTU (provenance-to-use) capture, merges per-worker containers into a unified reproducible package, and replays the workflow on any compatible node.

## Architecture

```
Head node                        Compute nodes
─────────────────────            ─────────────────────
sciunit-swarm controller  ←────  sciunit-swarm exec (workers)
  └─ runs manager (e.g. TaskVine)     └─ PTU captures vine_worker
  └─ collects containers              └─ sends raw container to controller
  └─ builds unified package
```

**Exec phase** — workers run under PTU capture, send containers to controller on KILL.  
**Repeat phase** — controller distributes unified container to workers; each replays via `cde.log`.

## Installation

```bash
git clone <repo>
cd sciunit-swarm
pip install -e .
```

Requires `sciunit2` (provides the PTU binary).

## Usage

### Single-node (everything on head node)

**Terminal 1 — controller:**
```bash
sciunit-swarm controller run \
  --manager "python3 manager.py" \
  --port 9000 \
  --output ./swarm-packages
```

**Terminal 2 — workers:**
```bash
sciunit-swarm exec \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  "vine_worker {manager_host} 9123"
```

`{manager_host}` is substituted at runtime with the controller's hostname.

### Multi-node with SLURM

**Terminal 1 — controller on head node:**
```bash
sciunit-swarm controller run \
  --manager "python3 manager.py" \
  --port 9000 \
  --output ./swarm-packages
```

**Terminal 2 — submit workers to SLURM:**
```bash
sciunit-swarm exec \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  --batch-type slurm \
  --batch-options "--nodes=4 --ntasks-per-node=2 --time=01:00:00 --export=ALL,CONDA_ENV=my_env" \
  "vine_worker {manager_host} 9123"
```

- `--batch-type slurm` generates a job script and submits via `sbatch`
- `--batch-options` passes arguments directly to `sbatch`
- Use `--oversubscribe` in `--batch-options` if requesting more tasks than CPUs per node
- Set `CONDA_ENV` in `--export` to activate a conda environment on compute nodes

### Replay (repeat phase)

After exec completes, the controller prints a replay command:

```
[controller] replay: sciunit-swarm repeat --controller-ip <host> --controller-port 9000 <workflow-id>
```

**Terminal 1 — controller (serves the unified container):**
```bash
sciunit-swarm controller run \
  --manager "python3 manager.py" \
  --port 9000 \
  --output ./swarm-packages
```

**Terminal 2 — repeat workers (single-node, same cluster):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  <workflow-id>
```

**Terminal 2 — repeat workers (single-node, new cluster with different manager host/port):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  <workflow-id> <new-manager-host> <new-manager-port>
```

The trailing arguments replace the original worker arguments (same executable, new args — following sciunit's substitution pattern). For example, if the original exec used `vine_worker old-host 9123`, providing `new-host 9123` replays as `vine_worker new-host 9123`.

**Terminal 2 — repeat workers (SLURM):**
```bash
sciunit-swarm repeat \
  --controller-ip $(hostname) \
  --controller-port 9000 \
  --batch-type slurm \
  --batch-options "--nodes=4 --ntasks-per-node=2 --time=01:00:00 --export=ALL,CONDA_ENV=my_env" \
  <workflow-id> [new-manager-host new-manager-port]
```

## CLI Reference

```
sciunit-swarm controller run
  --manager CMD       Manager command to run (required)
  --port PORT         Controller listen port (default: 9000)
  --output DIR        Directory for captured packages (default: ./swarm-packages)

sciunit-swarm exec
  --controller-ip IP        Controller hostname/IP (required)
  --controller-port PORT    Controller port (required)
  --work-dir DIR            PTU capture directory (default: auto temp, cleaned after)
  --batch-type {slurm}      Submit via batch scheduler
  --batch-options OPTS      Options passed to sbatch
  WORKER_CMD                Command to run under PTU; supports {manager_host} template

sciunit-swarm repeat
  --controller-ip IP        Controller hostname/IP (required)
  --controller-port PORT    Controller port (required)
  --work-dir DIR            Container extraction directory (default: auto temp, cleaned after)
  --batch-type {slurm}      Submit via batch scheduler
  --batch-options OPTS      Options passed to sbatch
  WORKFLOW_ID               ID printed by controller after exec phase
  [ARG ...]                 New args to substitute (keeps executable, replaces rest)

sciunit-swarm push
  WORKFLOW_ID               Workflow to upload
  --output DIR              Directory where packages are stored (default: ./swarm-packages)

sciunit-swarm pull
  URL                       CloudFront URL printed by push
  --output DIR              Directory to store downloaded package (default: ./swarm-packages)
```

## Output Layout

```
swarm-packages/
└── <workflow-id>/
    └── unified.tar.gz    # unified container for replay
```

Individual worker packages and intermediate merge artifacts are cleaned up automatically.

## Example

See `examples/montecarlo_manager.py` — Monte Carlo pi estimation using TaskVine PythonTask.

```bash
# Exec
sciunit-swarm controller run \
  --manager "python3 examples/montecarlo_manager.py --tasks 20 --samples 1000000" \
  --port 9000

# (in another terminal, on compute nodes or via SLURM)
sciunit-swarm exec --controller-ip <head-node> --controller-port 9000 \
  "vine_worker {manager_host} 9123"

# Repeat
sciunit-swarm controller run \
  --manager "python3 examples/montecarlo_manager.py --tasks 20 --samples 1000000" \
  --port 9000

# Repeat on same cluster
sciunit-swarm repeat --controller-ip <head-node> --controller-port 9000 <workflow-id>

# Repeat on new cluster (substitute manager host/port)
sciunit-swarm repeat --controller-ip <new-head-node> --controller-port 9000 <workflow-id> <new-manager-host> <new-manager-port>
```

## Sharing Workflows via S3

After exec completes, push the unified container to S3 and share the URL with collaborators:

```bash
# Push to S3 (uses sciunit's credentials and bucket)
sciunit-swarm push <workflow-id>
# prints: [push] url: https://d3okuktvxs1y4w.cloudfront.net/projects/...

# On another machine: pull and prepare for replay
sciunit-swarm pull https://d3okuktvxs1y4w.cloudfront.net/projects/.../unified.tar.gz
# prints: [pull] workflow_id: <workflow-id>
# prints: [pull] repeat with: sciunit-swarm repeat --controller-ip <host> --controller-port <port> <workflow-id> [new-args...]

# Then run the controller and repeat workers as usual
sciunit-swarm repeat --controller-ip <host> --controller-port 9000 <workflow-id>
```

The workflow is stored under `projects/{timestamp}/{workflow_id}/unified.tar.gz` in the same S3 bucket as sciunit, so the same lifecycle policies apply. The original `workflow-id` is preserved through push/pull.

## Notes

- Workers retry connecting to controller for 120 seconds on startup — safe to start workers before controller
- Workers on the same node must run in separate working directories (handled automatically via temp dirs)
- Container transfer between workers and controller is uncompressed for speed; controller compresses the unified package once
- SLURM jobs inherit the submitting shell's environment; use `--export=ALL,CONDA_ENV=<env>` to activate conda on compute nodes
