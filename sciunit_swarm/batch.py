"""
Batch submission backends for sciunit-swarm.
"""

import os
import shlex
import subprocess
import tempfile


def submit_slurm(subcommand, worker_args, batch_options):
    """
    Generate a minimal SLURM job script and submit via sbatch.

    subcommand   : 'exec' or 'repeat'
    worker_args  : list of CLI args for the subcommand (no --batch-* flags)
    batch_options: string of extra sbatch options, e.g. "--nodes=3 --time=01:00:00"
    """
    args_str = ' '.join(shlex.quote(a) for a in worker_args)
    script = f"""\
#!/bin/bash
#SBATCH --job-name=swarm-{subcommand}
#SBATCH --output=swarm-{subcommand}-%j-%t.log

if [ -n "$CONDA_ENV" ]; then
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV"
fi

echo "[slurm] {subcommand} job starting: nodes=$SLURM_JOB_NUM_NODES tasks=$SLURM_NTASKS"
srun sciunit-swarm {subcommand} {args_str}
"""
    fd, job_file = tempfile.mkstemp(suffix='.job', prefix='swarm_')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(script)
        batch_opts = shlex.split(batch_options) if batch_options else []
        cmd = ['sbatch'] + batch_opts + [job_file]
        print(f"[batch] submitting: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"[batch] {result.stdout.strip()}")
    finally:
        os.unlink(job_file)
