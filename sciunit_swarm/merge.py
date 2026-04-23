"""
Merge multiple PTU cde-packages into a single unified container.
Adapted from build_unified_container.py.
"""

import glob
import os
import shutil


def build_unified_container(pkg_dirs, output_dir):
    if not pkg_dirs:
        return

    base_pkg = pkg_dirs[0]
    unified  = output_dir

    print(f"[merge] base    : {base_pkg}")
    print(f"[merge] output  : {unified}")

    if os.path.exists(unified):
        print("[merge] output exists — merging into it")
    else:
        shutil.copytree(base_pkg, unified, symlinks=True)
        print("[merge] base copied")

    unified_cde_root = os.path.join(unified, 'cde-root')

    for pkg in pkg_dirs[1:]:
        cde_root = os.path.join(pkg, 'cde-root')
        print(f"[merge] merging {pkg}")

        if not os.path.isdir(cde_root):
            print("  no cde-root, skipping")
            continue

        stats = {'added': 0, 'exists': 0}
        merged = set()

        prov_log = _find_provenance_log(pkg)
        if prov_log:
            for abs_path in sorted(_parse_accessed(prov_log)):
                src_path  = cde_root + abs_path
                dest_path = unified_cde_root + abs_path
                merged.add(dest_path)
                r = _merge_file(src_path, dest_path)
                stats['added' if r == 'added' else 'exists'] += 1

        for dirpath, _, fnames in os.walk(cde_root, followlinks=False):
            rel = os.path.relpath(dirpath, cde_root)
            for name in fnames:
                src  = os.path.join(dirpath, name)
                dest = os.path.join(unified_cde_root, rel, name)
                if dest in merged:
                    continue
                if os.path.lexists(dest):
                    stats['exists'] += 1
                    continue
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                if os.path.islink(src):
                    os.symlink(os.readlink(src), dest)
                else:
                    shutil.copy2(src, dest)
                stats['added'] += 1

        print(f"  added={stats['added']} exists={stats['exists']}")

    total = sum(1 for _, _, fs in os.walk(unified_cde_root) for _ in fs)
    print(f"[merge] total files in cde-root: {total}")


def _find_provenance_log(pkg_dir):
    matches = sorted(glob.glob(os.path.join(pkg_dir, 'provenance.cde-root.*.log')))
    if not matches:
        return None
    for m in matches:
        if m.endswith('.1.log'):
            return m
    return matches[0]


def _parse_accessed(prov_log_path):
    paths = set()
    with open(prov_log_path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            event = parts[2]
            if event in ('READ', 'READ-WRITE') and parts[3].startswith('/'):
                paths.add(parts[3])
            elif event == 'EXECVE' and len(parts) >= 5 and parts[4].startswith('/'):
                paths.add(parts[4])
    return paths


def _merge_file(src, dest):
    if not os.path.isfile(src):
        return 'missing_src'
    if os.path.exists(dest):
        return 'exists'
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src, dest)
    return 'added'
