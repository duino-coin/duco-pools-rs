"""Validate four explicit mutations, then restore the normal disposable copy.
Run from any directory: python pool-verifier/check_sensitivity.py
Production sources are never edited. Requires a built verifier executable.
"""
from pathlib import Path
import argparse
import hashlib
import json
import os
import subprocess

home = Path(__file__).resolve().parent
root = home.parent
binary = home / 'target' / 'debug' / ('pool-verifier.exe' if os.name == 'nt' else 'pool-verifier')
checks = [('reward', 'internal/reward-vectors'), ('pending', 'sync/inflight-shares'),
          ('auth', 'lifecycle/dashboard-key-boundary'), ('release', 'connections/active-disconnect')]
# Hash every production input before invoking the verifier. The mutation mechanism
# must affect only work/source; this catches an accidental edit to the real tree.
files = [*root.joinpath('src').rglob('*.rs'), *(root / n for n in ['Cargo.toml', 'Cargo.lock', 'build.rs'])]
before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--only', choices=[m for m, _ in checks], action='append', help='Rerun selected mutations; preserve previously detected results.')
args = parser.parse_args()
summary = home / 'work/sensitivity.json'
results = json.loads(summary.read_text(encoding='utf-8')) if args.only and summary.exists() else []
if args.only:
    checks = [(m, c) for m, c in checks if m in args.only]
try:
    for mutation, case in checks:
        print(f'Checking sensitivity: {mutation} -> {case}', flush=True)
        status = subprocess.run([str(binary), '--mutation', mutation, '--case', case], cwd=root).returncode

        # A nonzero exit alone is insufficient: prove this run contained the
        # requested mutation and that its one designated assertion detected it.
        run = Path((home / 'work/latest-run.txt').read_text(encoding='utf-8'))
        metadata = json.loads((run / 'metadata.json').read_text(encoding='utf-8'))
        report = json.loads((run / 'report.json').read_text(encoding='utf-8'))
        report_path = str(run / 'report.json') if metadata.get('mutation') == mutation else None
        detected = (status == 1 and metadata['mutation'] == mutation and len(report) == 1
                    and report[0]['case'] == case and report[0]['status'] == 'FAIL')
        results = [r for r in results if r['mutation'] != mutation]
        results.append(dict(mutation=mutation, case=case, detected=detected, exit_code=status, report=report_path))
        (home / 'work/sensitivity.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
        if not detected:
            raise RuntimeError(f'Mutation not detected by an assertion: {mutation}')
finally:
    # Restoration is unconditional. Even a missed mutation or interrupted loop
    # rebuilds an unmodified disposable copy before source hashes are compared.
    print('Restoring normal copy and checking internal tests plus connection behavior', flush=True)
    restored = subprocess.run([str(binary), '--suite', 'internal', '--suite', 'connections'], cwd=root).returncode
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    if before != after:
        raise RuntimeError('Production source changed during sensitivity run')
    if restored:
        raise RuntimeError(f'Restored-copy verification failed: {restored}')
print('Selected mutations detected; normal copy restored; production hashes unchanged.', flush=True)
