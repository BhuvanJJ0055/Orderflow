"""Verification script for Phase 0: Preflight and repo setup."""

import os
import platform
import subprocess
import sys


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    """Execute a shell command and return returncode, stdout, stderr."""
    try:
        res = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except (subprocess.SubprocessError, OSError) as e:
        return -1, "", str(e)


def main() -> None:
    results: list[dict[str, str]] = []
    failed = False

    def record(check_name: str, passed: bool, evidence: str) -> None:
        nonlocal failed
        status = "PASS" if passed else "FAIL"
        if not passed:
            failed = True
        print(f"[{status}] {check_name}: {evidence}")
        results.append({"check": check_name, "status": status, "evidence": evidence})

    # 1. Git
    code, out, err = run_command(["git", "--version"])
    record("Git CLI", code == 0, out if code == 0 else err)

    # 2. Python
    py_ver = sys.version.split()[0]
    py_pass = sys.version_info >= (3, 10)
    record("Python 3.10+", py_pass, f"Python {py_ver}")

    # 3. Docker & Compose
    code_docker, _out_docker, err_docker = run_command(["docker", "info"])
    code_compose, out_compose, err_compose = run_command(["docker", "compose", "version"])
    docker_pass = (code_docker == 0) and (code_compose == 0)
    evidence_docker = f"Daemon running; {out_compose}" if docker_pass else f"Docker info err: {err_docker}; Compose err: {err_compose}"
    record("Docker & Compose", docker_pass, evidence_docker)

    # 4. Java
    code_java, out_java, err_java = run_command(["java", "-version"])
    java_out = out_java or err_java
    java_first = java_out.splitlines()[0] if java_out else "Java not found"
    record("Java (11 or 17)", code_java == 0, java_first)

    # 5. GitHub CLI (optional)
    code_gh, out_gh, _ = run_command(["gh", "--version"])
    record("GitHub CLI (optional)", True, out_gh.splitlines()[0] if code_gh == 0 else "Not installed (fallback to git merge)")

    # 6. Remote reachability
    code_remote, out_remote, err_remote = run_command(["git", "ls-remote", "origin"])
    record("Git remote reachable", code_remote == 0, out_remote.splitlines()[0] if code_remote == 0 else err_remote)

    # 7. .env not tracked
    _code_env, out_env, _ = run_command(["git", "ls-files", ".env"])
    record(".env not tracked", out_env == "", "Not tracked by git" if out_env == "" else f"Tracked: {out_env}")

    # 8. Raw CSV files check
    raw_files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_products_dataset.csv",
        "olist_order_payments_dataset.csv",
    ]
    all_files_ok = True
    file_evidence = []
    for f in raw_files:
        path = os.path.join("data", "raw", f)
        if os.path.exists(path):
            with open(path, encoding="utf-8", errors="ignore") as fp:
                line_count = sum(1 for _ in fp)
            file_evidence.append(f"{f} ({line_count} rows)")
            if line_count <= 1:
                all_files_ok = False
        else:
            all_files_ok = False
            file_evidence.append(f"{f} (MISSING)")

    record("Five raw CSV datasets exist", all_files_ok, "; ".join(file_evidence))

    # Write report
    os.makedirs("docs/verification", exist_ok=True)
    report_path = os.path.join("docs", "verification", "phase-0.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Phase 0: Preflight and Environment Verification\n\n")
        f.write(f"- **OS**: {platform.system()} {platform.release()} ({platform.version()})\n")
        f.write(f"- **CPU Count**: {os.cpu_count()}\n")
        f.write(f"- **Python Version**: {sys.version}\n\n")
        f.write("| Check | Result | Evidence |\n")
        f.write("| --- | --- | --- |\n")
        for r in results:
            clean_ev = r["evidence"].replace("|", "\\|").replace("\n", " ")
            f.write(f"| {r['check']} | {r['status']} | {clean_ev} |\n")

    print(f"\nVerification report generated at: {report_path}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
