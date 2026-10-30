"""
Phase 6 verification — stranger clone test.

Checks that everything a stranger would need to run the project from scratch is
present and structurally sound. Does NOT require a running database or Docker.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def check(name: str, ok: bool, detail: str = "") -> bool:
    icon = PASS if ok else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{icon}] {name}{suffix}")
    return ok


def run() -> int:
    print("\n=== Phase 6 verification — stranger clone test ===\n")
    failures = 0

    # ── Required files ────────────────────────────────────────────────────────
    print("Required files:")
    required = [
        "Dockerfile",
        "docker-compose.yml",
        ".env.example",
        "requirements.txt",
        "requirements-extra.txt",
        "README.md",
        "LICENSE",
        "dashboard/Dockerfile",
        "dashboard/app.py",
        "src/etl/pipeline.py",
        "src/etl/config.py",
        "src/spark/daily_revenue.py",
        "streaming/producer.py",
        "streaming/consumer.py",
        "scripts/make_sample_data.py",
        "scripts/create_topic.py",
        "dags/ecom_pipeline_dag.py",
        ".github/workflows/ci.yml",
        "docs/verification/SUMMARY.md",
        "docs/RESULTS.md",
    ]
    for rel in required:
        ok = (ROOT / rel).exists()
        if not check(rel, ok):
            failures += 1

    # ── .env.example has all required keys ────────────────────────────────────
    print("\n.env.example completeness:")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    for key in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "POSTGRES_HOST", "POSTGRES_PORT"]:
        ok = key in env_example
        if not check(key, ok):
            failures += 1

    # ── Dockerfile parses (python syntax check not applicable, check for FROM) ─
    print("\nDockerfile sanity:")
    df_text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    ok = "FROM python:3.11" in df_text
    if not check("FROM python:3.11 present", ok):
        failures += 1
    ok = "CMD" in df_text
    if not check("CMD instruction present", ok):
        failures += 1

    # ── docker-compose.yml has all services ───────────────────────────────────
    print("\ndocker-compose.yml services:")
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    for svc in ["postgres", "kafka", "etl", "dashboard"]:
        ok = svc + ":" in compose_text
        if not check(f"service: {svc}", ok):
            failures += 1

    # ── Python syntax check on key modules ────────────────────────────────────
    print("\nPython syntax check:")
    py_files = [
        "src/etl/pipeline.py",
        "src/etl/config.py",
        "src/etl/extract.py",
        "src/etl/transform.py",
        "src/etl/validate.py",
        "src/etl/load.py",
        "src/etl/watermark.py",
        "src/etl/run_sql.py",
        "src/spark/daily_revenue.py",
        "streaming/producer.py",
        "streaming/consumer.py",
        "dashboard/app.py",
        "scripts/make_sample_data.py",
        "scripts/create_topic.py",
    ]
    for rel in py_files:
        path = ROOT / rel
        if not path.exists():
            if not check(rel, False, "file missing"):
                failures += 1
            continue
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(path)],
            check=False,
            capture_output=True,
        )
        ok = result.returncode == 0
        detail = result.stderr.decode().strip() if not ok else ""
        if not check(rel, ok, detail):
            failures += 1

    # ── README has key sections ────────────────────────────────────────────────
    print("\nREADME sections:")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for section in ["Quick start", "Tech stack", "Directory layout", "Docker usage", "Testing"]:
        ok = section in readme
        if not check(section, ok):
            failures += 1

    # ── Git tags ──────────────────────────────────────────────────────────────
    print("\nGit tags:")
    result = subprocess.run(["git", "tag"], check=False, capture_output=True, text=True, cwd=str(ROOT))
    tags = result.stdout.split()
    for tag in ["v0.1-phase1", "v0.2-phase2", "v0.3-phase3", "v0.4-phase4", "v0.5-phase5"]:
        ok = tag in tags
        if not check(tag, ok):
            failures += 1

    # ── Final ─────────────────────────────────────────────────────────────────
    print()
    if failures == 0:
        print(f"  [{PASS}] All checks passed — project is stranger-clone-ready.\n")
    else:
        print(f"  [{FAIL}] {failures} check(s) failed — fix before shipping.\n")

    return failures


if __name__ == "__main__":
    sys.exit(run())
