import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_json(path: Path, obj) -> Path:
    path.write_text(json.dumps(obj, indent=2))
    return path


def make_config(tmp_path: Path, dag: list, *, concurrency=2, base_seconds=0.02,
                max_attempts=2) -> Path:
    dag_file = write_json(tmp_path / "dag.json", {"tasks": dag})
    return write_json(tmp_path / "config.json", {
        "state_dir": str(tmp_path / "state"),
        "concurrency": concurrency,
        "retry": {"base_seconds": base_seconds, "max_attempts": max_attempts},
        "dag_file": str(dag_file),
    })


def run_cli(*args, timeout=60):
    return subprocess.run(
        [sys.executable, "-m", "taskflow", *args],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout,
    )


def status(config: Path) -> dict:
    result = run_cli("status", "--config", str(config))
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def task_by_id(payload: dict, task_id: str) -> dict:
    return next(t for t in payload["tasks"] if t["id"] == task_id)


def make_script(tmp_path: Path, name: str, body: str) -> str:
    script = tmp_path / name
    script.write_text(body)
    return f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}"


def wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_crash_recovery_resumes_unfinished_and_never_reruns_succeeded(tmp_path):
    """kill -9 mid-run: finished task must not re-run, interrupted one must."""
    a_log = tmp_path / "a.log"
    b_log = tmp_path / "b.log"
    release = tmp_path / "release"

    a_cmd = make_script(tmp_path, "task_a.py",
                        f"open({str(a_log)!r}, 'a').write('A\\n')\n")
    b_cmd = make_script(tmp_path, "task_b.py", f"""\
import sys, time
open({str(b_log)!r}, 'a').write('started\\n')
deadline = time.time() + 30
while time.time() < deadline:
    if {str(release)!r} and __import__('os').path.exists({str(release)!r}):
        sys.exit(0)
    time.sleep(0.05)
sys.exit(1)
""")
    config = make_config(tmp_path, [
        {"id": "a", "run": a_cmd, "needs": []},
        {"id": "b", "run": b_cmd, "needs": ["a"]},
    ], max_attempts=3)

    # First run: let "a" finish and "b" start, then hard-kill the process.
    proc = subprocess.Popen(
        [sys.executable, "-m", "taskflow", "run", "--config", str(config)],
        cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert wait_for(lambda: a_log.exists() and b_log.exists()
                    and b_log.read_text().count("started") == 1)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)
    assert proc.returncode == -signal.SIGKILL
    assert a_log.read_text().count("A") == 1

    # Second run: "b" must be picked up again, "a" must not run twice.
    release.touch()
    result = run_cli("run", "--config", str(config))
    assert result.returncode == 0, result.stderr + result.stdout

    assert a_log.read_text().count("A") == 1, "succeeded task re-ran after crash"
    assert b_log.read_text().count("started") == 2, "interrupted task was not resumed"

    payload = status(config)
    assert task_by_id(payload, "a")["status"] == "succeeded"
    assert task_by_id(payload, "a")["attempts"] == 1
    assert task_by_id(payload, "b")["status"] == "succeeded"
    assert task_by_id(payload, "b")["attempts"] == 2  # killed attempt + resumed attempt
    for task in payload["tasks"]:
        assert task["started_at"] and task["finished_at"]


def test_idempotency_key_prevents_duplicate_execution(tmp_path):
    """Re-submitting the same idempotency key must not execute the task again."""
    count_log = tmp_path / "count.log"
    cmd = make_script(tmp_path, "count.py",
                      f"open({str(count_log)!r}, 'a').write('ran\\n')\n")

    dag1 = [{"id": "t1", "run": cmd, "needs": [], "idempotency_key": "order-123"}]
    config = make_config(tmp_path, dag1)
    assert run_cli("run", "--config", str(config)).returncode == 0
    assert count_log.read_text().count("ran") == 1

    # Same DAG submitted again: nothing re-runs.
    assert run_cli("run", "--config", str(config)).returncode == 0
    assert count_log.read_text().count("ran") == 1

    # A new submission with a different task id but the same idempotency key
    # is recognised as a duplicate and skipped.
    dag2 = [{"id": "t2", "run": cmd, "needs": [], "idempotency_key": "order-123"}]
    write_json(tmp_path / "dag.json", {"tasks": dag2})
    result = run_cli("run", "--config", str(config))
    assert result.returncode == 0, result.stderr
    assert count_log.read_text().count("ran") == 1, "duplicate idempotency key re-executed"

    payload = status(config)
    assert [t["id"] for t in payload["tasks"]] == ["t1"]
    assert task_by_id(payload, "t1")["attempts"] == 1


def test_dead_letter_blocks_downstream_and_manual_retry_replays(tmp_path):
    flag = tmp_path / "ok.flag"
    down_log = tmp_path / "down.log"
    gate_cmd = make_script(tmp_path, "gate.py",
                           f"import sys; sys.exit(0 if "
                           f"__import__('os').path.exists({str(flag)!r}) else 1)\n")
    down_cmd = make_script(tmp_path, "down.py",
                           f"open({str(down_log)!r}, 'a').write('down\\n')\n")
    config = make_config(tmp_path, [
        {"id": "bad", "run": gate_cmd, "needs": []},
        {"id": "down", "run": down_cmd, "needs": ["bad"]},
    ], max_attempts=2)

    result = run_cli("run", "--config", str(config))
    assert result.returncode != 0, "dead task must make run exit non-zero"

    payload = status(config)
    bad = task_by_id(payload, "bad")
    assert bad["status"] == "dead"
    assert bad["attempts"] == 2  # retried up to max_attempts
    assert task_by_id(payload, "down")["status"] == "blocked"
    assert not down_log.exists()

    # Manual replay of the dead letter, then the downstream unblocks.
    retry = run_cli("retry", "--config", str(config), "--task", "bad")
    assert retry.returncode == 0, retry.stderr
    assert task_by_id(status(config), "bad")["status"] == "pending"

    flag.touch()
    result = run_cli("run", "--config", str(config))
    assert result.returncode == 0, result.stderr + result.stdout
    payload = status(config)
    assert task_by_id(payload, "bad")["status"] == "succeeded"
    assert task_by_id(payload, "down")["status"] == "succeeded"
    assert down_log.read_text().count("down") == 1


def test_sigterm_finishes_running_task_and_stops_scheduling(tmp_path):
    slow_log = tmp_path / "slow.log"
    after_log = tmp_path / "after.log"
    slow_cmd = make_script(tmp_path, "slow.py", f"""\
import time
time.sleep(1.5)
open({str(slow_log)!r}, 'a').write('done\\n')
""")
    after_cmd = make_script(tmp_path, "after.py",
                            f"open({str(after_log)!r}, 'a').write('after\\n')\n")
    config = make_config(tmp_path, [
        {"id": "slow", "run": slow_cmd, "needs": []},
        {"id": "after", "run": after_cmd, "needs": ["slow"]},
    ], concurrency=2)

    proc = subprocess.Popen(
        [sys.executable, "-m", "taskflow", "run", "--config", str(config)],
        cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    state_file = tmp_path / "state" / "state.json"
    assert wait_for(lambda: state_file.exists() and any(
        t["status"] == "running" for t in json.loads(state_file.read_text())["tasks"].values()))
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=20)

    # The in-flight task ran to completion and state was persisted; the
    # pending task was never scheduled.
    assert slow_log.exists(), "SIGTERM killed the in-flight task"
    assert not after_log.exists(), "new task was scheduled after SIGTERM"
    payload = status(config)
    assert task_by_id(payload, "slow")["status"] == "succeeded"
    assert task_by_id(payload, "after")["status"] == "pending"

    # A later run picks up where things stopped.
    result = run_cli("run", "--config", str(config))
    assert result.returncode == 0
    assert after_log.exists()


def test_concurrency_limit_is_respected(tmp_path):
    marks = tmp_path / "marks.log"
    body = f"""\
import os, time
path = {str(marks)!r}
with open(path, 'a') as fh:
    fh.write('start\\n')
time.sleep(0.4)
with open(path, 'a') as fh:
    fh.write('end\\n')
"""
    cmd = make_script(tmp_path, "worker.py", body)
    config = make_config(tmp_path, [
        {"id": f"w{i}", "run": cmd, "needs": []} for i in range(3)
    ], concurrency=1)

    started = time.monotonic()
    result = run_cli("run", "--config", str(config))
    elapsed = time.monotonic() - started
    assert result.returncode == 0, result.stderr
    assert elapsed >= 1.1, f"tasks overlapped despite concurrency=1 ({elapsed:.2f}s)"

    lines = marks.read_text().splitlines()
    assert lines.count("start") == 3 and lines.count("end") == 3
    # With concurrency=1 every "end" must come before the next "start".
    assert lines == ["start", "end"] * 3
