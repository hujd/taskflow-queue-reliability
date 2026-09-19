"""DAG scheduler: dependency gating, retries with backoff, dead letters,
crash recovery and graceful shutdown."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

PENDING = "pending"      # waiting for upstream dependencies
BLOCKED = "blocked"      # an upstream task is dead/blocked, will never run
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"        # last attempt failed, retry scheduled (see next_attempt_at)
DEAD = "dead"            # retries exhausted, sits in the dead-letter set

ALL_STATUSES = (PENDING, BLOCKED, RUNNING, SUCCEEDED, FAILED, DEAD)
_ACTIVE = (PENDING, RUNNING, FAILED)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def refresh_blocked(tasks: dict) -> None:
    """Recompute blocked/pending transitions until stable.

    A pending task becomes blocked when any dependency is dead or blocked.
    A blocked task goes back to pending when its dependencies are no longer
    dead/blocked (e.g. after a dead task was manually requeued).
    """
    changed = True
    while changed:
        changed = False
        for task in tasks.values():
            dep_bad = any(
                tasks[dep]["status"] in (DEAD, BLOCKED) for dep in task["needs"]
            )
            if task["status"] == PENDING and dep_bad:
                task["status"] = BLOCKED
                changed = True
            elif task["status"] == BLOCKED and not dep_bad:
                task["status"] = PENDING
                changed = True


def requeue_task(store, task_id: str) -> dict:
    """Reset a dead/failed task so the next ``run`` executes it again.

    The attempt counter is reset as well, so a replayed task enjoys the
    full backoff/retry budget instead of dying again after one try.
    """
    tasks = store.tasks()
    if task_id not in tasks:
        raise KeyError(f"unknown task: {task_id}")
    task = tasks[task_id]
    if task["status"] not in (DEAD, FAILED):
        raise ValueError(
            f"task {task_id} is {task['status']}; only dead or failed tasks can be requeued"
        )
    task.update(
        status=PENDING,
        started_at=None,
        finished_at=None,
        last_started_at=None,
        next_attempt_at=None,
        exit_code=None,
        attempts=0,
        pid=None,
    )
    refresh_blocked(tasks)
    return task


class Scheduler:
    def __init__(self, config: dict, store):
        self.store = store
        self.concurrency = int(config["concurrency"])
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        retry = config.get("retry", {})
        self.base_seconds = float(retry.get("base_seconds", 1.0))
        self.max_attempts = int(retry.get("max_attempts", 1))
        if self.max_attempts < 1:
            raise ValueError("retry.max_attempts must be >= 1")
        timeout = config.get("task_timeout_seconds")
        self.task_timeout = None if timeout is None else float(timeout)
        if self.task_timeout is not None and self.task_timeout <= 0:
            raise ValueError("task_timeout_seconds must be > 0")
        self.logs_dir = os.path.join(store.state_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        self._stop_requested = False

    # -- DAG ingest ---------------------------------------------------------

    def merge_dag(self, dag_tasks: list) -> list:
        """Merge a submitted DAG into persistent state.

        Returns the list of skipped (task_id, idempotency_key) duplicates.
        Tasks whose idempotency_key was seen before, or whose id already
        exists in state, are never added again and therefore never re-run.

        Every dependency must resolve to a task from this submission or to
        a task already in state; otherwise the whole submission is rejected
        before any state is mutated. Pipelines are submitted as complete
        batches, so a missing dependency is a configuration error and must
        never be silently dropped.
        """
        tasks = self.store.tasks()
        known_ids = set(tasks)
        accepted: list[tuple[dict, str | None]] = []
        added_ids: set[str] = set()
        skipped = []
        for spec in dag_tasks:
            task_id = spec["id"]
            key = spec.get("idempotency_key")
            if key is not None and key in self.store.data["idempotency_keys"]:
                skipped.append({"id": task_id, "idempotency_key": key})
                continue
            if task_id in known_ids:
                continue
            accepted.append((spec, key))
            added_ids.add(task_id)

        available = known_ids | added_ids
        problems = []
        for spec, _key in accepted:
            for dep in spec.get("needs") or []:
                if dep not in available:
                    problems.append(f"task {spec['id']!r} needs unknown task {dep!r}")
        if problems:
            raise ValueError("invalid DAG: " + "; ".join(problems))

        for spec, key in accepted:
            task_id = spec["id"]
            known_ids.add(task_id)
            tasks[task_id] = {
                "id": task_id,
                "run": spec["run"],
                "needs": list(spec.get("needs") or []),
                "idempotency_key": key,
                "status": PENDING,
                "attempts": 0,
                "started_at": None,
                "finished_at": None,
                "last_started_at": None,
                "next_attempt_at": None,
                "exit_code": None,
                "pid": None,
            }
            if key is not None:
                self.store.data["idempotency_keys"][key] = task_id
        return skipped

    # -- crash recovery -------------------------------------------------------

    def recover_interrupted(self) -> list:
        """Tasks left 'running' by a kill -9 / power cut become pending again.

        Succeeded tasks are never touched, so they never re-run.

        Any orphaned process group left behind by the dead scheduler is
        killed first, so a recovered task can never run twice at once and
        abandoned children cannot keep writing data in the background.
        """
        recovered = []
        for task in self.store.tasks().values():
            if task["status"] == RUNNING:
                self._kill_group(task.get("pid"))
                task["status"] = PENDING
                task["pid"] = None
                recovered.append(task["id"])
        return recovered

    # -- main loop ------------------------------------------------------------

    def run(self) -> int:
        self._install_signal_handlers()
        recovered = self.recover_interrupted()
        if recovered:
            print(f"recovered interrupted tasks: {', '.join(sorted(recovered))}")
        refresh_blocked(self.store.tasks())
        self.store.save()

        futures: dict = {}
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            while True:
                refresh_blocked(self.store.tasks())
                if not self._stop_requested:
                    for task in self._runnable_tasks():
                        if len(futures) >= self.concurrency:
                            break
                        self._submit(pool, futures, task)
                done = [fut for fut in futures if fut.done()]
                for fut in done:
                    self._on_complete(futures.pop(fut), fut.result())
                if done:
                    self.store.save()
                if not futures:
                    if self._stop_requested or not self._has_active_work():
                        break
                time.sleep(self._sleep_interval())
        self.store.save()
        if self._stop_requested:
            print("shutdown requested: stopped scheduling, running tasks finished, state saved")
            # An interrupted batch is not a successful round, even though
            # the shutdown itself was graceful: anything not yet finished
            # makes the exit code non-zero for deployment scripts.
            return self.exit_code()
        return self.exit_code()

    def exit_code(self) -> int:
        tasks = self.store.tasks()
        return 0 if all(t["status"] == SUCCEEDED for t in tasks.values()) else 1

    # -- internals --------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        def _handler(signum, frame):
            self._stop_requested = True

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    def _runnable_tasks(self) -> list:
        now = time.time()
        tasks = self.store.tasks()
        runnable = []
        for task in tasks.values():
            if task["status"] == PENDING:
                if all(tasks[dep]["status"] == SUCCEEDED for dep in task["needs"]):
                    runnable.append(task)
            elif task["status"] == FAILED:
                if task["next_attempt_at"] is not None and task["next_attempt_at"] <= now:
                    runnable.append(task)
        runnable.sort(key=lambda t: t["id"])
        return runnable

    def _has_active_work(self) -> bool:
        return any(t["status"] in _ACTIVE for t in self.store.tasks().values())

    def _sleep_interval(self) -> float:
        waits = [
            t["next_attempt_at"] - time.time()
            for t in self.store.tasks().values()
            if t["status"] == FAILED and t["next_attempt_at"] is not None
        ]
        if waits:
            return max(0.02, min(0.1, min(waits)))
        return 0.05

    def _submit(self, pool, futures, task) -> None:
        log_path = os.path.join(self.logs_dir, f"{task['id']}.log")
        log = open(log_path, "ab")
        log.write(
            f"\n===== attempt {task['attempts'] + 1} "
            f"at {_utc_now_iso()} =====\n$ {task['run']}\n".encode()
        )
        log.flush()
        proc = subprocess.Popen(
            task["run"],
            shell=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        # Flip to RUNNING and persist the process-group leader pid in the
        # same save right after fork: a kill -9 afterwards leaves enough
        # state for recovery to kill the orphaned group, so the task can
        # never run twice at once.
        task["attempts"] += 1
        task["status"] = RUNNING
        now = _utc_now_iso()
        if task["started_at"] is None:
            task["started_at"] = now
        task["last_started_at"] = now
        task["next_attempt_at"] = None
        task["pid"] = proc.pid
        self.store.save()
        futures[pool.submit(self._await_proc, proc, log, self.task_timeout)] = task

    @staticmethod
    def _await_proc(proc, log, timeout) -> int:
        """Wait for an attempt that runs in its own process group.

        ``start_new_session`` puts the shell and every process it spawns
        into a dedicated process group, so a timeout can kill the whole
        tree at once. The group is fully reaped before this method
        returns -- no second instance can start while an abandoned child
        is still alive and writing.
        """
        try:
            try:
                returncode = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                log.write(
                    f"[timeout after {timeout}s; killed process group {proc.pid}]\n".encode()
                )
                log.flush()
                Scheduler._kill_group(proc.pid)
                returncode = proc.wait()
            log.write(f"[exit {returncode}]\n".encode())
            return returncode
        finally:
            log.close()

    @staticmethod
    def _kill_group(pgid) -> None:
        """Best-effort SIGKILL of a process group created via start_new_session."""
        if not pgid:
            return
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            pass

    def _on_complete(self, task, exit_code: int) -> None:
        task["exit_code"] = exit_code
        task["pid"] = None
        if exit_code == 0:
            task["status"] = SUCCEEDED
            task["finished_at"] = _utc_now_iso()
        elif task["attempts"] >= self.max_attempts:
            task["status"] = DEAD
            task["finished_at"] = _utc_now_iso()
        else:
            task["status"] = FAILED
            delay = self.base_seconds * (2 ** (task["attempts"] - 1))
            task["next_attempt_at"] = time.time() + delay
