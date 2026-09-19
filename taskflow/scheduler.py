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
TIMEOUT_EXIT_CODE = 124  # conventional timeout exit status (same as `timeout(1)`)
_KILL_GRACE_SECONDS = 2.0


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

    The attempt budget is reset too: a manual replay gets the full
    backoff/retry sequence, not a single shot that dead-letters again on
    the next failure.
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
        attempts=0,
        started_at=None,
        finished_at=None,
        last_started_at=None,
        next_attempt_at=None,
        exit_code=None,
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
        if timeout is not None:
            self.task_timeout_seconds = float(timeout)
            if self.task_timeout_seconds <= 0:
                raise ValueError("task_timeout_seconds must be > 0")
        else:
            self.task_timeout_seconds = None
        self.logs_dir = os.path.join(store.state_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        self._stop_requested = False

    # -- DAG ingest ---------------------------------------------------------

    def merge_dag(self, dag_tasks: list) -> list:
        """Merge a submitted DAG into persistent state.

        Returns the list of skipped (task_id, idempotency_key) duplicates.
        Tasks whose idempotency_key was seen before, or whose id already
        exists in state, are never added again and therefore never re-run.

        Every dependency of every task in the submission must resolve to a
        task already present in state or one submitted in the same batch.
        An unknown dependency rejects the whole submission (nothing is
        mutated): batches are all-or-nothing and a downstream task must
        never run with an upstream that does not exist.
        """
        tasks = self.store.tasks()
        known_ids = set(tasks)
        skipped = []
        additions = []
        for spec in dag_tasks:
            task_id = spec["id"]
            key = spec.get("idempotency_key")
            if key is not None and key in self.store.data["idempotency_keys"]:
                skipped.append({"id": task_id, "idempotency_key": key})
                continue
            if task_id in known_ids:
                continue
            additions.append((task_id, spec, key))

        effective_ids = known_ids | {task_id for task_id, _, _ in additions}
        missing = []
        for task_id, spec, _ in additions:
            for dep in spec.get("needs") or []:
                if dep not in effective_ids:
                    missing.append((task_id, dep))
        if missing:
            detail = ", ".join(f"{task_id} needs '{dep}'" for task_id, dep in missing)
            raise ValueError(f"dag references tasks that do not exist: {detail}")

        for task_id, spec, key in additions:
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
            }
            if key is not None:
                self.store.data["idempotency_keys"][key] = task_id
        return skipped

    # -- crash recovery -------------------------------------------------------

    def recover_interrupted(self) -> list:
        """Tasks left 'running' by a kill -9 / power cut become pending again.

        Succeeded tasks are never touched, so they never re-run.
        """
        recovered = []
        for task in self.store.tasks().values():
            if task["status"] == RUNNING:
                task["status"] = PENDING
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
            # An interrupted batch is not a successful run: deployment
            # scripts must not treat a partial batch as finished. If every
            # task did happen to finish first, the normal exit code applies.
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
        task["attempts"] += 1
        task["status"] = RUNNING
        now = _utc_now_iso()
        if task["started_at"] is None:
            task["started_at"] = now
        task["last_started_at"] = now
        task["next_attempt_at"] = None
        self.store.save()
        log_path = os.path.join(self.logs_dir, f"{task['id']}.log")
        futures[pool.submit(
            self._execute, dict(task), log_path, self.task_timeout_seconds
        )] = task

    @staticmethod
    def _execute(task_snapshot: dict, log_path: str,
                 timeout_seconds: float | None) -> int:
        with open(log_path, "ab") as log:
            log.write(
                f"\n===== attempt {task_snapshot['attempts']} "
                f"at {_utc_now_iso()} =====\n$ {task_snapshot['run']}\n".encode()
            )
            log.flush()
            kwargs = {}
            if timeout_seconds is not None:
                kwargs["start_new_session"] = True
            proc = subprocess.Popen(
                task_snapshot["run"], shell=True,
                stdout=log, stderr=subprocess.STDOUT, **kwargs
            )
            try:
                returncode = proc.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                log.write(
                    f"[timeout after {timeout_seconds}s; terminating task "
                    f"and its child processes]\n".encode()
                )
                log.flush()
                Scheduler._kill_process_tree(proc)
                returncode = TIMEOUT_EXIT_CODE
            log.write(f"[exit {returncode}]\n".encode())
            return returncode

    @staticmethod
    def _kill_process_tree(proc: subprocess.Popen) -> None:
        """Kill the task shell and everything it spawned.

        The task runs as its own session/process-group leader, so the whole
        descendant tree can be signalled at once instead of leaving child
        processes behind writing data. SIGTERM first, then SIGKILL.
        """
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=_KILL_GRACE_SECONDS)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()

    def _on_complete(self, task, exit_code: int) -> None:
        # A timed-out attempt is killed and its slot is reused for the retry;
        # the dead subprocess may still report back late. Never let that
        # stale completion overwrite the state of a newer attempt.
        if task["status"] != RUNNING:
            return
        task["exit_code"] = exit_code
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
