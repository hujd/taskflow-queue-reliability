"""Command line interface: run / status / retry."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .scheduler import ALL_STATUSES, Scheduler, refresh_blocked, requeue_task
from .state import StateLock, StateStore


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    for key in ("state_dir", "concurrency", "retry"):
        if key not in config:
            raise ValueError(f"config is missing required key: {key}")
    for key in ("base_seconds", "max_attempts"):
        if key not in config["retry"]:
            raise ValueError(f"config.retry is missing required key: {key}")
    # Relative paths resolve against the config file's directory so the CLI
    # behaves the same no matter where it is invoked from.
    base = os.path.dirname(os.path.abspath(path))
    for key in ("state_dir", "dag_file"):
        if key in config and not os.path.isabs(config[key]):
            config[key] = os.path.normpath(os.path.join(base, config[key]))
    return config


def load_dag(path: str) -> list:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    tasks = payload["tasks"] if isinstance(payload, dict) else payload
    seen = set()
    for spec in tasks:
        for key in ("id", "run"):
            if key not in spec:
                raise ValueError(f"dag task is missing required key: {key}")
        if spec["id"] in seen:
            raise ValueError(f"duplicate task id in dag: {spec['id']}")
        seen.add(spec["id"])
    return tasks


def status_payload(store: StateStore) -> dict:
    tasks = [store.tasks()[tid] for tid in sorted(store.tasks())]
    summary = {status: 0 for status in ALL_STATUSES}
    for task in tasks:
        summary[task["status"]] += 1
    summary["total"] = len(tasks)
    return {"state_dir": store.state_dir, "summary": summary, "tasks": tasks}


def cmd_run(args) -> int:
    config = load_config(args.config)
    if "dag_file" not in config:
        raise ValueError("config is missing required key: dag_file")
    dag_tasks = load_dag(config["dag_file"])
    store = StateStore(config["state_dir"]).load()
    with StateLock(store.state_dir):
        scheduler = Scheduler(config, store)
        skipped = scheduler.merge_dag(dag_tasks)
        if skipped:
            print(f"skipped {len(skipped)} duplicate idempotency key(s): "
                  + ", ".join(s["id"] for s in skipped))
        code = scheduler.run()
    print(json.dumps(status_payload(store)["summary"], sort_keys=True))
    return code


def cmd_status(args) -> int:
    config = load_config(args.config)
    store = StateStore(config["state_dir"]).load()
    print(json.dumps(status_payload(store), indent=2, sort_keys=True))
    return 0


def cmd_retry(args) -> int:
    config = load_config(args.config)
    store = StateStore(config["state_dir"]).load()
    with StateLock(store.state_dir):
        task = requeue_task(store, args.task)
        refresh_blocked(store.tasks())
        store.save()
    print(json.dumps({"requeued": task["id"], "status": task["status"]}, sort_keys=True))
    print(f"task {task['id']} requeued; use 'run' to execute it", file=sys.stderr)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="taskflow")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="merge the DAG and run it to completion")
    p_run.add_argument("--config", required=True)
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="print task states as JSON")
    p_status.add_argument("--config", required=True)
    p_status.set_defaults(func=cmd_status)

    p_retry = sub.add_parser("retry", help="requeue a dead/failed task")
    p_retry.add_argument("--config", required=True)
    p_retry.add_argument("--task", required=True)
    p_retry.set_defaults(func=cmd_retry)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, KeyError, RuntimeError, OSError, json.JSONDecodeError) as exc:
        print(f"taskflow: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
