# Lobster operational guidance

This note supplements the command reference with practical requirements for
starting, controlling, and diagnosing a Lobster project.

## Configuration files and working directories

Most Lobster commands accept either the original Python configuration file or
an existing project workdir. When a configuration file is supplied, Lobster
imports it and uses the resulting `Config.workdir` to locate project state. If
that workdir already contains a Lobster checkpoint, the saved configuration is
loaded from the workdir.

This makes the workdir the durable identity of a running project. Prefer the
explicit workdir for status, configuration changes, recovery, and termination:

```bash
lobster status <workdir>
lobster configure <workdir>
lobster terminate <workdir>
```

`lobster terminate <config.py>` is also valid when importing `config.py`
resolves exactly the same workdir as the running manager. Generated or
temporary configurations that derive state paths from the current time must
freeze those values before processing if they will later be used for control
commands. Re-import the configuration in a separate process and compare at
least its label, workdir, plotdir, storage outputs, and expected `config.pkl`
location before relying on config-based termination.

If a configuration can resolve a different workdir on a later import, pass the
known workdir to the control command instead.

## Graceful termination

`lobster terminate` records a pending termination checkpoint. The running
manager observes that checkpoint in its control loop, stops creating new work,
and exits at a lifecycle checkpoint. Termination is therefore not
instantaneous, and tasks that are already running may finish before the manager
exits.

Confirm shutdown from the manager logs and process state. Do not delete the
workdir merely because a terminate command returned successfully; it contains
the database, checkpoints, saved configuration, and recovery state.

## Worker factories

Lobster advertises a Work Queue manager project, but it does not start or stop
an external `work_queue_factory`. When a factory is used, the operator is
responsible for its configuration, credentials, resource limits, logs,
lifetime, and shutdown. The factory project pattern must match the manager
project name.

Keep factory lifecycle records separate from Lobster manager records. A
successful manager start does not prove that workers connected, and connected
workers do not prove that tasks or stage-out succeeded.

## Logs and storage failures

Start with the native project logs in the workdir:

- `process.log` for manager lifecycle and task handling;
- `process.err` for daemon stdout/stderr;
- `configure.log` for live configuration updates;
- `work_queue.log` and `transactions.log` for manager/worker activity;
- per-task directories and `report.json` files for payload and transfer details.

The monitoring pages classify stage-in as exit code `179`, stage-out transfer
failure as `210`, and stage-out verification failure as `211`. For XRootD or
other storage failures, record the failing URL or command, return code, stderr,
stdout, endpoint, and affected task before retrying. Correct credentials,
reachability, permissions, quota, or path configuration first; raising retry
thresholds does not repair a persistent storage problem.

For local `file://` stage-out behavior and its worker-visible-directory
requirement, see [StorageFileURLs.md](StorageFileURLs.md). For retry-threshold
recovery, see [FailureThresholdRecovery.md](FailureThresholdRecovery.md).
