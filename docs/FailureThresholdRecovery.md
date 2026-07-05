# Recovering after `threshold_for_failure` / `threshold_for_skipping` is reached

When Lobster stops retrying work because units/files hit configured retry limits,
you can recover in-place from the same working directory after fixing the root
cause.

## Why retries stopped

- `threshold_for_failure` controls per-unit retries. In task creation, units with
  `failed > threshold_for_failure` are skipped; units with
  `failed == threshold_for_failure` are run as isolation tasks.
- `threshold_for_skipping` controls per-file access retries. Files are considered
  for new tasks only while `skipped < threshold_for_skipping`.

## Recovery workflow

1. **Fix the origin of failures**
   - Example: bad input endpoint, missing credentials, broken executable,
     site/storage issue.

2. **Stop the running Lobster process (if still running)**
   - Use `lobster terminate <workdir>` for graceful shutdown.

3. **Raise thresholds in the running workdir config**
   - Run `lobster configure <workdir>` and increase:
     - `advanced.threshold_for_failure`
     - `advanced.threshold_for_skipping`
   - Set them high enough for the current counters:
     - For failed units, retries resume when the threshold is at least the
       current `failed` counter (units at equality are retried in isolation).
     - For skipped files, threshold must be strictly greater than the current
       `skipped` counter.

4. **Resume processing from the same workdir**
   - Start again with `lobster process <workdir>` (or `--foreground` while
     debugging).

5. **Validate progress**
   - Use `lobster status <workdir>` to monitor failed/skipped summaries.

## Which config should you edit?

Edit the **workdir copy** (`<workdir>/config.py`), not the original config file
you used for the first `lobster process` launch.

- `lobster configure <workdir>` opens exactly `<workdir>/config.py`.
- When resuming from an existing run, Lobster loads state from the workdir
  (`config.pkl` / checkpointed config) rather than re-applying your original
  startup config path.

## Notes

- `threshold_for_failure` and `threshold_for_skipping` are runtime-mutable
  options; changes are wired to `source.update_stuck` and can be applied to an
  existing run.
- If you only need a clean wrap-up/merge after processing, `lobster process
  --finalize <workdir>` forces both thresholds to `0` (no new retries).


## Quick answer to the common sequence

Your sequence is close, with one tweak:

1. Edit `<workdir>/config.py` (this is the right file).
2. `lobster configure <workdir>` is just a convenience command to open that
   same file in `$EDITOR`.
   - So do **either** manual edit **or** `lobster configure`, not both.
3. Don’t just hope: verify in logs/status.
   - If `lobster process` is running, Lobster watches `<workdir>/config.py`
     and applies updates after mtime changes.
   - Then check `configure.log` / `process.log` and run
     `lobster status <workdir>`.


## When is `config.pkl` rewritten?

- **Not immediately on file edit.** Editing `<workdir>/config.py` alone does not
  rewrite `config.pkl`.
- `config.pkl` is rewritten when Lobster processes configuration updates in the
  running control loop (`Actions.update_configuration -> config.save()`).

Practical behavior:

- If `lobster process <workdir>` is already running, it will detect the modified
  `config.py` (mtime change), apply the update, and then rewrite `config.pkl`.
- If Lobster is not running, start/restart with `lobster process <workdir>` so
  the update loop runs and persists the new config.

So yes, you need a running `lobster process` instance for the change to be
applied and saved to `config.pkl`.
