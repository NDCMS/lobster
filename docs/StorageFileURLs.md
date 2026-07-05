# `file://` output handling

When an output destination in `StorageConfiguration(output=[...])` starts with
`file://`, Lobster treats it as a **local filesystem stage-out target**.

## Behavior

- Lobster strips the `file://` prefix and uses the remaining string as a local
  directory prefix. For each workflow output file, it appends the per-task
  remote filename and copies data with `shutil.copy2(...)`.
- A `file://` stage-out target is only attempted if the destination parent
  directory already exists on the worker (`os.path.isdir(os.path.dirname(...))`).
  It does not auto-create missing output directories during task stage-out.
- After copying, Lobster verifies transfer correctness via local `stat` size
  comparison. If size checks fail, that stage-out method is considered failed.
- If no configured output method succeeds for a produced file, the task raises a
  stage-out error.

## Important limitation

Yes: with `file://`, Lobster can only stage out to storage that is directly
reachable from the worker as a local filesystem path.

If workers cannot access that path (or parent directories are missing), the
`file://` method fails and Lobster must succeed via another configured output
method (for example `root://`) or the task fails stage-out.

## Practical implication

For values like:

- `file:///project01/ndcms//store/user/$USER/...`

Lobster will stage out by copying to local paths under:

- `/project01/ndcms//store/user/$USER/.../<remotename>`

(Extra slashes are tolerated by normal POSIX path handling.)
