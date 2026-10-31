# Local sync

A *checkout* is an ordinary folder kept in sync with a workspace branch (`ws/<agent>`). The same
branch can be written by agents running in the cloud, so a branch has several writers; syncing is
the fetch, merge and push that landing agent work already uses (`workspace/src/cmws_ws/sync.py`).

## Model

The folder records, in `.cmws/state.json`, the commit it last matched (`base`) and, per file, the
blob id plus size and mtime it had then. Local changes are whatever differs from that record.
Size and mtime let a scan skip hashing unchanged files; a file modified in the last two seconds
is always hashed, because a stat match that recent cannot be trusted.

| Operation | What it does |
|---|---|
| `clone` | fork the branch from main if new, write its files |
| `pull` | fetch the tip; if the folder has edits, three-way merge `(base, folder, tip)`; write the result into the folder; `base = tip` |
| `push` | pull if the tip moved; commit the folder's changes on top of the tip; CAS the branch from the tip to the commit; if the CAS loses a race, pull again and retry |
| `submit` | push, then `Repository.submit` the branch to main, then pull |
| `watch` | poll: pull when the tip moves, push once edits stop changing for a debounce period |

The merge is the one used between agents, including rename following: a file moved in the folder
(a plain `mv`, seen as a delete plus an add) is detected by content, and a concurrent remote edit
of the old name lands in the new one. The commit that `push` makes records those moves.

After a pull, the remaining differences between the folder and the new `base` are exactly the
local edits sitting on top of the tip, so a later push commits them with the tip as parent. This
is the stash-and-reapply shape of `git pull --rebase`; no local merge commits are created.

## Conflicts

A pull that conflicts still applies everything that merged cleanly. Text conflicts are written into
the file with `<<<<<<< local` / `=======` / `>>>>>>> remote` markers (diff3 style, with the base
between `|||||||` and `=======`). For other kinds (a file edited here and deleted there, binary,
file against directory) the side that still has the file is kept. `push` is blocked while
conflicts are open; a text conflict closes itself when the markers are gone from the file, any
other kind with `resolve PATH`.

## Multiple writers on one branch

`Workspace.commit` used to assume it was the branch's only writer. It now publishes with a retry:
if the branch tip moved, the tip is merged into the new commit (a merge commit with both parents)
and the CAS is retried; if the two cannot merge, `ConcurrentWorkspaceUpdate` is raised carrying the
conflicts and nothing changes. `Workspace.refresh` fast-forwards a handle to the tip (it needs a
clean overlay, so remote commits never sit silently under pending edits). `sync` and `submit`
refresh first.

## Offline

The folder is the buffer. While the store is unreachable the watcher reports `offline`, edits
accumulate on disk, and nothing is lost: on reconnect the first step is the same pull-merge-push,
so work done in the cloud meanwhile is merged with the offline edits.

## Limits

- Polling, not push: the store has no watch or long-poll operation, so a checkout learns of remote
  changes by reading the branch ref every `interval`, and local changes by walking the folder
  (stat-based) rather than via inotify/FSEvents. Fine for repositories of thousands of files,
  wasteful beyond that.
- Edits made by a person in the folder are not read-tracked, so stale-read detection (which needs
  to know what was read) protects agent work but not human edits.
- Between scanning and writing, a file edited by the user is detected: it is left alone and
  reported as a `raced` conflict. The window is not zero.
- Regular files only: symlinks are skipped; no executable bit or permissions; paths are treated as
  UTF-8 with `/` separators (not tried on Windows).
- A binary or non-UTF-8 file edited on both sides is a conflict that `resolve` closes, with no
  merge help.
