# Blocking a PR from Merging

To keep a PR from being merged until a follow-up is done, do any of these:

- put `#nocommit` in the code (for example `// #nocommit remove debug logging`),
- put `#nocommit` in the PR title or description, or
- add the `do-not-merge` label.

The `nocommit` check fails while any of them is present. Remove the marker or label once the follow-up is done. The check matches `nocommit` or `do not merge` in any case, but only on lines the PR adds, so deleting an old marker never blocks a PR.

The code scan skips this page and `.github/workflows/nocommit.yml`, which have to contain the marker. A marker added to either file doesn't block a merge.
