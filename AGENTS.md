# AGENTS.md — jamesbroadhead/moto

This fork carries the AWS behaviour that Taktile's "E2E in a box" suite needs (taktile-services `tests/box/`,
read its `README.md` and `AGENTS.md`). Agents maintain it; people review. Everything else about moto is in
`CONTRIBUTING.md` and https://docs.getmoto.org/en/latest/docs/contributing.

## Branches

- `box-fixes` is the integration branch. taktile-services pins one of its commits (`MOTO_COMMIT` in
  `.github/workflows/e2e-box.yaml`), so its history must stay reachable: merge into it, never rebase or
  force-push it.
- Do each change on a branch off `box-fixes`, then merge it in with a merge commit.
- Remotes: `fork` is this repo (push here); `origin` is getmoto/moto. Never push to getmoto/moto or open an
  upstream pull request.

## Making a change

1. Write a test in moto's own suite (`tests/test_<service>/`) that shows the AWS behaviour first. When the change
   comes from a difference between the box and real AWS, the test reproduces what AWS did (from the real-AWS run
   on main, AWS documentation or a recorded response), not what the box expected.
2. Implement it in the service's `models.py` / `responses.py`, as upstream moto would.
3. Behaviour that only the box wants (external endpoints, SQL engines, runners) goes behind a `MOTO_*` setting
   in `moto/settings.py`, off by default. taktile-services turns it on in `tests/box/services/start.py`.
4. Open an issue on this fork labelled `upstream-pr-template`, written as the future upstream pull request:
   what AWS does, what moto did, the change, the tests.
5. Merge into `box-fixes`, tag the merge `box-<first 8 of sha>`, push the branch and the tag, and set
   `MOTO_COMMIT` in taktile-services to the full SHA in the same piece of work as the box or spec change that
   needed it.

## Working with the box

Agents develop moto and the e2e specs together. When a spec fails in the box because moto lacks something AWS
does, fix it here, with a test, in the same piece of work, rather than working around it in `tests/box/`. The
test for whether it belongs here: would AWS do this for every customer?

## Syncing with upstream

The `Box upstream sync` workflow merges `origin/master` into `box-fixes` every week. When the merge or the tests
fail, it opens an issue labelled `upstream-sync`. To fix one:

1. Branch off `box-fixes`, `git merge origin/master`, resolve conflicts keeping both upstream's change and ours.
2. Run the tests of every service either side touched.
3. Merge into `box-fixes` and push. Do not bump `MOTO_COMMIT` unless the box needs the new commit; when you do,
   the "E2E in a box" workflow must stay green.

## Checks

```sh
uvx ruff@0.14.10 check moto tests && uvx ruff@0.14.10 format --check moto tests   # pin from requirements-dev.txt
pytest -q -n 4 tests/test_<service>
```

Run the tests of each service you changed. Before bumping `MOTO_COMMIT`, run the "E2E in a box" workflow in
taktile-services with the new SHA, or the box's laptop runner pointed at this checkout.
