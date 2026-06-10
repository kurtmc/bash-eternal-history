# bash-eternal-history

A FUSE filesystem exposing a single `.bash_eternal_history` file backed by AWS
DynamoDB, so bash history is stored indefinitely and shared across machines.

## Development

- Run tests with `go test -race ./...` (no AWS credentials needed; everything
  is mocked behind small interfaces).
- `main.go` wires everything together; `file.go` holds the FUSE file state,
  `writer.go` the async DynamoDB write queue, `content.go` the table scan that
  materialises the file content.

## Releases

Releases are cut by pushing a git tag. Tags are unprefixed semver (e.g.
`0.1.0`, no `v` prefix — check `git tag --sort=-v:refname` for the latest).
Pushing the tag triggers the `goreleaser` GitHub Actions workflow
(`.github/workflows/release.yml`), which builds linux binaries and publishes
the release to GitHub. There is no manual release step beyond:

```
git tag <version>
git push origin <version>
```
