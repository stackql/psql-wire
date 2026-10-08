# Fuzzing

The project uses Go's native fuzzing support. Fuzz tests and deterministic
regression seeds live beside the packages they exercise; the ordinary
`go test ./...` run executes every seed.

## Run a target

Go runs one fuzz target per invocation. Run a target for a time limit with
`make fuzz`, naming both the target and its package:

```sh
make fuzz FUZZ_TARGET=FuzzSessionStructured FUZZ_PACKAGE=. TIME=5m
make fuzz FUZZ_TARGET=FuzzReader FUZZ_PACKAGE=./internal/buffer TIME=5m
```

The available targets are `FuzzReader` (`./internal/buffer`),
`FuzzSplitCompoundQuery` (`./pkg/sqlbackend`), and the root-package targets
`FuzzStartup`, `FuzzSessionRaw`, `FuzzSessionStructured`, and `FuzzRowEncoding`.
The CI workflow runs the regression corpus on every pull request, then fuzzes
each target for one minute. Its nightly run fuzzes each target for ten minutes
and caches Go's generated corpus outside the repository.

## Add a target or seed

Add a `FuzzXxx(*testing.F)` test beside the package code. Seed it with
`f.Add` or add a Go fuzz corpus file under `testdata/fuzz/FuzzXxx/`. Keep
inputs synthetic and deterministic. For protocol targets, exercise complete
message sequences as well as malformed framing, and check responses with an
independent decoder. State protocol invariants in the test with a link to the
relevant PostgreSQL protocol section.

The root integration tests can optionally capture client-to-server bytes and
write sanitized startup and command seeds:

```sh
PSQL_WIRE_RECORD_FUZZ_SEEDS=1 go test -run '^TestClientConnect$' .
```

The capture code replaces client startup parameters with synthetic values
before writing a seed. Review every resulting corpus file before committing it;
never commit passwords, hostnames, real query text, or other environment data.

## Triage a failure

Go writes a minimized failing input under the target's `testdata/fuzz/FuzzXxx/`
directory. Reproduce it with the command printed by Go, or run all deterministic
regressions with:

```sh
go test ./...
```

Keep the failing input as a regression seed after fixing the underlying defect.
Do not delete a seed or narrow a target just to make the fuzz run pass. Review
the minimized bytes, identify the violated invariant or panic, make the
smallest correct fix, and rerun both the specific target and the full tests.
