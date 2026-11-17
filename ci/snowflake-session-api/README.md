# snowflake-session-api (temporary CI build)

**Temporary. Delete this directory, `scripts/install_session_api.sh`, and the CI steps that call it
once `com.snowflake:snowflake-session-api` is published.**

Snowpark depends on `com.snowflake:snowflake-session-api:0.1.0-SNAPSHOT` (see `sessionApi` in
`build.sbt`), which is not yet published to any repository; `build.sbt` resolves it from the local
Maven repository. So that CI can build, this directory vendors the module's sources and
`scripts/install_session_api.sh` compiles them (Java 8 target, `javac` + `jar` only, no Maven
required) and installs the jar into `~/.m2/repository` before `sbt` runs.

The sources under `src/main/java/com/snowflake/session/api/` are verbatim copies of
`ExecPlatform/src/coprocessor/java/libraries/snowflake-session-api/src/main/java/com/snowflake/session/api/`
from snowflake-eng/snowflake#575054, the source of truth. Do not edit them here; change them there
and re-copy.

To build locally the same way CI does:

```bash
scripts/install_session_api.sh
```
