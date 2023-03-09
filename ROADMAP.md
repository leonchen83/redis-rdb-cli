## Redis-Rdb-CLI v1.0.0 roadmap

- [x] remove jline dependency.
- [x] jdk baseline upgrade to jdk11.
- [ ] support redis cluster migration.
- [x] use `JAVA_OPTS` replace `JAVA_TOOL_OPTIONS`.
- [x] deprecate and remove `rct -f keyval` format.
- [ ] upgrade influxdb to 2.x and use fluxql replace CQ.
- [ ] use logback instead of log4j2 to reduce toolset size.
- [ ] redis uri support cluster and sentinel. (breaking change)
- [x] default export file format change to jsonl. (breaking change)
- [x] use java default keystore instead of user generated. (breaking change)

## Migrate to v1.0.0

TBD