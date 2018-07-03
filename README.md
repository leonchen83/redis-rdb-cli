```java  

usage: rct
 -d,--db <db num>            Database Number. Multiple databases can be
                             provided. If not specified, all databases
                             will be included.
 -e,--escape <escape>        Escape strings to encoding: raw (default),
                             print.
 -f,--format <format>        Command to execute. Valid commands are json,
                             dump, key, keyval, mem and resp
 -h,--help                   rct usage.
 -i,--in <redis uri>         Input uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb.
 -k,--key <regex regex...>   Keys to export. This can be a RegEx.
 -l,--largest <n>            Limit memory output to only the top N keys
                             (by size).
 -o,--out <file>             Output file.
 -t,--type <type type...>    Data type to include. Possible values are
                             string, hash, set, sortedset, list, module,
                             stream. Multiple types can be provided. If
                             not specified, all data types will be
                             returned.
 -v,--version                rct version.

```