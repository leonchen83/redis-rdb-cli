

```java  

usage: rct
 -b,--bytes <bytes>          Limit memory output(--format mem) to keys
                             greater to or equal to this value (in bytes)
 -d,--db <num num...>        Database Number. Multiple databases can be
                             provided. If not specified, all databases
                             will be included.
 -e,--escape <escape>        Escape strings to encoding: raw (default),
                             print.
 -f,--format <format>        Command to execute. Valid commands are json,
                             dump, key, keyval, mem and resp
 -h,--help                   rct usage.
 -i,--in <file>              Input file.
 -k,--key <regex regex...>   Keys to export. This can be a RegEx.
 -l,--largest <n>            Limit memory output(--format mem) to only the
                             top N keys (by size).
 -o,--out <file>             Output file.
 -t,--type <type type...>    Data type to include. Possible values are
                             string, hash, set, sortedset, list,
                             module(--format [mem|dump|key]),
                             stream(--format [mem|dump|key]). Multiple
                             types can be provided. If not specified, all
                             data types will be returned.
 -u,--uri <uri>              Input uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb.
 -v,--version                rct version.


```


```java  

usage: rmt
 -d,--db <num num...>        Database Number. Multiple databases can be
                             provided. If not specified, all databases
                             will be included.
 -h,--help                   rmt usage.
 -i,--in <file>              Input file.
 -k,--key <regex regex...>   Keys to export. This can be a RegEx.
 -m,--migrate <uri>          Migrate to uri. eg:
                             redis://host:port?authPassword=foobar.
 -r,--replace                Replace exist key value. If not specified
                             default value is false.
 -s,--source <uri>           Source uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb
                             redis:///path/to/appendonly.aof.
 -t,--type <type type...>    Data type to include. Possible values are
                             string, hash, set, sortedset, list,
                             module(--format [mem|dump|key]),
                             stream(--format [mem|dump|key]). Multiple
                             types can be provided. If not specified, all
                             data types will be returned.
 -v,--version                rmt version.


```