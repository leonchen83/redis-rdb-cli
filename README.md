

```java  

usage: rct
 -b,--bytes <bytes>          limit memory output(--format mem) to keys
                             greater to or equal to this value (in bytes)
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -e,--escape <escape>        escape strings to encoding: raw (default),
                             redis.
 -f,--format <format>        command to execute. valid commands are json,
                             dump, key, keyval, mem and resp
 -h,--help                   rct usage.
 -i,--in <file>              input file.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -l,--largest <n>            limit memory output(--format mem) to only the
                             top n keys (by size).
 -o,--out <file>             output file.
 -t,--type <type type...>    data type to include. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -u,--uri <uri>              input uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb.
 -v,--version                rct version.


```


```java  

usage: rmt
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -h,--help                   rmt usage.
 -i,--in <file>              input file.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -m,--migrate <uri>          migrate to uri. eg:
                             redis://host:port?authPassword=foobar.
 -r,--replace                replace exist key value. if not specified,
                             default value is false.
 -s,--source <uri>           source uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb
                             redis:///path/to/appendonly.aof.
 -t,--type <type type...>    data type to include. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rmt version.


```