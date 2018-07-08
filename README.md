

```java  

usage: rct -f <format> [-s <uri> | -i <file>] -o <file> [-d <num num...>]
           [-e <escape>] [-k <regex regex...>] [-t <type type...>] [-b
           <bytes>] [-l <n>]
Options:
 -b,--bytes <bytes>          limit memory output(--format mem) to keys
                             greater to or equal to this value (in bytes)
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -e,--escape <escape>        escape strings to encoding: raw (default),
                             redis.
 -f,--format <format>        format to export. valid commands are json,
                             dump, key, keyval, mem and resp
 -h,--help                   rct usage.
 -i,--in <file>              input file.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -l,--largest <n>            limit memory output(--format mem) to only the
                             top n keys (by size).
 -o,--out <file>             output file.
 -s,--source <uri>           source uri. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb.
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rct version.
Examples:
 rct -f resp -s redis://127.0.0.1:6379 -o ./target.aof -d 0 1
 rct -f json -i ./dump.rdb -o ./target.json -k user.* product.*
 rct -f mem -i ./dump.rdb -o ./target.aof -e redis -t list -l 10 -b 1024

```


```java  

usage: rmt [-s <uri> | -i <file>] -m <uri> [-d <num num...>] [-k <regex
           regex...>] [-t <type type...>]
Options:
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
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rmt version.
Examples:
 rmt -s redis://120.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0
 rmt -i ./dump.rdb -m redis://127.0.0.1:6380 -t string -r

```

```java  

usage: rdt [-b <uri> | [-s <uri> | -i <file>] -c <file> | -m <file
           file...>] -o <file> [-d <num num...>] [-k <regex regex...>] [-t
           <type type...>]
Options:
 -b,--backup <uri>           backup uri to local rdb file. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb
 -c,--config <file>          redis cluster's <node.conf> file(--split
                             <file>).
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -h,--help                   rdt usage.
 -i,--in <file>              input file.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -m,--merge <file file...>   merge multi rdb files to one rdb file.
 -o,--out <file>             if --backup <uri> or --merge <file file...>
                             specified. the <file> is the target file. if
                             --split <uri> specified. the <file> is the
                             target path.
 -s,--split <uri>            split uri to multi file via cluster's
                             <node.conf>. eg:
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rdt version.
Examples:
 rdt -b redis://127.0.0.1:6379 -o ./dump.rdb -k user.*
 rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash
 rdt -i ./dump.rdb -c ./node.conf -o /path/to/folder -t hash
 rdt -s redis://127.0.0.1:6379 -c ./node.conf -o /path/to/folder -d 0 1

```