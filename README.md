# redis-cli-tool

## Binary release

[binary releases](https://github.com/leonchen83/redis-cli-tool/releases)

## Runtime requirement

```java  
jdk 1.8+
```

## Install

```java  
mkdir redis-cli-tool
cd ./redis-cli-tool
wget https://github.com/leonchen83/redis-cli-tool/releases/download/${version}/redis-cli-tool.zip
unzip redis-cli-tool.zip
sudo chmod -R 755 ./
cd ./bin
./rct -h
```

## Compile requirement

```java  

jdk 1.8+
maven-3.3.1+

```

## Compile & run

```java  
    git clone https://github.com/leonchen83/redis-cli-tool.git
    cd redis-cli-tool
    mvn clean install -Dmaven.test.skip=true
    cd target/redis-cli-tool/bin
    ./rct -h 
```

## Windows Environment Variables
  
Add `/path/to/redis-cli-tool/bin` to `Path` environment variable  
  
## Usage

```java  

usage: rct -f <format> -s <source> -o <file> [-d <num num...>] [-e
           <escape>] [-k <regex regex...>] [-t <type type...>] [-b
           <bytes>] [-l <n>] [-r]

options:
 -b,--bytes <bytes>          limit memory output(--format mem) to keys
                             greater to or equal to this value (in bytes)
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -e,--escape <escape>        escape strings to encoding: raw (default),
                             redis.
 -f,--format <format>        format to export. valid formats are json,
                             dump, diff, key, keyval, count, mem and resp
 -h,--help                   rct usage.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -l,--largest <n>            limit memory output(--format mem) to only the
                             top n keys (by size).
 -o,--out <file>             output file.
 -r,--replace                whether the generated aof with <replace>
                             parameter(--format dump). if not specified,
                             default value is false.
 -s,--source <source>        <source> eg:
                             /path/to/dump.rdb
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb.
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rct version.

examples:
 rct -f dump -s ./dump.rdb -o ./appendonly.aof -r
 rct -f resp -s redis://127.0.0.1:6379 -o ./target.aof -d 0 1
 rct -f json -s ./dump.rdb -o ./target.json -k user.* product.*
 rct -f mem -s ./dump.rdb -o ./target.aof -e redis -t list -l 10 -b 1024

```


```java  

usage: rmt -s <source> [-m <uri> | -c <file>] [-d <num num...>] [-k <regex
           regex...>] [-t <type type...>] [-r]

options:
 -c,--config <file>          migrate data to cluster via redis cluster's
                             <nodes.conf> file, if specified, no need to
                             specify --migrate.
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -h,--help                   rmt usage.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -m,--migrate <uri>          migrate to uri. eg:
                             redis://host:port?authPassword=foobar.
 -r,--replace                replace exist key value. if not specified,
                             default value is false.
 -s,--source <source>        <source> eg:
                             /path/to/dump.rdb
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rmt version.

examples:
 rmt -s ./dump.rdb -c ./nodes.conf -t string -r
 rmt -s ./dump.rdb -m redis://127.0.0.1:6380 -t list -d 0
 rmt -s redis://120.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0

```

```java  

usage: rdt [-b <source> | -s <source> -c <file> | -m <file file...>] -o
           <file> [-d <num num...>] [-k <regex regex...>] [-t <type
           type...>]

options:
 -b,--backup <source>        backup <source> to local rdb file. eg:
                             /path/to/dump.rdb
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump.rdb
 -c,--config <file>          redis cluster's <nodes.conf> file(--split
                             <source>).
 -d,--db <num num...>        database number. multiple databases can be
                             provided. if not specified, all databases
                             will be included.
 -h,--help                   rdt usage.
 -k,--key <regex regex...>   keys to export. this can be a regex. if not
                             specified, all keys will be returned.
 -m,--merge <file file...>   merge multi rdb files to one rdb file.
 -o,--out <file>             if --backup <source> or --merge <file
                             file...> specified. the <file> is the target
                             file. if --split <source> specified. the
                             <file> is the target path.
 -s,--split <source>         split rdb to multi rdb files via cluster's
                             <nodes.conf>. eg:
                             /path/to/dump.rdb
                             redis://host:port?authPassword=foobar
                             redis:///path/to/dump
 -t,--type <type type...>    data type to export. possible values are
                             string, hash, set, sortedset, list, module,
                             stream. multiple types can be provided. if
                             not specified, all data types will be
                             returned.
 -v,--version                rdt version.

examples:
 rdt -b ./dump.rdb -o ./dump.rdb1 -d 0 1
 rdt -b redis://127.0.0.1:6379 -o ./dump.rdb -k user.*
 rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash
 rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -t hash -d 0
 rdt -s redis://127.0.0.1:6379 -c ./nodes.conf -o /path/to/folder -d 0

```

### Filter

`rct`, `rdt` and `rmt` all these commands support data filter by `type`,`db` and `key` RegEx(Java style).  
For example:

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -d 0
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -t string hash
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r -d 0 1 -t list
```

### Redis mass insertion

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -r
cat /path/to/dump.aof | /redis/src/redis-cli -p 6379 --pipe

```

### Convert rdb to dump format

```java  
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof
```

### Convert rdb to json format

```java  
rct -f json -s /path/to/dump.rdb -o /path/to/dump.json
```

### Numbers of key in rdb

```java  
rct -f count -s /path/to/dump.rdb -o /path/to/dump.csv
```

### Find top 50 largest keys

```java  
rct -f mem -s /path/to/dump.rdb -o /path/to/dump.mem -l 50
```

### Diff rdb

```java  
rct -f diff -s /path/to/dump1.rdb -o /path/to/dump1.diff
rct -f diff -s /path/to/dump2.rdb -o /path/to/dump2.diff
diff /path/to/dump1.diff /path/to/dump2.diff
```

### Convert rdb to RESP

```java  
rct -f resp -s /path/to/dump.rdb -o /path/to/appendonly.aof
```

### Migrate rdb to remote redis

```java  
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r
```

### Migrate rdb to remote redis cluster

```java  
rmt -s /path/to/dump.rdb -c ./nodes-30001.conf -r
```


### Backup remote redis's rdb

```java  
rdt -b redis://192.168.1.105:6379 -o /path/to/dump.rdb
```

### Filter rdb

```java  
rdt -b /path/to/dump.rdb -o /path/to/filtered-dump.rdb -d 0 -t string
```

### Split rdb via cluster's nodes.conf

```java  
rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -d 0
```

### Merge multi rdb to one

```java  
rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash
```

### Other parameter

More configurable parameter can be modified in `/path/to/redis-cli-tool/conf/redis-cli.conf`

## Hack rmt

### Rmt threading model

The `rmt` command use the following 4 parameters([redis-cli.conf](https://github.com/leonchen83/redis-cli-tool/blob/master/src/main/resources/redis-cli.conf)) to migrate data to remote.  
  
```java  
migrate_batch_size=4096
migrate_threads=4
migrate_flush=yes
migrate_retries=1
```

The most important parameter is `migrate_threads=4`. this means we use the following threading model to migrate data.  

```java  

single redis ----> single redis

+--------------+         +----------+     thread 1      +--------------+
|              |    +----| Endpoint |-------------------|              |
|              |    |    +----------+                   |              |
|              |    |                                   |              |
|              |    |    +----------+     thread 2      |              |
|              |    |----| Endpoint |-------------------|              |
|              |    |    +----------+                   |              |
| Source Redis |----|                                   | Target Redis |
|              |    |    +----------+     thread 3      |              |
|              |    |----| Endpoint |-------------------|              |
|              |    |    +----------+                   |              |
|              |    |                                   |              |
|              |    |    +----------+     thread 4      |              |
|              |    +----| Endpoint |-------------------|              |
+--------------+         +----------+                   +--------------+

``` 

```java  

single redis ----> redis cluster

+--------------+         +----------+     thread 1      +--------------+
|              |    +----| Endpoints|-------------------|              |
|              |    |    +----------+                   |              |
|              |    |                                   |              |
|              |    |    +----------+     thread 2      |              |
|              |    |----| Endpoints|-------------------|              |
|              |    |    +----------+                   |              |
| Source Redis |----|                                   | Redis cluster|
|              |    |    +----------+     thread 3      |              |
|              |    |----| Endpoints|-------------------|              |
|              |    |    +----------+                   |              |
|              |    |                                   |              |
|              |    |    +----------+     thread 4      |              |
|              |    +----| Endpoints|-------------------|              |
+--------------+         +----------+                   +--------------+

``` 

The difference between cluster migration and single migration is `Endpoint` and `Endpoints`. In cluster migration the `Endpoints` contains multi `Endpoint` to point to every `master` instance in cluster. For example:  
  
3 masters 3 replicas redis cluster. if `migrate_threads=4` then we have `3 * 4 = 12` connections that connected with `master` instance. 

### Migration performance

The following 3 parameters affect migration performance  
  
```java  
migrate_batch_size=4096
migrate_retries=1
migrate_flush=yes
```

By default we use redis `pipeline` to migrate data to remote. the `migrate_batch_size` is the `pipeline` batch size. if `migrate_batch_size=1` then the `pipeline` devolved into 1 single command to sent and wait the response from remote.
The `migrate_retries=1` means if socket error occurred. we recreate a new socket and retry to send that failed command to target redis with `migrate_retries` times. 
The `migrate_flush=yes` means we write every 1 command to socket. then we invoke `SocketOutputStream.flush()` immediately. if `migrate_flush=no` we invoke `SocketOutputStream.flush()` when write to socket every 64KB. notice that this parameter also affect `migrate_retries`. the `migrate_retries` only take effect when `migrate_flush=yes` 

### Migration principle

```java  

+---------------+             +-------------------+    restore      +---------------+ 
|               |             | redis dump format |---------------->|               |
|               |             |-------------------|    restore      |               |
|               |   convert   | redis dump format |---------------->|               |
|    Dump rdb   |------------>|-------------------|    restore      |  Targe Redis  |
|               |             | redis dump format |---------------->|               |
|               |             |-------------------|    restore      |               |
|               |             | redis dump format |---------------->|               |
+---------------+             +-------------------+                 +---------------+
```

### limitation of cluster migration

We use cluster's `nodes.conf` to migrate data to cluster. because of we did't handle the `MOVED` `ASK` redirection. so the only limitation is that the cluster **MUST** in stable state during the migration. this means the cluster **MUST** have no `migrating`, `importing` slot and no switch slave to master. 

