# redis-cli-tool

## 下载

[binary releases](https://github.com/leonchen83/redis-cli-tool/releases)

## 运行时依赖

```java  
jdk 1.8+
```

## 安装

```java  
wget https://github.com/leonchen83/redis-cli-tool/releases/download/${version}/redis-cli-tool.zip
unzip redis-cli-tool.zip
sudo chmod -R 755 ./redis-cli-tool
cd ./redis-cli-tool/bin
./rct -h
```

## 手动编译依赖

```java  

jdk 1.8+
maven-3.3.1+

```

## 编译 & 运行

```java  
    cd redis-cli-tool
    mvn clean install -Dmaven.test.skip=true
    cd target/redis-cli-tool/bin
    ./rct -h 
```

## 设置Windows环境变量
  
把 `/path/to/redis-cli-tool/bin` 添加到 `Path` 中
  
## 使用

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
                             dump, diff, key, keyval, mem and resp
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

usage: rmt -s <source> -m <uri> [-d <num num...>] [-k <regex regex...>]
           [-t <type type...>] [-r]

options:
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
 rmt -s redis://120.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0
 rmt -s ./dump.rdb -m redis://127.0.0.1:6380 -t string -r

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

## 过滤

`rct`, `rdt` 和 `rmt` 这3个命令支持`type`,`db`,`key`正则表达式数据过滤  
举例如下:  

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -d 0
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -t string hash
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r -d 0 1 -t list
```

## Redis大量数据插入

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -r
cat /path/to/dump.aof | /redis/src/redis-cli -p 6379 --pipe

```

## 把rdb转换成dump格式

```java  
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof
```

## 把rdb转换成json格式

```java  
rct -f json -s /path/to/dump.rdb -o /path/to/dump.json
```

## 找到占用内存最大的50个key

```java  
rct -f mem -s /path/to/dump.rdb -o /path/to/dump.mem -l 50
```

## Diff rdb

```java  
rct -f diff -s /path/to/dump1.rdb -o /path/to/dump1.diff
rct -f diff -s /path/to/dump2.rdb -o /path/to/dump2.diff
diff /path/to/dump1.diff /path/to/dump2.diff
```

## 把rdb转换成RESP格式

```java  
rct -f resp -s /path/to/dump.rdb -o /path/to/appendonly.aof
```

## 同步rdb到远端redis

```java  
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r
```

## 备份远端redis的rdb

```java  
rdt -b redis://192.168.1.105:6379 -o /path/to/dump.rdb
```

## 过滤rdb

```java  
rdt -b /path/to/dump.rdb -o /path/to/filtered-dump.rdb -d 0 -t string
```

## 通过集群的nodes.conf把1个rdb分割成多个rdb

```java  
rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -d 0
```

## 合并多个rdb成1个

```java  
rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash
```