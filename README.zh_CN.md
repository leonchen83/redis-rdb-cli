# redis-rdb-cli

## 一个可以解析, 过滤, 分割, 合并 rdb 离线内存分析的工具.

## 下载

[binary releases](https://github.com/leonchen83/redis-rdb-cli/releases)

## 运行时依赖

```java  
jdk 1.8+
```

## 安装

```java  
wget https://github.com/leonchen83/redis-rdb-cli/releases/download/${version}/redis-rdb-cli-release.zip
unzip redis-rdb-cli-release.zip
cd .redis-rdb-cli/bin
./rct -h
```

## 手动编译依赖

```java  

jdk 1.8+
maven-3.3.1+

```

## 编译 & 运行

```java  
git clone https://github.com/leonchen83/redis-rdb-cli.git
cd redis-rdb-cli
mvn clean install -Dmaven.test.skip=true
cd target/redis-rdb-cli-release/redis-rdb-cli/bin
./rct -h 
```

## 在docker中运行

```java  
docker run -it --rm redisrdb/redis-rdb-cli
rct -v
```

## 设置Windows环境变量
  
把 `/path/to/redis-rdb-cli/bin` 添加到 `Path` 中
  
### 使用

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

### 过滤

`rct`, `rdt` 和 `rmt` 这3个命令支持`type`,`db` 和 `key`正则表达式(Java风格)数据过滤  
举例如下:  

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -d 0
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -t string hash
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r -d 0 1 -t list
```

### Redis大量数据插入

```java  

rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof -r
cat /path/to/dump.aof | /redis/src/redis-cli -p 6379 --pipe

```

### 把rdb转换成dump格式

```java  
rct -f dump -s /path/to/dump.rdb -o /path/to/dump.aof
```

### 把rdb转换成json格式

```java  
rct -f json -s /path/to/dump.rdb -o /path/to/dump.json
```

### rdb的key数量统计

```java  
rct -f count -s /path/to/dump.rdb -o /path/to/dump.csv
```

### 找到占用内存最大的50个key

```java  
rct -f mem -s /path/to/dump.rdb -o /path/to/dump.mem -l 50
```

### Diff rdb

```java  
rct -f diff -s /path/to/dump1.rdb -o /path/to/dump1.diff
rct -f diff -s /path/to/dump2.rdb -o /path/to/dump2.diff
diff /path/to/dump1.diff /path/to/dump2.diff
```

### 把rdb转换成RESP格式

```java  
rct -f resp -s /path/to/dump.rdb -o /path/to/appendonly.aof
```

### 同步rdb到远端redis

```java  
rmt -s /path/to/dump.rdb -m redis://192.168.1.105:6379 -r
```

### 同步rdb到远端redis集群

```java  
rmt -s /path/to/dump.rdb -c ./nodes-30001.conf -r
```
  
或者不用 `nodes-30001.conf` 这个配置文件, 直接使用如下命令  
  
```java  
rmt -s /path/to/dump.rdb -m redis://127.0.0.1:30001 -r
```

### 备份远端redis的rdb

```java  
rdt -b redis://192.168.1.105:6379 -o /path/to/dump.rdb
```

### 过滤rdb

```java  
rdt -b /path/to/dump.rdb -o /path/to/filtered-dump.rdb -d 0 -t string
```

### 通过集群的nodes.conf把1个rdb分割成多个rdb

```java  
rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -d 0
```

### 合并多个rdb成1个

```java  
rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash
```

### 其他参数

更多的可配置参数可以在 `/path/to/redis-rdb-cli/conf/redis-rdb-cli.conf` 这里配置

## Dashboard

从 `v0.1.9` 起, `rct -f mem` 支持在grafana上显示结果  
![img](./images/memory-dashboard.png)  

如果你想开启这项功能. **必须** 先安装 `docker` 和 `docker-compose`, 安装方法请参照 [docker](https://docs.docker.com/install/)  
然后遵循如下的步骤:  

```java  
cd /path/to/redis-rdb-cli/dashboard
docker-compose up -d
```
  
`cd /path/to/redis-rdb-cli/conf/redis-rdb-cli.conf`  
把 [metric_gateway](https://github.com/leonchen83/redis-rdb-cli/blob/master/src/main/resources/redis-rdb-cli.conf#L149) 这个参数从 `none` 改成 `prometheus`  
  
浏览器打开 `http://localhost:3000` 来查看 `rct -f mem` 命令的结果.  
  
结束!  
  
## Hack rmt

### Rmt 线程模型

`rmt`使用下面四个参数([redis-rdb-cli.conf](https://github.com/leonchen83/redis-rdb-cli/blob/master/src/main/resources/redis-rdb-cli.conf))来同步数据到远端.  
  
```java  
migrate_batch_size=4096
migrate_threads=4
migrate_flush=yes
migrate_retries=1
```

最重要的参数是 `migrate_threads=4`. 这意味着我们用如下的线程模型同步数据  

```java  

单 redis ----> 单 redis

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

单 redis ----> redis 集群

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

上面两张图的不同点在 `Endpoint` 和 `Endpoints`. 在集群同步中 `Endpoints` 包含多个 `Endpoint`, 每个`Endpoint` 和集群中的 `master` 链接, 举例如下:  
  
集群中有 3 master 3 replica. 如果 `migrate_threads=4` 那么我们有 `3 * 4 = 12` 个连接与redis集群相连. 

### 同步性能

下面3个参数影响同步性能  
  
```java  
migrate_batch_size=4096
migrate_retries=1
migrate_flush=yes
```

默认我们使用redis的 `pipeline` 来同步数据. `migrate_batch_size` 就是 `pipeline` 批处理大小. 如果 `migrate_batch_size=1` 那么 `pipeline` 的大小就退化成处理单条命令并同步等待命令结果返回. 
`migrate_retries=1` 意思是如果 socket 连接错误发生. 我们重建一个新的 socket 并重试1次把上次发送失败的命令重新发送一遍. 
`migrate_flush=yes` 意思是我们每写入socket一条命令之后, 立即调用一次 `SocketOutputStream.flush()`. 如果 `migrate_flush=no` 我们每写入 64KB 到 socket 才调用一次 `SocketOutputStream.flush()`. 请注意这个参数影响 `migrate_retries`. `migrate_retries` 只有在 `migrate_flush=yes` 的时候生效. 

### 同步原理

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

### 同步到集群的限制

我们通过集群的 `nodes.conf` 文件来同步数据到集群. 因为我们没有处理 `MOVED` `ASK` 重定向. 因此唯一的限制是集群在同步期间 **必须** 是稳定的状态. 这意味着集群 **必须** 不存在 `migrating`, `importing` 这样的slot. 而且没有主从切换. 

