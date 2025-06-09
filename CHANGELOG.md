### 0.9.7

Add support for redis 8.0.  

### 0.9.6

Fix `rdt` convert db bug.  

### 0.9.5

Fix full sync offset bug. this bug could cause losing data in reconnection  
Support for java 21.  
Docker support Apple silicon.  

### 0.9.4

Add command option `--ignore-ttl`.  
Add support for redis 7.2.  

### 0.9.3

Add command description.  
Add support for `SCAN` mode.

### 0.9.2

Fix Lzf compress bug.  
Upgrade dependencies.  

### 0.9.1

Support for redis 7.0-GA.  
Fix `listpack` decode bug.  

### 0.9.0

Fix `redis sentinel` auth and ssl bug.  
  
Support export `jsonl` for `rct -f mem` and `rct -f count`.  
To export jsonl file need to edit `/path/to/redis-rdb-cli/conf/redis-rdb-cli.conf` and change `export_file_format` from `csv` to `jsonl`.  
  
Optimize grafana dashboard and add `top N key` metric to grafana.  
To show `top N key` need to add `--largest` option in `rct` command.  

Support for `enable_progress_bar` config. the value is `true` by default.  

Add new command `rmonitor` to monitor redis server.  

```java  
# standalone or master-slave redis 
$ rmonitor -s redis://127.0.0.1:6379 -n standalone
# cluster
$ rmonitor -s redis://127.0.0.1:30001 -n cluster
# sentinel
$ rmonitor -s redis-sentinel://sntnl-usr:sntnl-pwd@127.0.0.1:26379?master=mymaster&authUser=usr&authPassword=pwd -n sentinel
```

### 0.8.2

Support for redis 7.0-RC2.  
Fix windows cmd running failed bug.  
Fix redis cluster acl bug. see [issue 28](https://github.com/leonchen83/redis-rdb-cli/issues/28).  

### 0.8.1

Fix `rct -f resp` expire time bug.  

### 0.8.0

Support for redis 7.0-RC1.  
Add `rcut` command to handle `aof-use-rdb-preamble` file.  

### 0.7.4

Fix `rct -f dump` can't export correct `dump` aof bug.  

### 0.7.3

Upgrade log4j2 to 2.17.0 to fix Security Vulnerability CVE-2021-45105.  

### 0.7.2

Upgrade log4j2 to 2.16.0 to fix Security Vulnerability CVE-2021-44228.  

### 0.7.1

Upgrade log4j2 to 2.15.0.  

### 0.7.0

1. build native image supported.  
2. Fix connect `sentinel` bug.  
3. Fix progress bar bug.  
4. `rdt -b` add optional `-g` option to convert source db to `<goal>` db.  
5. CLI show version incompatible changes:  
```
# use upper case -V instead of -v to show version.
# redis-rdb-cli<=0.6.5 show the CLI version
rct -v
# redis-rdb-cli>=0.7.0 show the CLI version
rct -V
``` 

### 0.6.5

Optimize `rct -f dump` memory usage.  

### 0.6.4

Fix lzf compress bug.  

### 0.6.3

fix windows cmd running failed bug.  

### 0.6.2

support downgrade migration from redis 6.2 to 2.8.  

### 0.6.1

Fix OOM bug when use `RawByteListener`.  
  
Redis 6.2 support.  
  
1. Add `PXAT/EXAT` arguments to `SET` command.  
2. Add the `CH`, `NX`, `XX` arguments to `GEOADD`.
3. Add the `COUNT` argument to `LPOP` and `RPOP`.
4. Add `SYNC` arg to `FLUSHALL` and `FLUSHDB`, and `ASYNC/SYNC` arg to `SCRIPT FLUSH`
5. Add the `MINID` trimming strategy and the `LIMIT` argument to `XADD` and `XTRIM`  

### 0.6.0

Redis 6.2-rc1 support.  

`rst` command support `COPY,BLMOVE,LMOVE,ZDIFFSTORE,GEOSEARCHSTORE` commands.  

### 0.5.1

Fix `RedisURI` `setAuthUser` bug.  
Change default `readTimeout`, `connectionTimeout`, `timeout` from `30` seconds to `60` seconds.  

### 0.5.0

add self define `FormatterService` that allow user define their own format.  

### 0.4.2

fix critical bug from 0.3.0 - 0.4.1.  
affect `rmt` , `rst` , `ret` command.  
when these command 's target is cluster, migrate data failed.  

### 0.4.1

export unit support. [issue#16](https://github.com/leonchen83/redis-rdb-cli/issues/16).   


### 0.4.0

Jsonl support. [issue#13](https://github.com/leonchen83/redis-rdb-cli/issues/13).   

### 0.3.2

Fix invalid json [issue#12](https://github.com/leonchen83/redis-rdb-cli/issues/12).   

### 0.3.1

Fix invalid json [issue#11](https://github.com/leonchen83/redis-rdb-cli/issues/11).   

### 0.3.0

Support redis 6 ACL.  
Support redis 6 SSL.  

### 0.2.0

Improve dashboard.  
Add `rst` command.  
Add `ret` command.  

### 0.1.19

Improve dashboard

### 0.1.18

Improve dashboard

### 0.1.17

Add sentinel support

### 0.1.16

Fix CLI exit code bug.  

### 0.1.15

Fix offset bug.  

### 0.1.14

Fix `rmt` bug in prior to Redis 3.0.  
Add `rmt -l` parameter.  

### 0.1.13

Support multi instance dashboard.  

### 0.1.12

Support Redis-5.0-GA.  
Upgrade dependencies.  

### 0.1.11

Fix `rct` bug in redis-3.0.  

### 0.1.10

`rmt` support cluster migration.  

### 0.1.9

Add `metric` feature.  

### 0.1.8

Fix `rct -f count` bug.  
Fix `expire` bug.  

### 0.1.7

`rct` add a new format `count`.  

### 0.1.6

`rmt` command add parameter `--config`.   

### 0.1.5

Fix ProgressBar bug.  
Fix `rct -f mem` bug.  

### 0.1.3

Initial commit  