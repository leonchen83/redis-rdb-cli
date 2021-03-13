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