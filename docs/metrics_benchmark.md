# 延迟采样优化验证（2026-09-13）

`RecordLatency` 改用 8 个固定原子分片和 10000 个共享原子槽，移除每次记录的全局 mutex。计数和累计均值覆盖全部记录；多线程分位数是近似近期样本，不保证严格的全局最近 10000 条。停机语义与采样一致性说明见 [architecture.md](architecture.md)。

## 环境与方法

- 容器：`minirpc-linux`，Linux aarch64，10 个逻辑 CPU，内核 `6.12.54-linuxkit`，GCC 13.3.0，C++17 Release / `-O3 -DNDEBUG`。
- 基线：本轮修改前的 mutex 采样实现；端到端基线保留修改前构建的可执行文件。构建缓存中的 commit 标签不能独立代表未提交工作区内容。
- 微基准：`benchmark/metrics_bench.cpp`；每个写者记录 5000000 次，1 / 5 个写者，各运行 5 轮。带 reader 的场景每次 `Snapshot()` 后休眠 1 ms。线程创建和 reader 收尾不计入计时，写者通过同一个 gate 起跑；吞吐按总记录数除以墙钟时间计算。
- 微基准的修改前后使用同一份 benchmark 源码和优化级别。没有固定 CPU 亲和性或频率，单轮结果存在波动。

```sh
docker exec minirpc-linux bash -c 'cd /work && cmake --build build -j --target metrics_bench && build/metrics_bench'
```

## 采样微基准

单位：百万次记录/秒；表格为 5 轮中位数。这里只包含采样成本，不是 RPC QPS。

| 写者 | Snapshot reader | 修改前 | 修改后 | 吞吐比 |
| --- | --- | --- | --- | --- |
| 1 | 无 | 134.371 | 280.686 | 2.09× |
| 1 | 有 | 136.680 | 285.457 | 2.09× |
| 5 | 无 | 43.905 | 375.545 | 8.55× |
| 5 | 有 | 44.643 | 431.731 | 9.67× |

## 端到端结果

固定 Reactor 场景：32 条连接、每轮 100000 请求、16B payload、无 handler 延迟、3000 ms timeout。两版每组均为 500000 次成功、0 失败、0 拒绝、0 超时。

先分别连续运行 5 轮，QPS 中位数从 **84230.4** 变为 **72295.7**。随后交替运行两版，每轮翻转执行顺序，得到：

| 配对轮次 | 修改前 QPS | 修改后 QPS |
| --- | --- | --- |
| 1 | 62599.4 | 84348.2 |
| 2 | 74180.4 | 81477.9 |
| 3 | 81483.7 | 89813.2 |
| 4 | 89674.2 | 84120.1 |
| 5 | 87857.9 | 82973.6 |
| 中位数 | 81483.7 | 84120.1 |

两种执行顺序给出的变化方向不一致，当前数据不能证明端到端 QPS 有稳定提升，也不能把约 8.4 万 QPS 的瓶颈归因于采样锁。此次可确认的收益是移除了采样 mutex，并提高单写者和并发写者的微基准吞吐。端到端结论需要独立控制负载与 CPU 调度后的重复测量。

```sh
./build/rpc_bench --server reactor --connections 32 --requests 100000 \
  --runs 5 --payload-size 16 --handler-delay-ms 0 --timeout-ms 3000 --port 19920
```

## 正确性

Docker 全部 16 项测试通过。新增覆盖线程池排空、资源释放、重复启停、并发 Post、RPC 硬停时排队 handler 完成、启动失败回滚后重启，以及延迟采样的负值/零值、极大值、环绕、并发查询与累计计数。

## 原始微基准数据

修改前：

```csv
writers,snapshot_reader,round,million_samples_per_second
1,0,1,136.751
1,0,2,134.371
1,0,3,137.147
1,0,4,124.249
1,0,5,120.604
1,0,median,134.371
1,1,1,126.792
1,1,2,136.680
1,1,3,137.110
1,1,4,136.938
1,1,5,135.225
1,1,median,136.680
5,0,1,44.995
5,0,2,42.776
5,0,3,44.455
5,0,4,39.876
5,0,5,43.905
5,0,median,43.905
5,1,1,42.941
5,1,2,43.646
5,1,3,44.643
5,1,4,46.888
5,1,5,45.660
5,1,median,44.643
```

修改后：

```csv
writers,snapshot_reader,round,million_samples_per_second
1,0,1,300.100
1,0,2,325.291
1,0,3,280.686
1,0,4,119.375
1,0,5,230.626
1,0,median,280.686
1,1,1,285.811
1,1,2,228.857
1,1,3,285.457
1,1,4,285.507
1,1,5,167.431
1,1,median,285.457
5,0,1,431.482
5,0,2,328.993
5,0,3,301.370
5,0,4,375.545
5,0,5,396.238
5,0,median,375.545
5,1,1,495.190
5,1,2,431.731
5,1,3,412.919
5,1,4,488.137
5,1,5,349.266
5,1,median,431.731
```

<details>
<summary>端到端原始输出</summary>

### 连续 5 轮：修改前

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=5 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       72384       360       720      1406     54578         0     1.132     3.663         0         0  0:100000
reactor            2      32    100000       93500       325       558       705      3778         0     0.944     3.008         0         0  0:100000
reactor            3      32    100000       82745       349       635       910     24643         0     1.043     3.276         0         0  0:100000
reactor            4      32    100000       84230       341       644       891     25138         0     1.065     3.361         0         0  0:100000
reactor            5      32    100000       88800       335       575       728     26734         0     1.005     3.099         0         0  0:100000

summary (performance=median, counters=sum across 5 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            5      32    100000       84230       341       635       891     25138         0     1.043     3.276         0         0  0:500000

[reactor summary, runs=5]
requests_per_run: 100000
success_sum: 500000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1187.22
qps_median: 84230.4
avg_latency_median_ms: 0.379
p50_latency_median_ms: 0.341
p95_latency_median_ms: 0.635
p99_latency_median_ms: 0.891
max_latency_median_ms: 25.138
```

### 连续 5 轮：修改后

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=5 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       69900       379       843      1759     19342         0     1.192     3.612         0         0  0:100000
reactor            2      32    100000       72295       373       855      1826     13670         0     1.172     3.575         0         0  0:100000
reactor            3      32    100000       79588       356       736      1362      3689         0     1.022     3.222         0         0  0:100000
reactor            4      32    100000       78243       367       751      1302      3018         0     1.053     3.333         0         0  0:100000
reactor            5      32    100000       62322       427       891      1963     45650         0     1.320     4.015         0         0  0:100000

summary (performance=median, counters=sum across 5 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            5      32    100000       72295       373       843      1759     13670         0     1.172     3.575         0         0  0:500000

[reactor summary, runs=5]
requests_per_run: 100000
success_sum: 500000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1383.21
qps_median: 72295.7
avg_latency_median_ms: 0.441
p50_latency_median_ms: 0.373
p95_latency_median_ms: 0.843
p99_latency_median_ms: 1.759
max_latency_median_ms: 13.670
```

### before / 1

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       62599       397       904      1818     49717         0     1.253     3.961         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       62599       397       904      1818     49717         0     1.253     3.961         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1597.46
qps_median: 62599.4
avg_latency_median_ms: 0.510
p50_latency_median_ms: 0.397
p95_latency_median_ms: 0.904
p99_latency_median_ms: 1.818
max_latency_median_ms: 49.717
```
### after / 1

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       84348       330       603      1156     41578         0     1.052     3.219         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       84348       330       603      1156     41578         0     1.052     3.219         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1185.56
qps_median: 84348.2
avg_latency_median_ms: 0.378
p50_latency_median_ms: 0.330
p95_latency_median_ms: 0.603
p99_latency_median_ms: 1.156
max_latency_median_ms: 41.578
```
### before / 2

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       74180       379       805      1400     14956         0     1.120     3.492         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       74180       379       805      1400     14956         0     1.120     3.492         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1348.06
qps_median: 74180.4
avg_latency_median_ms: 0.430
p50_latency_median_ms: 0.379
p95_latency_median_ms: 0.805
p99_latency_median_ms: 1.400
max_latency_median_ms: 14.956
```
### after / 2

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       81477       361       684      1092      3821         0     1.041     3.156         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       81477       361       684      1092      3821         0     1.041     3.156         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1227.33
qps_median: 81477.9
avg_latency_median_ms: 0.392
p50_latency_median_ms: 0.361
p95_latency_median_ms: 0.684
p99_latency_median_ms: 1.092
max_latency_median_ms: 3.821
```
### before / 3

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       81483       353       710      1235      5252         0     1.080     3.200         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       81483       353       710      1235      5252         0     1.080     3.200         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1227.24
qps_median: 81483.7
avg_latency_median_ms: 0.392
p50_latency_median_ms: 0.353
p95_latency_median_ms: 0.710
p99_latency_median_ms: 1.235
max_latency_median_ms: 5.252
```
### after / 3

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       89813       332       607       837      3476         0     0.969     3.109         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       89813       332       607       837      3476         0     0.969     3.109         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1113.42
qps_median: 89813.2
avg_latency_median_ms: 0.355
p50_latency_median_ms: 0.332
p95_latency_median_ms: 0.607
p99_latency_median_ms: 0.837
max_latency_median_ms: 3.476
```
### before / 4

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       89674       336       600       846      3775         0     1.015     2.993         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       89674       336       600       846      3775         0     1.015     2.993         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1115.15
qps_median: 89674.2
avg_latency_median_ms: 0.356
p50_latency_median_ms: 0.336
p95_latency_median_ms: 0.600
p99_latency_median_ms: 0.846
max_latency_median_ms: 3.775
```
### after / 4

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       84120       349       629       847     21846         0     1.032     3.225         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       84120       349       629       847     21846         0     1.032     3.225         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1188.78
qps_median: 84120.1
avg_latency_median_ms: 0.379
p50_latency_median_ms: 0.349
p95_latency_median_ms: 0.629
p99_latency_median_ms: 0.847
max_latency_median_ms: 21.846
```
### before / 5

```text
commit=75dea26-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       87857       338       616       814     13903         0     0.993     3.185         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       87857       338       616       814     13903         0     0.993     3.185         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1138.20
qps_median: 87857.9
avg_latency_median_ms: 0.363
p50_latency_median_ms: 0.338
p95_latency_median_ms: 0.616
p99_latency_median_ms: 0.814
max_latency_median_ms: 13.903
```
### after / 5

```text
commit=8888234-dirty kernel=6.12.54-linuxkit compiler="13.3.0" reactor_io_backend=epoll coroutine_io_backend=epoll
host=127.0.0.1 payload_size=16 connections=32 requests_per_run=100000 runs=1 handler_delay_ms=0 timeout_ms=3000

raw runs:
server           run    conn       req         qps     p50us     p95us     p99us     maxus    failed   cpu_usr   cpu_sys  rejected   timeout  statuses
reactor            1      32    100000       82973       353       657       962     11364         0     1.013     3.285         0         0  0:100000

summary (performance=median, counters=sum across 1 runs):
server          runs    conn   req/run     qps_med p50us_med p95us_med p99us_med maxus_med  fail_sum usr_s_med sys_s_med   rej_sum  tout_sum  statuses_sum
reactor            1      32    100000       82973       353       657       962     11364         0     1.013     3.285         0         0  0:100000

[reactor summary, runs=1]
requests_per_run: 100000
success_sum: 100000
failed_sum: 0
rejected_sum: 0
timeout_sum: 0
duration_median_ms: 1205.20
qps_median: 82973.6
avg_latency_median_ms: 0.385
p50_latency_median_ms: 0.353
p95_latency_median_ms: 0.657
p99_latency_median_ms: 0.962
max_latency_median_ms: 11.364
```

</details>
