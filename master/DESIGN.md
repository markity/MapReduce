# MapReduce Master 设计文档

本文档描述 `master` 模块的整体设计，包括职责边界、HTTP/RPC 协议、调度模型、任务状态机、心跳机制、幂等性、插件生命周期、容灾策略、当前实现边界和后续扩展方向。

文档按当前代码实现为基准编写。对于尚未实现但架构上建议保留的能力，会明确标注为“后续建议”或“待扩展”。

## 1. 设计目标

Master 是整个 MapReduce 系统的控制平面。它不直接执行用户计算逻辑，而是负责接收客户端提交的作业、管理插件、拆分任务、维护 worker 状态、调度 task attempt、处理 task report、推进 job 阶段，并为 worker 提供必要的运行信息。

核心目标如下：

1. 单点维护全局调度状态，避免多线程直接读写复杂共享结构。
2. API 层只处理网络 DTO 和 HTTP 状态码，内部调度逻辑只使用 `master/entity` 数据结构。
3. 任务调度以 attempt 为单位，支持失败后重新调度。
4. Heartbeat 协议同时承载 worker slot snapshot、task reports、cleanup query 和新任务分配。
5. Task report 必须具备幂等能力，worker 重试上报不会重复修改 job 状态。
6. Plugin 删除采用软删除语义，避免正在运行的 job 因插件被删除而无法 fetch。
7. Map output 丢失时，reduce 可以通过 `LostMapOutputs` 通知 master 重跑对应 map task。
8. 当前 master 状态主要在内存中维护；进程级故障恢复属于后续持久化扩展范围。

## 2. 目录结构与职责

```text
master/
  cmd/
    root.go                    # master 命令行入口，配置 listen 和 plugin-store
  entity/
    code.go                    # master 内部通用返回码
    heartbeat.go               # heartbeat 内部协议模型
    job.go                     # job 创建、查询内部模型
    task.go                    # task/job 阶段枚举
    task_spec.go               # split/map/reduce/assigned task 内部模型
  scheduler/
    scheduler.go               # Scheduler 接口、单例、actor run loop
    handle_heartbeat.go        # worker heartbeat、report、slot reconcile
    handle_job.go              # job 创建、列表、详情、阶段推进
    handle_plugin.go           # plugin 注册、删除、列表、cleanup、fetch path
    handle_schedule.go         # runnable queue、task pick、attempt 分配
    job_status.go              # jobStatus、task indexes、状态迁移方法
    plugin_status.go           # plugin 文件加载、排序、安全路径
    woker_status.go            # workerStatus、slotStatus、slot snapshot
  server/
    server.go                  # Gin server 初始化、路由注册、cleanup loop
    client-api.go              # client job API，DTO <-> entity 转换
    worker-api.go              # worker API 路由注册
    clientapis/
      plugin_upload.go         # 上传插件
      plugin_delete.go         # 软删除插件
      plugin_list.go           # 列表插件
    workerapis/
      heartbeat.go             # worker heartbeat DTO <-> entity 转换
      fetch_plugin.go          # worker fetch plugin blob
```

外部 RPC DTO 位于：

```text
rpc/
  comm/                        # 公共 RPC DTO、返回码、task/report/spec
  master/client-call/          # client -> master DTO
  master/worker-call/          # worker -> master DTO
  worker/master-call/          # master -> worker DTO，目前通过 heartbeat response 下发
```

## 3. 分层边界

Master 当前采用三层边界：

1. HTTP server / API 层
2. Scheduler actor 层
3. Entity 内部模型层

### 3.1 API 层

API 层的职责是：

1. 绑定 HTTP 请求。
2. 做基础参数校验。
3. 将 RPC DTO 转成 `master/entity`。
4. 调用 `scheduler.Scheduler` 接口。
5. 将 entity 输出转回 RPC DTO。
6. 设置正确 HTTP status code。

API 层可以引用：

```go
mapreduce/rpc/...
mapreduce/master/entity
mapreduce/master/scheduler
```

API 层不应该直接修改 scheduler 内部结构。

### 3.2 Scheduler 层

Scheduler 层是 master 的核心状态机。它通过 channel 接收输入事件，并在单 goroutine `runLoopForever` 中串行处理。

Scheduler 层可以引用：

```go
mapreduce/master/entity
```

Scheduler 层不应该引用 `rpc/...` DTO。这样可以保证内部逻辑不被网络协议污染。

### 3.3 Entity 层

`master/entity` 是 master 内部领域模型。它不承载 JSON tag，也不表达 HTTP 语义。

Entity 层包括：

1. `CreateMapReduceJobInput`
2. `HeartbeatInput`
3. `TaskReport`
4. `SplitSpec`
5. `MapTaskSpec`
6. `ReduceTaskSpec`
7. `AssignedTask`
8. `Code`
9. `HeartbeatCode`

### 3.4 DTO 转换原则

DTO 转换只发生在 server 层：

```text
client JSON/RPC DTO
  -> server/client-api.go
  -> master/entity
  -> scheduler
```

```text
worker JSON/RPC DTO
  -> server/workerapis/heartbeat.go
  -> master/entity
  -> scheduler
```

返回方向反过来：

```text
scheduler entity output
  -> server layer converts to RPC DTO
  -> JSON response
```

## 4. Scheduler Actor 模型

Scheduler 使用 actor-like 模型。所有状态变更都通过 channel 进入同一个 goroutine：

```go
func (impl *schedulerImpl) runLoopForever() {
    for {
        select {
        case heartbeatInput := <-impl.heartbeatReqInputChan:
            impl.handleHeartbeatInput(heartbeatInput)
        case createJobInput := <-impl.createJobInputChan:
            impl.handleCreateMapReduceJobInput(createJobInput)
        ...
        }
    }
}
```

### 4.1 为什么用 actor 模型

调度器同时维护以下状态：

1. worker slots
2. job tasks
3. runnable job queue
4. plugin registry
5. in-flight attempts
6. acked reports

这些状态之间存在强耦合。例如一个 heartbeat report 成功后，需要同时：

1. 修改 task state。
2. 从 in-flight index 删除 attempt。
3. 更新 map output meta。
4. 释放 worker slot。
5. 推进 job stage。
6. 刷新 runnable job queue。

如果使用多个 mutex，很容易出现锁顺序、局部状态不一致、忘记刷新队列等问题。Actor 模型通过串行化事件处理降低复杂度。

### 4.2 外部并发模型

HTTP handler 可以并发调用 Scheduler 接口，但接口内部会把请求送入 channel，并等待响应 channel：

```go
func (impl *schedulerImpl) CreateMapReduceJob(req *entity.CreateMapReduceJobInput) *entity.CreateMapReduceJobOutput {
    c := make(chan *entity.CreateMapReduceJobOutput, 1)
    impl.createJobInputChan <- &createMapReduceJobInput{Req: req, C: c}
    return <-c
}
```

这样调用者看到的是同步接口，内部实现仍是串行状态机。

## 5. 核心状态结构

### 5.1 schedulerImpl

`schedulerImpl` 维护全局状态：

```go
type schedulerImpl struct {
    workerStatus map[string]*workerStatus
    jobStatus map[string]*jobStatus
    runnableJobQueue *list.List
    runnableJobIndex map[string]*list.Element
    pluginStatus map[string]*pluginStatus
    nextJobSeq uint64
    nextAttemptSeq uint64
}
```

重要索引：

1. `workerStatus`: worker unique id -> worker 状态。
2. `jobStatus`: job id -> job 状态。
3. `runnableJobQueue`: 可运行 job 的 FIFO/round-robin 队列。
4. `runnableJobIndex`: job id -> list element，用于 O(1) 判断 job 是否已经在队列中。
5. `pluginStatus`: plugin unique id -> plugin 状态。

### 5.2 jobStatus

`jobStatus` 是一个 job 的所有调度状态：

```go
type jobStatus struct {
    JobID string
    JobName string
    PluginUniqueID string
    PluginFilePath string
    NumReduceTasks int
    JobStage entity.JobStageCode

    AllMapTasks map[string]*taskStatus
    PendingMapTasks map[string]struct{}
    InFlightMapTasks map[string]map[entity.TaskAttemptKey]struct{}
    AllAvailableMapOutputs map[string][]entity.MapOutputMetaEntry

    AllReduceTasks map[string]*taskStatus
    PendingReduceTasks map[string]struct{}
    InFlightReduceTasks map[string]map[entity.TaskAttemptKey]struct{}

    AckedReports map[entity.TaskAttemptKey]struct{}
    LastError string
}
```

设计原则：

1. `AllMapTasks` / `AllReduceTasks` 是 task 对象事实源。
2. `PendingMapTasks` / `PendingReduceTasks` 只保存 task id set。
3. `InFlightMapTasks` / `InFlightReduceTasks` 使用 `taskID -> attempt set`，允许同一个 logical task 同时存在多个 attempt，为后续 speculative execution/长尾优化预留语义。
4. `AllAvailableMapOutputs` 按 map task id 维护仍可被 reduce 拉取的输出 attempt 列表。
5. 不维护 `SucceededXXXTasks`，成功状态由 `AllXXXTasks[taskID].State` 表达。
6. 一个 logical task 可以有多个 in-flight attempt；一旦某个 attempt 成功，同 task 的其它 in-flight attempt 会从 master 事实状态中清除，并通过 `ShouldStopAttempts` 要求 worker 停止。

### 5.3 taskStatus

```go
type taskStatus struct {
    TaskType entity.TaskType
    JobID string
    TaskID string
    AttemptID string
    State taskRunState
    Error string
    MapTaskSpec *entity.MapTaskSpec
    ReduceTaskSpec *entity.ReduceTaskSpec
}
```

`taskStatus` 保存 task 的稳定身份和当前 attempt。

`TaskID` 是逻辑 task ID，例如：

1. `map-0`
2. `map-1`
3. `reduce-0`
4. `reduce-1`

`AttemptID` 是每次调度生成的执行尝试 ID。一个 task 失败后重新调度，会获得新的 attempt id。

### 5.4 entity.TaskAttemptKey

```go
type TaskAttemptKey struct {
    JobID string
    TaskID string
    AttemptID string
}
```

`TaskAttemptKey` 位于 `master/entity`，用于区分同一 task 的不同执行尝试。它和 `TaskAttemptKey` 一样属于稳定领域身份，不依赖 scheduler 的具体实现；scheduler 用它作为 in-flight attempt 的 value，key 仍然是 logical `taskID`。

例如：

```text
job-1 map-0 attempt-1
job-1 map-0 attempt-2
```

这两个 attempt 对应同一个逻辑 map task，但只有当前 in-flight attempt 的 report 才能真正改变 task 状态。

### 5.5 workerStatus

```go
type workerStatus struct {
    UniqueID string
    Addr string
    Epoch int64
    LastSeq uint64
    HasSeq bool
    AllSlots map[string]*slotStatus
    FreeSlots map[string]struct{}
    BusySlots map[string]struct{}
}
```

设计要点：

1. `Epoch` 用于识别 worker 重启。
2. `Seq` 用于避免旧 heartbeat 覆盖新的 slot snapshot。
3. `HasSeq` 用于区分“尚未接受过任何 seq”和“已经接受过 seq=0”。
4. `AllSlots` 保存 slot 状态。
5. `FreeSlots` 和 `BusySlots` 是 slot availability 索引。

## 6. Job 生命周期

Job 当前有四种阶段：

```go
const (
    JobStageCodeMapping   JobStageCode = "mapping"
    JobStageCodeReducing  JobStageCode = "reducing"
    JobStageCodeSucceeded JobStageCode = "succeed"
    JobStageCodeKilled    JobStageCode = "killed"
)
```

当前实现没有 kill job API，`killed` 是预留状态。

### 6.1 创建阶段

客户端通过 `POST /client-api/jobs` 创建 job。

请求必须包含：

1. `job_name`
2. `plugin_unique_id`
3. `num_reduce_tasks`
4. `task_splits`

`plugin_unique_id` 不能为空；`task_splits` 不能为空；`num_reduce_tasks` 不能小于 0。

创建流程：

```text
client request
  -> bind CreateMapReduceJobReq
  -> convert to entity.CreateMapReduceJobInput
  -> scheduler.handleCreateMapReduceJobInput
  -> validate plugin visible
  -> create jobStatus
  -> materialize map tasks from task_splits
  -> materialize reduce tasks from NumReduceTasks
  -> refreshRunnableJob
  -> return job id
```

### 6.2 Task 物化

Map tasks 从 `TaskSplits` 创建：

```text
split[0] -> map-0
split[1] -> map-1
split[2] -> map-2
```

Reduce tasks 从 `NumReduceTasks` 创建：

```text
partition 0 -> reduce-0
partition 1 -> reduce-1
partition 2 -> reduce-2
```

创建后：

1. 所有 map task 进入 `AllMapTasks`。
2. 所有 map task id 进入 `PendingMapTasks`。
3. 所有 reduce task 进入 `AllReduceTasks`。
4. 所有 reduce task id 进入 `PendingReduceTasks`。

虽然 reduce task 在创建时已经 pending，但调度器只会在 map 阶段完成后调度 reduce。

### 6.3 Mapping 阶段

只要存在 pending map task，job 就可运行。

调度优先级：

```text
pending map task
  before
pending reduce task
```

原因：

1. reduce 依赖所有 map outputs。
2. reduce fetch 失败时，可能重新生成 pending map task。
3. 即使 job 已进入 reducing 阶段，只要有 pending map task，也必须优先补跑 map。

### 6.4 Reducing 阶段

Map 阶段完成条件：

```text
len(AllMapTasks) > 0
and len(PendingMapTasks) == 0
and len(InFlightMapTasks) == 0
```

当 map 阶段完成且存在 reduce task 时，job 进入 reducing。

Reduce task 被调度时会带上当前 `job.MapOutputMetas`：

```go
ReduceTaskSpec{
    ReducePartitionID: partitionID,
    MapOutputs: job.MapOutputMetas,
}
```

Worker 端应遍历 `MapOutputs`，并用每个 map output 的 `TaskAttemptKey` 加上当前 `ReducePartitionID` 去对应 worker 拉取分区数据。

### 6.5 Succeeded 阶段

Job 完成条件：

```text
map done
and reduce done
```

如果 `NumReduceTasks == 0`，当前设计允许 map-only job，此时：

```text
allMapsDone == true
allReducesDone == true because anyReduce == false
```

因此 map-only job 在所有 map task 完成后可直接进入 succeeded。

## 7. Task 状态迁移

### 7.1 状态枚举

```go
const (
    taskRunStatePending   taskRunState = "pending"
    taskRunStateRunning   taskRunState = "running"
    taskRunStateSucceeded taskRunState = "succeeded"
)
```

当前没有单独持久化 failed 状态。失败 task 会重新进入 pending。

### 7.2 状态迁移图

```text
                  assign attempt
      +---------+ --------------> +---------+
      | pending |                 | running |
      +---------+ <-------------- +---------+
          ^          failed / lost     |
          |                            |
          +----------------------------+
                     succeeded
                         |
                         v
                   +-----------+
                   | succeeded |
                   +-----------+
```

### 7.3 markTaskPending

`markTaskPending` 做的事情：

1. 设置 `task.State = pending`。
2. 清除同一 task 的所有 in-flight attempts。
3. 写入 `AllMapTasks` 或 `AllReduceTasks`。
4. 写入 `PendingMapTasks` 或 `PendingReduceTasks`。

它适用于：

1. task 初始创建。
2. task 执行失败。
3. worker restart。
4. slot snapshot 表明 task 丢失。
5. reduce fetch 失败导致 map output 丢失。

### 7.4 markTaskInFlight

`markTaskInFlight` 做的事情：

1. 设置 `task.State = running`。
2. 清除同一 task 的旧 attempt。
3. 从 pending set 删除 task id。
4. 写入 `InFlightMapTasks` 或 `InFlightReduceTasks`。

它适用于 master 给 worker 分配新 attempt。

### 7.5 markTaskSuccess

`markTaskSuccess` 做的事情：

1. 设置 `task.State = succeeded`。
2. 清除同一 task 的所有 in-flight attempts。
3. 从 pending set 删除 task id。
4. 写入 `AllMapTasks` 或 `AllReduceTasks`。

成功 task 不再维护独立 succeeded set。

## 8. 调度模型

### 8.1 Runnable Job Queue

Master 维护一个真正的双向链表：

```go
runnableJobQueue *list.List
runnableJobIndex map[string]*list.Element
```

队列元素是 `jobID`。

为什么不用扫描所有 job：

1. 每次 heartbeat 都扫描所有 job 会导致调度复杂度随 job 数增长。
2. 大量 job 时会造成不必要的 CPU 消耗。
3. runnable queue 可以快速定位“有任务可调度”的 job。

### 8.2 入队规则

`refreshRunnableJob(job)` 会检查：

```go
job.hasRunnableTask()
```

如果 job 有 runnable task 且不在队列中，则入队。

如果 job 没有 runnable task，则从队列中移除。

### 8.3 hasRunnableTask

一个 job runnable 的条件：

1. job 不是 terminal。
2. 有 pending map task；或
3. 有 runnable reduce task。

Runnable reduce task 还要求：

1. job stage 是 reducing。
2. map tasks 全部完成。
3. 有 pending reduce task。

### 8.4 分配过程

分配发生在 heartbeat response 中：

```text
accepted heartbeat slot snapshot
  -> assignTasksToWorker(worker)
  -> for each free slot:
       pop runnable job
       pick pending map first
       else pick runnable reduce
       generate attempt id
       mark task in-flight
       mark slot busy
       append AssignedTask
```

只有 seq 合法的 heartbeat 才允许分配新任务。seq backoff 的 heartbeat 仍处理 report，但不分配任务。

### 8.5 Round-robin 公平性

当前 runnable queue 是 job 级队列。

每次 `popRunnableJob` 会从队头取一个 job。如果该 job 仍然有 runnable task，则分配一个 task。分配后如果 job 仍有 runnable task，`refreshRunnableJob` 会把它放回队尾。

这形成 job 级 round-robin：

```text
jobA -> jobB -> jobC -> jobA -> jobB ...
```

它避免单个大 job 长时间占满所有 slot。

### 8.6 当前调度策略限制

当前调度策略不考虑：

1. 数据本地性。
2. worker 负载差异。
3. task 运行时间估计。
4. speculative execution。
5. worker blacklist。
6. job priority。

这些可以在后续扩展。

## 9. Heartbeat 协议

Worker 通过 `POST /worker-api/heartbeat` 与 master 通信。

Heartbeat 同时承担以下职责：

1. 注册/刷新 worker。
2. 上报 worker epoch。
3. 上报 slot snapshot。
4. 上报 task reports。
5. 查询哪些 job 的中间文件可以 cleanup。
6. 接收 master 分配的新 tasks。

### 9.1 Request DTO

RPC DTO:

```go
type HeartbeatReq struct {
    WorkerUniqueID string `json:"worker_unique_id"`
    WorkerEpoch int64 `json:"worker_epoch"`
    WorkerAddr string `json:"worker_addr"`
    Seq uint64 `json:"seq"`
    SlotStatus map[string]comm.TaskSlotStatus `json:"slots"`
    Reports []comm.TaskReport `json:"reports"`
    CleanupWantedJobs []string `json:"cleanup_wanted_jobs"`
}
```

内部 entity：

```go
type HeartbeatInput struct {
    WorkerUniqueID string
    WorkerEpoch int64
    WorkerAddr string
    Seq uint64
    SlotStatus map[string]TaskSlotStatus
    Reports []TaskReport
    CleanupWantedJobs []string
}
```

### 9.2 Response DTO

RPC DTO:

```go
type HeartbeatResp struct {
    comm.RespComm
    AckedReports []comm.TaskAttemptKey `json:"acked_reports"`
    CleanableJobs []string `json:"cleanable_jobs"`
    AssignedTasks []mastercall.RunTaskReq `json:"assigned_tasks"`
}
```

内部 entity：

```go
type HeartbeatOutput struct {
    Code HeartbeatCode
    AckedReports []TaskAttemptKey
    CleanableJobs []string
    AssignedTasks []AssignedTask
}
```

### 9.3 HeartbeatCode

```go
const (
    HeartbeatCodeOK HeartbeatCode = iota
    HeartbeatCodeSeqBackoffRequest
    HeartbeatCodeEpochStaleRequest
)
```

语义：

1. `OK`: heartbeat 被接受，slot snapshot 可以更新，可以分配任务。
2. `SeqBackoffRequest`: heartbeat 比 master 已接受的 seq 更旧或相同；忽略 slot snapshot，不分配新任务，但仍处理 reports 和 cleanup query。
3. `EpochStaleRequest`: heartbeat 来自旧 worker epoch；直接拒绝，不处理 reports，不处理 cleanup，不分配任务。

### 9.4 Seq 机制

`Seq` 用于解决 heartbeat 后发先至的问题。

例子：

```text
t1: worker sends seq=10, slot A busy
t2: worker sends seq=11, slot A free
t3: master first receives seq=11
t4: master later receives seq=10
```

如果没有 seq，旧的 seq=10 会覆盖新的 slot snapshot，让 master 以为 slot A 仍然 busy。

当前规则：

```text
if worker has never accepted seq:
    accept
else if req.Seq > worker.LastSeq:
    accept
else:
    backoff
```

`HasSeq` 用于支持首个合法 heartbeat 的 `Seq=0`。

### 9.5 Epoch 机制

`WorkerEpoch` 用于识别 worker 重启。

规则：

```text
req.Epoch < master.Epoch:
    stale epoch, reject heartbeat

req.Epoch == master.Epoch:
    normal heartbeat

req.Epoch > master.Epoch:
    worker restarted
    requeue old in-flight attempts on this worker
    replace worker status
    continue processing current heartbeat
```

### 9.6 Slot Snapshot

Worker 上报当前所有 slot：

```go
type TaskSlotStatus struct {
    SlotID string `json:"slot_id"`
    CurrentTask *RunningTaskStatus `json:"current_task"`
}
```

`CurrentTask == nil` 表示空闲 slot。

Master 接受 slot snapshot 时：

1. 先 reconcile 旧 running tasks。
2. 再 rebuild worker slot snapshot。

### 9.7 Slot Reconcile

Reconcile 解决这种情况：

```text
master old view:
    slot-1 is running job-1/map-0/attempt-1

new worker heartbeat:
    slot-1 is free
    or slot-1 disappeared
    or slot-1 is running another task
```

如果 master 仍认为旧 attempt 是 in-flight，则说明旧 attempt 丢失，应重新放回 pending：

```text
old task not found in new slots
  -> find job
  -> find in-flight attempt
  -> markTaskPending
  -> refreshRunnableJob
```

这个机制可以处理：

1. worker 本地 task goroutine 崩溃但没 report。
2. worker 本地状态丢失。
3. slot 缩容。
4. worker 自己覆盖了 slot 上的 task。

### 9.8 Reports 不受 Seq Backoff 影响

Seq 只保护 slot snapshot，不保护 reports。

原因：

1. Report 是以 `(jobID, taskID, attemptID)` 为 key 的事件。
2. Report 本身有幂等机制。
3. 旧 heartbeat 里携带的 report 可能是之前没被 ack 的有效 report。

因此即使 heartbeat 返回 `SeqBackoffRequest`，master 仍处理 `Reports`。

### 9.9 Assignment 受 Seq 控制

虽然 reports 不受 seq backoff 影响，但 assignment 必须受 seq 控制。

当前规则：

```text
if slotSnapshotAccepted:
    assignTasksToWorker(worker)
else:
    AssignedTasks = []
```

原因：

1. 分配任务依赖 master 当前认为的 free slots。
2. backoff heartbeat 的 slot snapshot 被忽略。
3. 如果仍然分配任务，就可能基于过期 free slot 发送新 task。

## 10. Task Report 协议

Task report 用于 worker 向 master 汇报 task attempt 结束。

RPC DTO：

```go
type TaskReport struct {
    TaskAttemptKey
    TaskType TaskType `json:"task_type"`
    Status TaskReportStatus `json:"status"`
    Error string `json:"error,omitempty"`
    MapOutput *MapOutputMetaEntry `json:"map_output,omitempty"`
    LostMapOutputs []MapOutputMetaEntry `json:"lost_map_outputs"`
}
```

内部 entity：

```go
type TaskReport struct {
    TaskAttemptKey
    TaskType TaskType
    Status TaskReportStatus
    Error string
    MapOutput *MapOutputMeta
    LostMapOutputs []MapOutputMeta
}
```

### 10.1 TaskAttemptKey

```go
type TaskAttemptKey struct {
    JobID string
    TaskID string
    AttemptID string
}
```

Report key 是幂等 key。

同一个 report 被 worker 重复上报时，master 应返回 ack，但不重复修改状态。

### 10.2 成功 report

Map task 成功：

```text
Status = succeeded
TaskType = map
MapOutput = map attempt output meta
```

Master 行为：

1. 追加 `MapOutput` 到该 map task 的可用 output 列表。
2. `markTaskSuccess(task)`。
3. ack report。
4. 释放 worker slot。
5. refresh job stage。

Reduce task 成功：

```text
Status = succeeded
TaskType = reduce
```

Master 行为：

1. `markTaskSuccess(task)`。
2. ack report。
3. 释放 worker slot。
4. refresh job stage。

### 10.3 失败 report

普通失败：

```text
Status = failed
Error = error message
```

Master 行为：

1. 写入 `task.Error`。
2. 写入 `job.LastError`。
3. `markTaskPending(task)`。
4. ack report。
5. 释放 worker slot。
6. refresh job stage。

### 10.4 Reduce fetch failure

Reduce 失败时可以携带 `LostMapOutputs`：

```text
TaskType = reduce
Status = failed
LostMapOutputs = [map output meta...]
```

这表示 reduce 拉取某些 map output 失败，master 需要重新调度对应 map task。

Master 行为：

1. 将 reduce task 本身重新放回 pending。
2. 对每个 lost output：
   1. 从 `job.MapOutputMetas` 删除对应 map output。
   2. 找到对应 map task。
   3. 将 map task 重新放回 pending。
3. job 保持 reducing 阶段。
4. 刷新 runnable queue。

为什么保持 reducing：

1. Hadoop 语义中 job 已进入 reduce 阶段后，不必整体回退为 mapping。
2. 但调度时 map task 优先于 reduce task。
3. 因此补跑 lost map 后，reduce 会等待 map pending/in-flight 清空后再继续。

## 11. 幂等性设计

### 11.1 Report 幂等

`job.AckedReports` 保存已经 ack 过的 report key：

```go
AckedReports map[entity.TaskAttemptKey]struct{}
```

处理逻辑：

```text
if report key in AckedReports:
    return ack
else:
    handle report
    add key to AckedReports
    return ack
```

这样 worker 可以安全重试上报 report，直到收到 ack。

### 11.2 Stale Attempt Report

如果 report 对应的 attempt 不在 in-flight 中：

```text
job.inFlightAttempt(report.TaskType, report.Key()) not found
```

当前 master 会：

1. 将该 report key 记入 `AckedReports`。
2. 释放 worker slot 上匹配的 task。
3. 返回 ack。
4. 不修改 task 状态。

这可以处理：

1. 旧 attempt report 晚到。
2. task 已被重新调度。
3. report 被重复发送。

### 11.3 Create Job 幂等

当前创建 job 不是幂等接口。

每次合法 `POST /client-api/jobs` 都会创建一个新的 job id。

后续如果需要幂等创建，可以引入：

1. client request id。
2. job name + user + nonce。
3. 幂等 key -> job id 映射。

### 11.4 Delete Plugin 幂等

当前 `DeletePlugin` 对存在的 plugin 是幂等的：

1. 第一次删除设置 `DeletedAt`。
2. 再次删除同一个 plugin，如果仍在 `pluginStatus` 中，会继续返回成功。

如果 plugin 被 cleanup 从 `pluginStatus` 删除，再 delete 会返回 not found。

### 11.5 Cleanup 幂等

`CleanupDeletedPlugins` 对文件缺失是幂等的：

```go
if err := os.Remove(plugin.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
    return err
}
```

文件已经不存在不视为错误。

## 12. 插件生命周期

### 12.1 上传插件

Endpoint:

```http
POST /client-api/upload-plugin/:plugin_name
Body: binary blob
```

流程：

1. sanitize 用户传入的 plugin name。
2. 生成 `PluginUniqueID`：
   ```text
   sanitizedUserID + "-" + tool.GitLikeRandomHex()
   ```
3. 写入临时文件：
   ```text
   <uniqueID>.tmp-<random>
   ```
4. 写完并 close 后 rename 到最终路径。
5. 调用 `scheduler.RegisterPlugin(uniqueID, path)`。
6. 返回 `plugin_unique_id`。

### 12.2 PluginUniqueID

Plugin unique id 由两部分组成：

```text
用户传的 ID - 随机不重复 ID
```

例如：

```text
wc-7f91d4321bbc54d6e68884de434f3078a8d3f921
```

随机部分当前使用 `sha1(randomBytes)` 生成 40 位 hex，风格类似 git hash。

### 12.3 插件路径安全

上传、删除和启动加载都会通过安全路径逻辑限制 plugin unique id：

1. 不能为空。
2. 不能包含路径分隔符。
3. clean 后必须仍在 plugin store 目录直接子路径中。

这避免通过 `../` 删除或写入任意文件。

### 12.4 启动加载插件

Server 启动时：

```go
plugins, err := scheduler.LoadPluginStatusesFromStore(pluginStorePath)
scheduler.InitScheduler(plugins)
```

加载规则：

1. 忽略目录。
2. 忽略临时文件名中包含 `.tmp-` 的文件。
3. 对每个合法文件构造 `pluginStatus`。

### 12.5 创建 job 时的 plugin snapshot

创建 job 时，master 会读取 plugin 的当前文件路径：

```go
job.PluginUniqueID = input.Req.PluginUniqueID
job.PluginFilePath = plugin.FilePath
```

这样即使 plugin 后续被软删除，正在运行的 job 仍然可以通过 job snapshot fetch 到对应 plugin 文件。

### 12.6 删除插件

Endpoint:

```http
DELETE /client-api/plugin/:plugin_unique_id
```

当前 delete API 只做软删除：

1. 检查 plugin unique id 是否安全。
2. 调用 `scheduler.DeletePlugin(uniqueID)`。
3. 设置 `DeletedAt`。
4. 从 list API 中隐藏。
5. 不立即物理删除文件。

### 12.7 Cleanup 插件

Scheduler 暴露：

```go
CleanupDeletedPlugins() error
```

Cleanup 规则：

1. 找出所有非 terminal job 使用的 plugin unique id。
2. 遍历 deleted plugin。
3. 如果 plugin 仍被非 terminal job 使用，跳过。
4. 否则删除文件。
5. 从 `pluginStatus` 删除。

当前 server 代码中仍有定时 cleanup loop。若设计上希望完全由外部显式调用 cleanup，应移除 `server.NewServer` 中的自动 loop。

## 13. Worker Fetch Plugin 协议

Endpoint:

```http
GET /worker-api/fetch-job-plugin/{job_id}
```

成功响应：

```http
200 OK
Content-Type: application/octet-stream

<plugin binary>
```

失败响应：

```json
{
  "code": ...,
  "msg": "..."
}
```

失败语义：

1. `job_id` 为空：HTTP 400 + `CodeBadRequest`
2. job 不存在：HTTP 404 + `CodeJobNotFound`
3. job 已终止：HTTP 410 + `CodeFetchJobPluginJobTerminated`
4. job 没有 plugin snapshot 或 plugin 文件不存在：HTTP 410 + `CodeFetchJobPluginJobTerminated`
5. 其他文件系统错误：HTTP 500 + `CodeInternalError`

## 14. Client API 协议

### 14.1 Upload Plugin

```http
POST /client-api/upload-plugin/:plugin_name
Body: plugin binary
```

成功：

```json
{
  "code": 0,
  "msg": "ok",
  "plugin_unique_id": "wc-..."
}
```

失败：

1. plugin name 无效：HTTP 400 + `CodeBadRequest`
2. 文件系统错误：HTTP 500 + `CodeInternalError`
3. scheduler register 失败：HTTP 500 + `CodeInternalError`

### 14.2 Delete Plugin

```http
DELETE /client-api/plugin/:plugin_unique_name
```

成功：

```json
{
  "code": 0,
  "msg": "ok"
}
```

失败：

1. unique id 非法：HTTP 400 + `CodeBadRequest`
2. plugin 不存在：HTTP 404 + `CodeDeletePluginPluginNotFound`

语义：

1. 只软删除。
2. 从 list 中隐藏。
3. 正在运行 job 的 plugin snapshot 仍可 fetch。

### 14.3 List Plugins

```http
GET /client-api/plugins/:order
```

`order` 可选：

1. `dict-order`
2. `time-order`

成功：

```json
{
  "code": 0,
  "msg": "ok",
  "plugins": ["wc-..."]
}
```

失败：

1. order 非法：HTTP 400 + `CodeBadRequest`

### 14.4 Create Job

```http
POST /client-api/jobs
Content-Type: application/json
```

Request:

```json
{
  "job_name": "word-count",
  "plugin_unique_id": "wc-...",
  "num_reduce_tasks": 2,
  "task_splits": [
    {
      "type": "file",
      "data": {
        "path": "/input/part-0000"
      }
    }
  ]
}
```

成功：

```json
{
  "code": 0,
  "msg": "ok",
  "job_id": "job-20260611120000-10000"
}
```

失败：

1. bind JSON 失败：HTTP 400 + `CodeBadRequest`
2. plugin unique id 为空：HTTP 400 + `CodeBadRequest`
3. task splits 为空：HTTP 400 + `CodeBadRequest`
4. num reduce tasks 小于等于 0：HTTP 400 + `CodeBadRequest`
5. plugin 不存在或已删除：HTTP 404 + `CodeCreateJobPluginNotFound`

### 14.5 List Jobs

```http
GET /client-api/jobs
```

成功：

```json
{
  "code": 0,
  "msg": "ok",
  "jobs": [
    {
      "job_id": "job-...",
      "job_name": "word-count",
      "status": "mapping"
    }
  ]
}
```

### 14.6 Get Job

```http
GET /client-api/jobs/:id
```

成功：

```json
{
  "code": 0,
  "msg": "ok",
  "job": {
    "job_id": "job-...",
    "job_name": "word-count",
    "status": "reducing"
  }
}
```

失败：

1. job 不存在：HTTP 404 + `CodeJobNotFound`

## 15. Worker Assignment 协议

Master 不主动调用 worker 当前暴露的 run task API；而是在 heartbeat response 中返回 `AssignedTasks`。

DTO：

```go
type RunTaskReq struct {
    SlotID string `json:"slot_id"`
    JobID string `json:"job_id"`
    TaskID string `json:"task_id"`
    AttemptID string `json:"attempt_id"`
    TaskType comm.TaskType `json:"task_type"`
    Plugin comm.PluginSpec `json:"plugin_spec"`
    Conf map[string]string `json:"conf"`
    MapTask *comm.MapTaskSpec `json:"map_task,omitempty"`
    ReduceTask *comm.ReduceTaskSpec `json:"reduce_task,omitempty"`
}
```

当前 master 下发：

1. `slot_id`
2. `job_id`
3. `task_id`
4. `attempt_id`
5. `task_type`
6. `map_task` 或 `reduce_task`

当前 `Plugin` 和 `Conf` 预留但未填充。Worker 可以通过 `job_id` 调 `/worker-api/fetch-job-plugin/{job_id}` 获取 plugin。

## 16. Map Output 元数据

Map 成功后上报：

```go
type MapOutputMetaEntry struct {
    TaskAttemptKey
    WorkerUniqueID string
    WorkerAddr string
    Size int64
}
```

一个 map task 成功后向 master 上报一个 output meta。这个 meta 表示该 map attempt 的输出位置，不暴露具体 partition。

例如 `NumReduceTasks = 3`：

```text
map-0 attempt-1 -> worker-1
```

Master 按 map task id 记录可用 output meta。

Reduce task 分配时拿到每个 map task 的一个可用 output meta：

```text
reduce-0 receives map-0/map-1/... output metas
worker fetches each output with ReducePartitionID == 0
```

因此 `PartitionID` 属于 reduce fetch 请求语义，不属于 `MapOutputMetaEntry`。

## 17. 容灾设计

### 17.1 Worker 进程重启

Worker 重启后应携带更大的 `WorkerEpoch`。

Master 行为：

1. 检测 `req.WorkerEpoch > worker.Epoch`。
2. 遍历旧 worker 上的 running tasks。
3. 对仍是 in-flight 的 attempts 执行 `markTaskPending`。
4. 删除旧 worker 状态。
5. 建立新 worker 状态。
6. 继续处理当前 heartbeat。

这样旧 worker 上运行的任务会重新调度。

### 17.2 Worker 旧进程延迟心跳

如果旧 worker 进程或旧网络包携带更小 epoch：

```text
req.WorkerEpoch < worker.Epoch
```

Master 返回 `HeartbeatCodeEpochStaleRequest`，并且：

1. 不处理 slot snapshot。
2. 不处理 reports。
3. 不处理 cleanup query。
4. 不分配任务。

### 17.3 Heartbeat 乱序

如果 `req.Seq <= worker.LastSeq`：

Master 返回 `HeartbeatCodeSeqBackoffRequest`。

行为：

1. 不更新 slot snapshot。
2. 不分配新 task。
3. 仍处理 reports。
4. 仍处理 cleanup query。

### 17.4 Task 丢失

如果 worker 新 slot snapshot 不再包含旧 running task：

```text
old worker view has task
new slot snapshot does not report same attempt
```

Master 会把仍 in-flight 的 attempt 对应 task 放回 pending。

这解决：

1. worker 本地 task 丢失。
2. slot 消失。
3. slot 被重置为空。
4. worker 进程没重启但局部执行状态丢失。

### 17.5 Task 执行失败

Worker 上报 `TaskReportFailed`。

Master 将 task 重新 pending，并在后续 heartbeat 中重新分配新的 attempt。

当前没有 attempt retry limit。后续建议增加：

1. `AttemptCount`
2. max attempts
3. failed terminal state
4. job killed/failed state

当前代码刻意不做上限，失败 task 会持续回到 pending，直到后续引入显式 kill/fail 策略。

### 17.6 Reduce 拉取 Map Output 失败

Reduce task 可以通过 `LostMapOutputs` 指出哪些 map outputs 拉取失败。

Master 会：

1. 删除对应 output meta。
2. 将对应 map task 重新 pending。
3. 将 reduce task 重新 pending。
4. 保持 job reducing。

这是 Hadoop 风格的 shuffle 容错：reduce 阶段发现 map output 丢失时，不需要重跑整个 job，只需要重跑丢失 output 对应的 map task。

### 17.7 Plugin 删除期间 job 仍运行

创建 job 时保存 plugin file path snapshot：

```go
job.PluginFilePath = plugin.FilePath
```

即使 plugin 后续被软删除：

1. list API 不再展示该 plugin。
2. 新 job 不能使用该 plugin。
3. 已创建 job 仍可 fetch plugin file。
4. cleanup 不会删除仍被非 terminal job 使用的 plugin。

### 17.8 Master 进程故障

当前 master 主要状态在内存中：

1. jobs
2. workers
3. runnable queue
4. attempts
5. acked reports

Master 进程重启后，这些状态会丢失。

当前支持启动加载 plugin 文件，但不支持恢复 job 状态。

后续要支持 master 级容灾，需要引入持久化：

1. job metadata WAL
2. task state WAL
3. attempt state WAL
4. acked report key 持久化
5. plugin registry 持久化
6. master lease 或 HA 选主

## 18. 并发与一致性

### 18.1 状态串行化

所有 scheduler 状态都在 `runLoopForever` 内处理。

这意味着以下结构不需要额外 mutex：

1. `jobStatus`
2. `workerStatus`
3. `pluginStatus`
4. `runnableJobQueue`
5. `runnableJobIndex`

### 18.2 单例初始化

Scheduler 通过全局单例维护：

```go
var schedulerInstance *schedulerImpl
var schedulerInstanceMu sync.Mutex
```

`GetScheduler()` 懒加载。

`InitScheduler(plugins)` 会替换当前实例。

测试中多次 `NewServer` 会多次 `InitScheduler`，旧 scheduler goroutine 会继续存在，但不再被全局引用。当前测试可接受；生产环境中通常只初始化一次。

### 18.3 HTTP Handler 并发

Gin handler 可并发执行，但 handler 只是同步调用 Scheduler 接口。Scheduler 接口通过 channel 串行化实际状态修改。

### 18.4 Runnable Queue 一致性

每次可能改变 runnable 状态的地方都应调用 `refreshRunnableJob`：

1. 创建 job 后。
2. task 分配后。
3. task report 后。
4. task requeue 后。
5. job stage 改变后。
6. worker restart 后。
7. slot snapshot reconcile 后。

## 19. 返回码设计

### 19.1 Entity Code

```go
const (
    CodeOK Code = iota
    CodeInternalError Code = -1
    CodeBadRequest Code = -2
)
```

除 `CodeOK`、`CodeInternalError`、`CodeBadRequest` 这类通用 code 外，每条业务链路定义自己的 code 类型，例如 create job、fetch job plugin、delete plugin、heartbeat 各自维护业务错误码，避免不同接口复用含义不精确的 code。

### 19.2 RPC Code

```go
const (
    CodeOK = iota
    CodeUploadPluginPluginNameInvalid
    CodeDeletePluginPluginNotFound
    CodeFetchJobPluginJobNotFound
    CodeFetchJobPluginJobTerminated
    CodeCreateJobPluginNotFound
    CodeGetJobJobNotFound
    CodeHeartbeatSeqBackoffRequest
    CodeHeartbeatEpochStaleRequest
    CodeInternalError = -1
    CodeBadRequest = -2
)
```

### 19.3 HTTP 映射

常见映射：

| 场景 | HTTP | Code |
| --- | --- | --- |
| 成功 | 200/201 | CodeOK |
| bind JSON 失败 | 400 | CodeBadRequest |
| 输入参数非法 | 400 | CodeBadRequest |
| delete plugin 时 plugin 不存在 | 404 | CodeDeletePluginPluginNotFound |
| create job 时 plugin 不存在 | 404 | CodeCreateJobPluginNotFound |
| get job 时 job 不存在 | 404 | CodeGetJobJobNotFound |
| fetch job plugin 时 job 不存在 | 404 | CodeFetchJobPluginJobNotFound |
| fetch job plugin 时 job 已终止 | 410 | CodeFetchJobPluginJobTerminated |
| 内部错误 | 500 | CodeInternalError |

## 20. 当前已知限制与建议增强

### 20.1 Report 来源未绑定 worker

当前 in-flight attempt 只按 `(jobID, taskID, attemptID)` 索引，没有记录该 attempt 被分配给哪个 worker 和 slot。

风险：

1. 另一个 worker 如果错误上报同一个 attempt id，master 会接受。
2. 旧 worker 如果拿到了当前 attempt id，也可能影响当前 task。

后续建议：

1. `taskStatus` 增加 `WorkerUniqueID` 和 `SlotID`。
2. `markTaskInFlight` 或 assign 时记录 worker/slot。
3. `handleTaskReportIdempotently` 校验 report 来源。
4. `TaskAttemptKey` 仍保持三元组，来源校验作为额外条件。

### 20.2 Worker timeout 未实现

当前只有 heartbeat 到达时才触发 reconcile。

如果 worker 彻底宕机且不再发 heartbeat，master 无法自动发现。

后续建议：

1. 记录 worker last heartbeat time。
2. 定期扫描超时 worker。
3. 超时后将其 running tasks requeue。
4. 从 workerStatus 中移除 worker 或标记 lost。

### 20.3 Attempt retry limit 未实现

当前失败 task 会无限重试。

后续建议：

1. task 记录 attempt count。
2. 超过阈值后 job failed/killed。
3. API 展示最后错误。

### 20.4 Job failed 状态未实现

当前只有 `succeed` 和 `killed` 终态，且 killed 没有 API。

后续建议：

1. 增加 `failed`。
2. 增加 kill job API。
3. 增加 retry exhausted -> failed。
4. 增加 job error details。

### 20.5 Master 状态持久化未实现

Master 重启会丢失 job/worker/attempt 状态。

后续建议：

1. WAL
2. snapshot
3. external DB
4. replay recovery

### 20.6 Plugin cleanup 策略需要最终确认

当前 `DeletePlugin` API 只软删除，但 server 中仍存在定时 cleanup loop。

如果最终设计要求“只由显式接口 cleanup”，应移除自动 loop。

如果最终设计允许后台 cleanup，则需要文档化 cleanup 周期、可配置开关和失败重试策略。

### 20.7 Reduce input 元数据可压缩

当前 reduce task 收到每个 map task 的一个 `MapOutputMetaEntry`，worker 使用 `ReducePartitionID` 拉取对应分区。

后续如果 map output 存储有更紧凑的集中索引，可以把多个 map output meta 压缩为 worker 级别的批量 fetch 描述，减少 heartbeat response 体积。

### 20.8 Metrics 和可观测性未实现

建议增加：

1. job count
2. pending task count
3. in-flight task count
4. worker count
5. free/busy slots
6. report ack count
7. task retry count
8. plugin cleanup count

### 20.9 安全与鉴权未实现

当前 API 没有鉴权。

后续建议：

1. client token
2. worker token
3. plugin upload size limit
4. request body size limit
5. plugin checksum
6. worker identity registration

## 21. 典型流程

### 21.1 提交并运行一个 job

```text
client upload plugin
  -> master stores plugin file
  -> scheduler registers plugin

client create job with plugin_unique_id + task_splits
  -> scheduler creates job
  -> materializes map/reduce tasks
  -> job enters runnable queue

worker heartbeat with free slots
  -> master accepts seq
  -> master assigns map attempts

worker runs map
  -> produces map outputs
  -> heartbeat reports succeeded

master ack report
  -> stores map output metas
  -> marks map task succeeded
  -> when all maps done, job enters reducing

worker heartbeat with free slots
  -> master assigns reduce attempts with map output metas

worker runs reduce
  -> heartbeat reports succeeded

master marks reduce succeeded
  -> when all reduces done, job enters succeed

worker asks cleanup wanted job
  -> master returns job id if terminal
```

### 21.2 Worker 重启流程

```text
old worker epoch = 1
master has task attempt running on worker

worker restarts
new heartbeat epoch = 2

master detects epoch increased
  -> requeue old running attempts
  -> replace worker status
  -> accept current heartbeat
  -> may assign new tasks
```

### 21.3 Heartbeat 乱序流程

```text
master accepted seq=100
late heartbeat seq=99 arrives

master:
  -> returns SeqBackoff
  -> ignores slot snapshot
  -> still handles reports
  -> does not assign tasks
```

### 21.4 Reduce fetch failure 流程

```text
reduce-0 tries to fetch map-3 partition-0
fetch failed

worker reports:
  reduce-0 failed
  LostMapOutputs includes map-3 attempt-x partition-0

master:
  -> remove that map output meta
  -> requeue map-3
  -> requeue reduce-0
  -> keep job reducing
  -> scheduler will assign map-3 first
  -> after map-3 succeeds, reduce-0 can run again
```

## 22. 测试覆盖概览

当前测试覆盖了以下关键语义：

1. 创建 job 后会物化 map/reduce pending tasks。
2. 创建 job 必须提供 plugin unique id。
3. Heartbeat report 幂等 ack。
4. stale report 不修改 job。
5. superseded attempt report 不覆盖当前 running attempt。
6. stale epoch 被拒绝。
7. unknown worker 首个 seq 被接受。
8. seq backoff 不更新 slot snapshot，但处理 reports。
9. seq backoff 不分配新任务。
10. slot snapshot 只更新 worker，不创建 job task。
11. slot snapshot 丢失 in-flight task 时 requeue。
12. pending map task 能分配给 free slot。
13. reduce 必须等 map 完成后才能分配。
14. runnable job queue 支持 job 间 round-robin。
15. worker restart requeue in-flight task。
16. reduce fetch failure requeue lost map 和 failed reduce。
17. plugin upload/list/delete。
18. delete missing plugin 返回 not found code。
19. fetch plugin 成功。
20. fetch plugin 文件缺失返回 not found code。
21. 删除 plugin 后 running job snapshot 仍可 fetch。
22. get missing job 返回 job not found code。

## 23. 维护规则

后续修改 master 时建议遵守以下规则：

1. Scheduler 内部不要引用 `rpc/...`。
2. API 层新增字段时必须同步 DTO -> entity 转换。
3. 修改 task 状态必须通过 `markTaskPending`、`markTaskInFlight`、`markTaskSuccess`。
4. 改变 runnable 条件后必须更新 `refreshRunnableJob` 调用点和测试。
5. Heartbeat report 必须保持幂等。
6. Seq backoff 不能更新 slot snapshot，也不能分配 task。
7. Stale epoch 不能处理 report。
8. Plugin 删除不能破坏已创建 job 的 plugin snapshot。
9. 新增 not found 场景时不要返回 internal error。
10. 分布式状态相关改动必须补 scheduler 单测。

## 24. 推荐后续路线

优先级建议：

1. 移除或配置化 server 自动 plugin cleanup loop，明确 cleanup 所有权。
2. 给 task attempt 绑定 worker unique id 和 slot id，并校验 report 来源。
3. 增加 worker heartbeat timeout scanner。
4. 增加 attempt retry limit 和 job failed 状态。
5. 为 master 状态增加 WAL 或 snapshot。
6. 为 reduce task 下发按 partition 过滤后的 map outputs。
7. 增加 job detail API，展示 task counts、last error、plugin unique id。
8. 增加 metrics 和 structured logs。
9. 增加 request body size limit 和 worker/client auth。
10. 增加 cleanup API 或 admin RPC。
