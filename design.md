
This document describes the internal design of a centralized MapReduce coordinator, including its data structures, scheduling logic, RPC behavior, and fault tolerance mechanisms.

# 1. Coordinator State

All shared coordinator state is protected by a mutex and stored in a single CoordinatorState structure.

Coordinator
```rust
pub struct Coordinator {
    inner: Arc<Mutex<CoordinatorState>>,
}
```

CoordinatorState
- The coordinator maintains global metadata for workers, jobs, and scheduling:
- next_worker_id: monotonically increasing worker identifier
- next_job_id: monotonically increasing job identifier\
- workers: mapping from worker ID to worker metadata
- jobs: mapping from job ID to job metadata
- job_queue: FIFO queue of active job IDs

Jobs remain in the queue until all map and reduce tasks complete.

# 2. Worker Metadata

Each worker is tracked using a WorkerMeta structure:

```rust
struct WorkerMeta {
    worker_id: u16,
    heartbeat: Option<Instant>,
    assigned_task: Option<TaskID>,
    completed: Vec<TaskID>,
}
```

The coordinator uses heartbeats to detect worker's alive status and tracks both assigned and completed tasks to support fault recovery.

# 3. Job and Task Metadata
Task Identification

Each task is uniquely identified by:

```rust
struct TaskID {
    job_id: u32,
    task_index: u32,
    task_type: TaskType,
}
```

Task types are either Map or Reduce.

```rust
Task State
enum TaskStatus {
    Pending,
    Assigned,
    Completed,
}

Task Metadata
struct TaskMeta {
    id: TaskID,
    task_status: TaskStatus,
    file: Option<String>,
    assigned_worker: Option<u16>,
}
```

Each task explicitly tracks its status and assigned worker.

Job Metadata
```rust
struct JobMeta {
    job_id: u32,
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    args: Vec<u8>,

    failed: bool,
    errors: Vec<String>,

    map_tasks: Vec<TaskMeta>,
    reduce_tasks: Vec<TaskMeta>,
}
```

A job is considered complete only when all map and reduce tasks have finished successfully.

# 4. Task Lifecycle

Tasks transition through the following states:

Pending → Assigned → Completed

State transitions occur atomically while holding the coordinator lock.

# 5. Scheduling Model

- Jobs are scheduled in FIFO order using job_queue
- Map tasks are always scheduled before reduce tasks
- Reduce tasks are eligible only after all map tasks complete
- Workers pull tasks from the coordinator via RPC
- If a job has no eligible tasks, scheduling proceeds to the next job in the queue.

# 6. RPC Interfaces
SubmitJob
- Validates application name
- Initializes job metadata
- Creates map and reduce task structures
- Inserts job into jobs and enqueues its ID into job_queue
- Returns assigned job ID to the client

GetTask
- Validates worker ID and heartbeat
- Detects and handles failed workers before assignment
- Scans jobs in FIFO order for the first eligible pending task
- Assigns either a map or reduce task
- Returns Wait = true if no tasks are available

FinishTask
- Marks the specified task as completed
- Updates worker and job metadata accordingly

FailTask
- Workers invoke this RPC when a task fails.

Two failure modes are supported:
- Retryable failures: tasks are reset to Pending
- Fatal failures: the job is marked as failed

PollJob
- Returns job status to the client
- done = true when all tasks complete
- failed = true if the job encounters an unrecoverable error

# 7. Fault Tolerance
Worker Failures
- Workers are considered failed if their heartbeat exceeds a timeout.

When a worker fails:
- Assigned map tasks are reset to Pending
- Completed map tasks are re-executed
- Incomplete reduce tasks are retried

Reduce Failures
If a reduce worker cannot reach a map worker:
- The coordinator reassigns affected map tasks
- Reduce execution is retried once inputs are regenerated

Job Failures
Unrecoverable errors include:
- Input/output file errors
- Application-level map or reduce errors

In these cases:
- The job is marked failed
- Errors are recorded in job metadata
- Clients observe failure via PollJob

# 8. Concurrency

All coordinator state is protected by a single mutex
- Task assignment and failure handling are atomic
- Worker failure detection occurs before task scheduling
- No task is assigned to more than one worker at a time
