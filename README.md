# MapReduce Coordinator

This project implements a fault-tolerant MapReduce coordinator that
manages job submission, worker registration, task scheduling, and
failure recovery.

Core logic I wrote is mostly in `src/coordinator/mod.rs`


## Architecture

- Centralized coordinator with mutex-protected state
- Workers communicate via RPC
- Jobs are scheduled using a FIFO queue
- Tasks follow a Pending → Assigned → Completed lifecycle


## Scheduling Model

- Map tasks are scheduled before reduce tasks
- Reduce tasks begin only after all map tasks complete
- Workers pull tasks from the coordinator
- Idle workers block when no eligible tasks are available


## Fault Tolerance

- Workers are declared failed after missed heartbeats
- Map tasks from failed workers are re-executed
- Reduce tasks are retried on failure
- Unrecoverable errors mark the job as failed


## Limitations

- Single coordinator (no leader election)
- No persistent state or crash recovery
- Assumes reliable RPC delivery


## Design Details

See `docs/design.md` for full data structures, RPC behavior,
and failure-handling logic.
