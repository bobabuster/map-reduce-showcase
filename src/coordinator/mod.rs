//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Code};

use crate::rpc::coordinator::*;
use crate::*;

use tokio::time::Instant;
use std::collections::HashMap;
use std::sync::Arc;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use crate::rpc::coordinator::MapTaskAssignment;


use ::log::info;

pub mod args;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
	Pending,
	Assigned,
	Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskType {
	Map,
	Reduce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TaskID {
	job_id: u32,
	task_index: u32,
    task_type: TaskType,
}

struct TaskMeta {
	id: TaskID,
	task_status: TaskStatus,
	file: Option<String>,
	assigned_worker: Option<u16>,
}

struct JobMeta {
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

impl JobMeta {
    fn maps_done(&self) -> bool {
        for task_meta in &self.map_tasks {
            if task_meta.task_status == TaskStatus::Completed {
                continue;
            } else if task_meta.task_status == TaskStatus::Pending || task_meta.task_status == TaskStatus::Assigned {
                return false;
            }
        }
        return true;
    }

    fn reduces_done(&self) -> bool {
        for task_meta in &self.reduce_tasks {
            if task_meta.task_status == TaskStatus::Completed {
                continue;
            } else if task_meta.task_status == TaskStatus::Pending || task_meta.task_status == TaskStatus::Assigned {
                return false;
            }
        }
        return true;
    }

    fn get_map_assignments(&self) -> Vec<MapTaskAssignment> {
        let mut res = Vec::new();
        for task_meta in &self.map_tasks {
            if let Some(wid) = task_meta.assigned_worker {
                let assignment = MapTaskAssignment {
                    task: task_meta.id.task_index,
                    worker_id: wid as u32,
                };
                res.push(assignment);
            }
        }
        return res;
    }

    fn get_task_mut(&mut self, task_id: TaskID) -> Option<&mut TaskMeta> {
        match task_id.task_type {
            TaskType::Map =>
                self.map_tasks.iter_mut().find(|t| t.id == task_id),
            TaskType::Reduce =>
                self.reduce_tasks.iter_mut().find(|t| t.id == task_id),
        }
    }

}

#[derive(Debug, Clone)]
struct WorkerMeta {
    worker_id: u16,
    heartbeat: Option<Instant>,
    assigned_task: Option<TaskID>,
    completed: Vec<TaskID>,
}



pub struct CoordinatorState {
    next_worker_id: u16,
    next_job_id: u32,
    workers: HashMap<u16, WorkerMeta>,
    jobs: HashMap<u32, JobMeta>,
    job_queue: VecDeque<u32>,
}

pub struct Coordinator {
    inner: Arc<Mutex<CoordinatorState>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(CoordinatorState {
                next_worker_id: 0,
                next_job_id: 0,
                workers: HashMap::new(),
                jobs: HashMap::new(),
                job_queue: VecDeque::new(),
            }))
        }
    }

    fn prepare_task_reply(job: &JobMeta, task_id: TaskID) -> GetTaskReply {
        let t = task_id;

        let file = match t.task_type {
            TaskType::Map => {
                let tm = job.map_tasks.iter().find(|x| x.id == t).unwrap();
                tm.file.clone().unwrap()
            }
            TaskType::Reduce => "".to_string(),
        };

        GetTaskReply {
            job_id: t.job_id,
            output_dir: job.output_dir.clone(),
            app: job.app.clone(),
            task: t.task_index,
            file,
            n_reduce: job.n_reduce,
            n_map: job.files.len() as u32,
            reduce: matches!(t.task_type, TaskType::Reduce),
            wait: false,
            map_task_assignments: job.get_map_assignments(),
            args: job.args.clone(),
        }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    /// An example RPC.
    ///
    /// Feel free to delete this.
    /// Make sure to also delete the RPC in `proto/coordinator.proto`.
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        let req = req.get_ref();
        let message = format!("Hello, {}!", req.name);
        Ok(Response::new(ExampleReply { message }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        info!("submit_job: entered");

        let mut state = self.inner.lock().await;
        let req_inner = req.into_inner(); 
        
        let valid_app = crate::app::named(&req_inner.app);
        if let Err(e) = valid_app {
            return Err(Status::new(Code::InvalidArgument, e.to_string()));
        }

        let id = state.next_job_id;

        let mut map_tasks = Vec::new();
        for (i, file) in req_inner.files.iter().enumerate() {
            let task_id = TaskID {
                job_id: id,
                task_index: i as u32,
                task_type: TaskType::Map,
            };
            let task = TaskMeta {
                id: task_id,
                task_status: TaskStatus::Pending,
                file: Some(file.clone()),
                assigned_worker: None,
            };
            map_tasks.push(task);
        }

        let mut reduce_tasks = Vec::new();
        for i in 0..req_inner.n_reduce {
            reduce_tasks.push(TaskMeta {
                id: TaskID {
                    job_id: id,
                    task_index: i,
                    task_type: TaskType::Reduce,
                },
                task_status: TaskStatus::Pending,
                file: None,
                assigned_worker: None,
            });
        }

        let job_meta = JobMeta {
            files: req_inner.files,
            output_dir: req_inner.output_dir,
            app: req_inner.app,
            n_reduce: req_inner.n_reduce,
            args: req_inner.args,

            failed: false,
            errors: Vec::new(),

            map_tasks: map_tasks,
            reduce_tasks: reduce_tasks,
        };

        state.job_queue.push_back(id);
        state.jobs.insert(id, job_meta);

        state.next_job_id += 1;
        Ok(Response::new(SubmitJobReply { job_id: id }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        info!("poll_job: entered");
        let state = self.inner.lock().await;
        let req_inner = req.into_inner();
        
        let job = match state.jobs.get(&req_inner.job_id) {
            Some(j) => j,
            None => return Err(Status::new(Code::NotFound, "job id is invalid")),
        };

        if job.failed == true {
            return Ok(Response::new(PollJobReply {
                done: false,
                failed: true,
                errors: job.errors.clone(),
            }))
        }

        let done = job.maps_done() && job.reduces_done();
        let failed = job.failed;
        let errors = job.errors.clone();

        Ok(Response::new(PollJobReply {
            done: done,
            failed: failed,
            errors: errors,
        }))

    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        info!("heartbeat: entered");
        let mut state = self.inner.lock().await;
        let worker_id = req.into_inner().worker_id as u16;
        info!("Worker {} sent heartbeat request", worker_id);

        if let Some(worker) = state.workers.get_mut(&worker_id) {
            worker.heartbeat = Some(Instant::now());
            return Ok(Response::new(HeartbeatReply {}));
        }
        Err(Status::not_found(format!("Worker {} not found", worker_id))) 
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        info!("register: entered");

        let mut state = self.inner.lock().await;
        let id = state.next_worker_id;
        let worker_meta = WorkerMeta {
            worker_id: id,
            heartbeat: None,
            assigned_task: None,
            completed: Vec::new(),
        };
        
        state.next_worker_id += 1;

        state.workers.insert(id, worker_meta.clone());
        info!("Worker {} registered", worker_meta.worker_id);

        Ok(Response::new(RegisterReply { worker_id: id as u32 }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
        ) -> Result<Response<GetTaskReply>, Status> {
        info!("get_task: entered");
    
        let worker_id = req.into_inner().worker_id as u16;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
    
        if let Some(worker) = state.workers.get_mut(&worker_id) {
            worker.heartbeat = Some(now);
        }
    
        let dead_workers: Vec<u16> = state.workers.iter().filter_map(|(&wid, w)| {
            if let Some(hb) = w.heartbeat {
                if now.duration_since(hb).as_secs() > TASK_TIMEOUT_SECS {
                    return Some(wid);
                }
            } else if w.assigned_task.is_some() {
                return Some(wid);
            }
            None
        }).collect();
    
        let dead_workers_clone = dead_workers.clone();

        for wid in dead_workers {
            let assigned_task = {
                if let Some(worker) = state.workers.get_mut(&wid) {
                    worker.assigned_task.take()
                } else {
                    None
                }
            };
            
            let completed_map_tasks: Vec<TaskID> = {
                if let Some(worker) = state.workers.get_mut(&wid) {
                    worker.completed
                        .iter()
                        .filter(|tid| tid.task_type == TaskType::Map)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                }
            };
            
            
            // reassign currently assigned task
            if let Some(task_id) = assigned_task {
                if let Some(job) = state.jobs.get_mut(&task_id.job_id) {
                    if let Some(task) = job.get_task_mut(task_id) {
                        task.task_status = TaskStatus::Pending;
                        task.assigned_worker = None;
                    }
                }
            }
            
            for tid in completed_map_tasks {
                if let Some(job) = state.jobs.get_mut(&tid.job_id) {
                    if let Some(task) = job.map_tasks.iter_mut().find(|t| t.id == tid) {
                        task.task_status = TaskStatus::Pending;
                        task.assigned_worker = None;
                    }
                }
            }
        }
    
        for wid in &dead_workers_clone {
            state.workers.remove(wid);
        }
    
        // core logic to assign task to a worker
        let worker_assigned_task: Option<TaskID> = {
            let worker = match state.workers.get_mut(&worker_id) {
                Some(w) => w,
                None => return Err(Status::not_found("worker not found")),
            };
    
            if let Some(task_id) = worker.assigned_task {
                Some(task_id)
            } else {
                None
            }
        };
        
        // if the worker has an assigned task, return that task
        if let Some(task_id) = worker_assigned_task {
            let job = state.jobs.get(&task_id.job_id).unwrap();
            
            if job.failed {
                if let Some(worker) = state.workers.get_mut(&worker_id) {
                    worker.assigned_task = None;
                }
            } else {
                return Ok(Response::new(Self::prepare_task_reply(job, task_id)));
            }
        }
    
        let job_ids: Vec<u32> = state.job_queue.iter().cloned().collect();
        let mut found_task: Option<TaskID> = None;
    
        for job_id in job_ids {
            let job = match state.jobs.get_mut(&job_id) {
                Some(j) => j,
                None => continue,
            };
    
            if job.failed {
                continue;
            }
    
            if !job.maps_done() {
                if let Some(task) = job.map_tasks.iter_mut().find(|t| t.task_status == TaskStatus::Pending) {
                    task.task_status = TaskStatus::Assigned;
                    task.assigned_worker = Some(worker_id);
                    found_task = Some(task.id);
                    break;
                }
            } else if !job.reduces_done() {
                if let Some(task) = job.reduce_tasks.iter_mut().find(|t| t.task_status == TaskStatus::Pending) {
                    task.task_status = TaskStatus::Assigned;
                    task.assigned_worker = Some(worker_id);
                    found_task = Some(task.id);
                    break;
                }
            }
        }
    
        if let Some(task_id) = found_task {
            if let Some(worker) = state.workers.get_mut(&worker_id) {
                worker.assigned_task = Some(task_id);
            }
        }
    
        let reply = if let Some(worker) = state.workers.get(&worker_id) {
            if let Some(task_id) = worker.assigned_task {
                let job = state.jobs.get(&task_id.job_id).unwrap();
                Self::prepare_task_reply(job, task_id)
            } else {
                GetTaskReply {
                    job_id: 0,
                    output_dir: "".to_string(),
                    app: "".to_string(),
                    task: 0,
                    file: "".to_string(),
                    n_reduce: 0,
                    n_map: 0,
                    reduce: false,
                    wait: true,
                    map_task_assignments: Vec::new(),
                    args: Vec::new(),
                }
            }
        } else {
            GetTaskReply {
                job_id: 0,
                output_dir: "".to_string(),
                app: "".to_string(),
                task: 0,
                file: "".to_string(),
                n_reduce: 0,
                n_map: 0,
                reduce: false,
                wait: true,
                map_task_assignments: Vec::new(),
                args: Vec::new(),
            }
        };
    
        Ok(Response::new(reply))
    }   

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
        ) -> Result<Response<FinishTaskReply>, Status> {
        info!("finish_task: entered");
        let mut state = self.inner.lock().await;
        let req_inner = req.into_inner();
        
        let task_id = TaskID {
            job_id: req_inner.job_id,
            task_index: req_inner.task,
            task_type: match req_inner.reduce {
                true => TaskType::Reduce,
                false => TaskType::Map,
            }
        };
        
        if let Some(w) = state.workers.get_mut(&(req_inner.worker_id as u16)) {
            w.assigned_task = None;
            w.completed.push(task_id); 
        }
        
        if let Some(job) = state.jobs.get_mut(&req_inner.job_id) {
            if let Some(task) = job.get_task_mut(task_id) {
                task.task_status = TaskStatus::Completed;
            }
        }
        
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
        ) -> Result<Response<FailTaskReply>, Status> {
        info!("fail_task: entered");
        let mut state = self.inner.lock().await;
        let req_inner = req.into_inner();
    
        if !req_inner.retry {
            if let Some(job) = state.jobs.get_mut(&req_inner.job_id) {
                job.errors.push(req_inner.error);
                job.failed = true;
            }
            
            if let Some(worker) = state.workers.get_mut(&(req_inner.worker_id as u16)) {
                worker.assigned_task = None;
            }
            
            return Ok(Response::new(FailTaskReply {}));
        }
    
        if req_inner.retry && !req_inner.reduce {
            let wid = req_inner.worker_id as u16;
            if let Some(worker) = state.workers.get(&wid) {
                let completed_map_tasks: Vec<TaskID> = worker
                    .completed
                    .iter()
                    .filter(|tid| tid.task_type == TaskType::Map)
                    .cloned()
                    .collect();
    
                drop(worker);
    
                for tid in completed_map_tasks {
                    if let Some(job) = state.jobs.get_mut(&tid.job_id) {
                        if let Some(task) = job.map_tasks.iter_mut().find(|t| t.id == tid) {
                            if task.task_status != TaskStatus::Completed {
                                task.task_status = TaskStatus::Pending;
                                task.assigned_worker = None;
                            }
                        }
                    }
                }
            }
    
            let wid = req_inner.worker_id as u16;
            if let Some(worker) = state.workers.get_mut(&wid) {
                if let Some(task_id) = worker.assigned_task.take() {
                    if let Some(job) = state.jobs.get_mut(&task_id.job_id) {
                        if let Some(task) = job.get_task_mut(task_id) {
                            task.task_status = TaskStatus::Pending;
                            task.assigned_worker = None;
                        }
                    }
                }
            }
    
            return Ok(Response::new(FailTaskReply {}));
        }
    
        if req_inner.retry && req_inner.reduce {
            let wid = req_inner.worker_id as u16;
            if let Some(worker) = state.workers.get_mut(&wid) {
                if let Some(task_id) = worker.assigned_task.take() {
                    if let Some(job) = state.jobs.get_mut(&task_id.job_id) {
                        if let Some(task) = job.get_task_mut(task_id) {
                            task.task_status = TaskStatus::Pending;
                            task.assigned_worker = None;
                        }
                    }
                }
            }
            return Ok(Response::new(FailTaskReply {}));
        }
    
        Ok(Response::new(FailTaskReply {}))
    }

}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();
    let coordinator = Coordinator::new();
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
