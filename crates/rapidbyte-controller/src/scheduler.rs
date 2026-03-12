//! FIFO task queue with lease-based assignment and fencing.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use thiserror::Error;

use crate::lease::{EpochGenerator, Lease};

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("unknown task: {0}")]
    UnknownTask(String),
    #[error("task {0} is not in expected state {1:?}")]
    InvalidState(String, TaskState),
}

/// Task lifecycle states within the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

/// A task record in the scheduler.
#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub task_id: String,
    pub run_id: String,
    pub attempt: u32,
    pub lease: Option<Lease>,
    pub state: TaskState,
    pub pipeline_yaml: Vec<u8>,
    pub dry_run: bool,
    pub limit: Option<u64>,
    pub assigned_agent_id: Option<String>,
}

/// Returned to an agent when it successfully polls a task.
#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: String,
    pub run_id: String,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub pipeline_yaml: Vec<u8>,
    pub dry_run: bool,
    pub limit: Option<u64>,
}

/// FIFO task queue with lease management.
pub struct TaskQueue {
    pending: VecDeque<String>, // task_ids in FIFO order
    tasks: HashMap<String, TaskRecord>,
    next_task_id: u64,
}

impl TaskQueue {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            tasks: HashMap::new(),
            next_task_id: 1,
        }
    }

    /// Enqueue a new task. Returns the `task_id`.
    pub fn enqueue(
        &mut self,
        run_id: String,
        pipeline_yaml: Vec<u8>,
        dry_run: bool,
        limit: Option<u64>,
        attempt: u32,
    ) -> String {
        let task_id = format!("task-{}", self.next_task_id);
        self.next_task_id += 1;

        let record = TaskRecord {
            task_id: task_id.clone(),
            run_id,
            attempt,
            lease: None,
            state: TaskState::Pending,
            pipeline_yaml,
            dry_run,
            limit,
            assigned_agent_id: None,
        };

        self.tasks.insert(task_id.clone(), record);
        self.pending.push_back(task_id.clone());
        task_id
    }

    /// Poll the next pending task for an agent. Assigns a lease and returns the assignment.
    /// Returns `None` if the queue is empty.
    pub fn poll(
        &mut self,
        agent_id: &str,
        lease_ttl: Duration,
        epoch_gen: &EpochGenerator,
    ) -> Option<TaskAssignment> {
        let task_id = self.pending.pop_front()?;
        let record = self.tasks.get_mut(&task_id)?;

        let epoch = epoch_gen.next();
        record.state = TaskState::Assigned;
        record.lease = Some(Lease::new(epoch, lease_ttl));
        record.assigned_agent_id = Some(agent_id.to_string());

        Some(TaskAssignment {
            task_id: record.task_id.clone(),
            run_id: record.run_id.clone(),
            attempt: record.attempt,
            lease_epoch: epoch,
            pipeline_yaml: record.pipeline_yaml.clone(),
            dry_run: record.dry_run,
            limit: record.limit,
        })
    }

    /// Mark a task as running. Validates the lease epoch.
    ///
    /// # Errors
    ///
    /// Returns `SchedulerError` if the task is unknown, not in `Assigned` state,
    /// or the provided lease epoch is stale.
    pub fn report_running(
        &mut self,
        task_id: &str,
        lease_epoch: u64,
    ) -> Result<(), SchedulerError> {
        let record = self
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| SchedulerError::UnknownTask(task_id.to_string()))?;

        if record.state != TaskState::Assigned {
            return Err(SchedulerError::InvalidState(
                task_id.to_string(),
                TaskState::Assigned,
            ));
        }

        if let Some(lease) = &record.lease {
            if !lease.is_valid(lease_epoch) {
                return Err(SchedulerError::InvalidState(
                    task_id.to_string(),
                    TaskState::Assigned,
                ));
            }
        }

        record.state = TaskState::Running;
        Ok(())
    }

    /// Complete a task. Validates the lease epoch.
    /// Returns `true` if acknowledged (epoch valid), `false` if stale.
    ///
    /// # Errors
    ///
    /// Returns `SchedulerError::UnknownTask` if the task does not exist.
    pub fn complete(
        &mut self,
        task_id: &str,
        lease_epoch: u64,
        succeeded: bool,
    ) -> Result<bool, SchedulerError> {
        let record = self
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| SchedulerError::UnknownTask(task_id.to_string()))?;

        // Check lease validity
        if let Some(lease) = &record.lease {
            if !lease.is_valid(lease_epoch) {
                return Ok(false); // stale epoch — not acknowledged
            }
        }

        record.state = if succeeded {
            TaskState::Completed
        } else {
            TaskState::Failed
        };
        record.lease = None;
        Ok(true)
    }

    /// Cancel a task.
    /// If pending, removes it from the queue. If running/assigned, marks it cancelled.
    ///
    /// # Errors
    ///
    /// Returns `SchedulerError` if the task is unknown or already in a terminal
    /// state (`Completed`, `Failed`, `Cancelled`, `TimedOut`).
    pub fn cancel(&mut self, task_id: &str) -> Result<(), SchedulerError> {
        let record = self
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| SchedulerError::UnknownTask(task_id.to_string()))?;

        match record.state {
            TaskState::Pending => {
                record.state = TaskState::Cancelled;
                self.pending.retain(|id| id != task_id);
            }
            TaskState::Assigned | TaskState::Running => {
                record.state = TaskState::Cancelled;
                record.lease = None;
            }
            _ => {
                return Err(SchedulerError::InvalidState(
                    task_id.to_string(),
                    record.state,
                ));
            }
        }
        Ok(())
    }

    /// Find tasks with expired leases, transition them to `TimedOut`.
    /// Returns `(task_id, run_id)` pairs for expired tasks.
    pub fn expire_leases(&mut self) -> Vec<(String, String)> {
        let mut expired = Vec::new();

        for record in self.tasks.values_mut() {
            if matches!(record.state, TaskState::Assigned | TaskState::Running) {
                if let Some(lease) = &record.lease {
                    if lease.is_expired() {
                        expired.push((record.task_id.clone(), record.run_id.clone()));
                        record.state = TaskState::TimedOut;
                        record.lease = None;
                    }
                }
            }
        }

        expired
    }

    /// Get a task record by ID.
    #[must_use]
    pub fn get(&self, task_id: &str) -> Option<&TaskRecord> {
        self.tasks.get(task_id)
    }

    /// Find the task for a given `run_id` (most recent attempt).
    #[must_use]
    pub fn find_by_run_id(&self, run_id: &str) -> Option<&TaskRecord> {
        self.tasks
            .values()
            .filter(|t| t.run_id == run_id)
            .max_by_key(|t| t.attempt)
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_queue_and_gen() -> (TaskQueue, EpochGenerator) {
        (TaskQueue::new(), EpochGenerator::new())
    }

    #[test]
    fn enqueue_and_poll_returns_task() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);

        let assignment = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();
        assert_eq!(assignment.run_id, "r1");
        assert_eq!(assignment.attempt, 1);
        assert_eq!(assignment.lease_epoch, 1);
        assert_eq!(assignment.pipeline_yaml, b"yaml");
    }

    #[test]
    fn poll_empty_queue_returns_none() {
        let (mut q, gen) = make_queue_and_gen();
        assert!(q.poll("agent-1", Duration::from_secs(60), &gen).is_none());
    }

    #[test]
    fn complete_with_valid_epoch_succeeds() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);
        let assignment = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();

        let ack = q
            .complete(&assignment.task_id, assignment.lease_epoch, true)
            .unwrap();
        assert!(ack);
        assert_eq!(
            q.get(&assignment.task_id).unwrap().state,
            TaskState::Completed
        );
    }

    #[test]
    fn complete_with_stale_epoch_returns_false() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);
        let assignment = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();

        // Use a wrong epoch
        let ack = q
            .complete(&assignment.task_id, assignment.lease_epoch + 999, true)
            .unwrap();
        assert!(!ack);
    }

    #[test]
    fn expire_leases_catches_timed_out_tasks() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);
        // Assign with 0 TTL so it expires immediately
        let assignment = q.poll("agent-1", Duration::from_secs(0), &gen).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        let expired = q.expire_leases();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], (assignment.task_id.clone(), "r1".to_string()));
        assert_eq!(
            q.get(&assignment.task_id).unwrap().state,
            TaskState::TimedOut
        );
    }

    #[test]
    fn cancel_pending_task_removes_from_queue() {
        let (mut q, gen) = make_queue_and_gen();
        let task_id = q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);
        q.cancel(&task_id).unwrap();

        assert_eq!(q.get(&task_id).unwrap().state, TaskState::Cancelled);
        // Queue should be empty
        assert!(q.poll("agent-1", Duration::from_secs(60), &gen).is_none());
    }

    #[test]
    fn cancel_running_task_marks_cancelled() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"yaml".to_vec(), false, None, 1);
        let assignment = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();
        q.report_running(&assignment.task_id, assignment.lease_epoch)
            .unwrap();

        q.cancel(&assignment.task_id).unwrap();
        assert_eq!(
            q.get(&assignment.task_id).unwrap().state,
            TaskState::Cancelled
        );
    }

    #[test]
    fn fifo_ordering_preserved() {
        let (mut q, gen) = make_queue_and_gen();
        q.enqueue("r1".into(), b"y1".to_vec(), false, None, 1);
        q.enqueue("r2".into(), b"y2".to_vec(), false, None, 1);
        q.enqueue("r3".into(), b"y3".to_vec(), false, None, 1);

        let a1 = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();
        let a2 = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();
        let a3 = q.poll("agent-1", Duration::from_secs(60), &gen).unwrap();

        assert_eq!(a1.run_id, "r1");
        assert_eq!(a2.run_id, "r2");
        assert_eq!(a3.run_id, "r3");
    }
}
