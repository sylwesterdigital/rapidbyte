//! Agent registry with heartbeat monitoring and liveness reaping.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("unknown agent: {0}")]
    UnknownAgent(String),
}

/// Record for a registered agent.
#[derive(Debug, Clone)]
pub struct AgentRecord {
    pub agent_id: String,
    pub max_tasks: u32,
    pub active_tasks: u32,
    pub flight_endpoint: String,
    pub plugin_bundle_hash: String,
    pub last_heartbeat: Instant,
    pub available_plugins: Vec<String>,
    pub memory_bytes: u64,
}

/// In-memory registry of connected agents.
pub struct AgentRegistry {
    agents: HashMap<String, AgentRecord>,
}

impl AgentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            agents: HashMap::new(),
        }
    }

    /// Register a new agent. Returns the `agent_id`.
    pub fn register(
        &mut self,
        agent_id: String,
        max_tasks: u32,
        flight_endpoint: String,
        plugin_bundle_hash: String,
        available_plugins: Vec<String>,
        memory_bytes: u64,
    ) -> String {
        let record = AgentRecord {
            agent_id: agent_id.clone(),
            max_tasks,
            active_tasks: 0,
            flight_endpoint,
            plugin_bundle_hash,
            last_heartbeat: Instant::now(),
            available_plugins,
            memory_bytes,
        };
        self.agents.insert(agent_id.clone(), record);
        agent_id
    }

    /// Update heartbeat timestamp for an agent.
    ///
    /// # Errors
    ///
    /// Returns `RegistryError::UnknownAgent` if the agent is not registered.
    pub fn heartbeat(
        &mut self,
        agent_id: &str,
        active_tasks: u32,
    ) -> Result<&AgentRecord, RegistryError> {
        let record = self
            .agents
            .get_mut(agent_id)
            .ok_or_else(|| RegistryError::UnknownAgent(agent_id.to_string()))?;
        record.last_heartbeat = Instant::now();
        record.active_tasks = active_tasks;
        Ok(record)
    }

    /// Get an agent record.
    #[must_use]
    pub fn get(&self, agent_id: &str) -> Option<&AgentRecord> {
        self.agents.get(agent_id)
    }

    /// Restore an agent record loaded from durable storage.
    pub fn restore_agent(&mut self, record: AgentRecord) {
        self.agents.insert(record.agent_id.clone(), record);
    }

    /// Remove an agent from the registry.
    pub fn remove(&mut self, agent_id: &str) -> Option<AgentRecord> {
        self.agents.remove(agent_id)
    }

    /// Reap agents whose last heartbeat is older than `timeout`.
    /// Returns the IDs of removed agents.
    pub fn reap_dead(&mut self, timeout: Duration) -> Vec<String> {
        let now = Instant::now();
        let dead: Vec<String> = self
            .agents
            .iter()
            .filter(|(_, r)| now.duration_since(r.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dead {
            self.agents.remove(id);
        }
        dead
    }

    /// List all registered agents.
    #[must_use]
    pub fn list(&self) -> Vec<&AgentRecord> {
        self.agents.values().collect()
    }

    /// Snapshot all registered agents.
    #[must_use]
    pub fn all_agents(&self) -> Vec<AgentRecord> {
        self.agents.values().cloned().collect()
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn register_test_agent(registry: &mut AgentRegistry, id: &str) -> String {
        registry.register(
            id.to_string(),
            2,
            "localhost:9091".into(),
            "hash123".into(),
            vec!["source-postgres".into()],
            1024 * 1024 * 512,
        )
    }

    #[test]
    fn register_and_get() {
        let mut reg = AgentRegistry::new();
        let id = register_test_agent(&mut reg, "agent-1");
        let record = reg.get(&id).unwrap();
        assert_eq!(record.agent_id, "agent-1");
        assert_eq!(record.max_tasks, 2);
    }

    #[test]
    fn heartbeat_updates_timestamp() {
        let mut reg = AgentRegistry::new();
        register_test_agent(&mut reg, "agent-1");
        let before = reg.get("agent-1").unwrap().last_heartbeat;
        std::thread::sleep(Duration::from_millis(10));
        reg.heartbeat("agent-1", 1).unwrap();
        let after = reg.get("agent-1").unwrap().last_heartbeat;
        assert!(after > before);
        assert_eq!(reg.get("agent-1").unwrap().active_tasks, 1);
    }

    #[test]
    fn heartbeat_unknown_agent_returns_error() {
        let mut reg = AgentRegistry::new();
        assert!(reg.heartbeat("nonexistent", 0).is_err());
    }

    #[test]
    fn reap_dead_removes_stale_agents() {
        let mut reg = AgentRegistry::new();
        register_test_agent(&mut reg, "agent-1");
        std::thread::sleep(Duration::from_millis(20));
        let dead = reg.reap_dead(Duration::from_millis(10));
        assert_eq!(dead, vec!["agent-1"]);
        assert!(reg.get("agent-1").is_none());
    }

    #[test]
    fn reap_dead_preserves_live_agents() {
        let mut reg = AgentRegistry::new();
        register_test_agent(&mut reg, "agent-1");
        let dead = reg.reap_dead(Duration::from_secs(60));
        assert!(dead.is_empty());
        assert!(reg.get("agent-1").is_some());
    }

    #[test]
    fn remove_agent() {
        let mut reg = AgentRegistry::new();
        register_test_agent(&mut reg, "agent-1");
        let removed = reg.remove("agent-1");
        assert!(removed.is_some());
        assert!(reg.get("agent-1").is_none());
    }

    #[test]
    fn list_agents() {
        let mut reg = AgentRegistry::new();
        register_test_agent(&mut reg, "agent-1");
        register_test_agent(&mut reg, "agent-2");
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn restore_agent_makes_agent_available() {
        let mut reg = AgentRegistry::new();
        reg.restore_agent(AgentRecord {
            agent_id: "agent-1".into(),
            max_tasks: 2,
            active_tasks: 1,
            flight_endpoint: "localhost:9091".into(),
            plugin_bundle_hash: "hash123".into(),
            last_heartbeat: Instant::now(),
            available_plugins: vec!["source-postgres".into()],
            memory_bytes: 1024,
        });

        let record = reg.get("agent-1").unwrap();
        assert_eq!(record.active_tasks, 1);
        assert_eq!(record.flight_endpoint, "localhost:9091");
    }
}
