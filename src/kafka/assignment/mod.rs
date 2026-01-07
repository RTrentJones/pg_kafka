//! Partition Assignment Module
//!
//! This module implements automatic partition assignment strategies for consumer groups.
//! It provides three standard Kafka assignment strategies:
//!
//! - **Range**: Assigns consecutive partition ranges to consumers
//! - **RoundRobin**: Distributes partitions evenly in round-robin order
//! - **Sticky**: Minimizes partition movement during rebalancing (KIP-54)
//!
//! # Architecture
//!
//! The assignment process works as follows:
//!
//! 1. Consumers send JoinGroup with supported strategies and subscription metadata
//! 2. Coordinator selects a common strategy supported by all members
//! 3. When leader sends SyncGroup (with empty assignments), coordinator computes assignments
//! 4. Each consumer receives their partition assignment in SyncGroup response
//!
//! # Wire Format
//!
//! Kafka uses a custom binary format for subscription and assignment metadata:
//!
//! ```text
//! MemberSubscription (JoinGroup metadata):
//!   version: i16
//!   topics: [String]  // array of subscribed topic names
//!   user_data: bytes  // optional client data
//!
//! MemberAssignment (SyncGroup response):
//!   version: i16
//!   topic_partitions: [TopicPartition]  // array of (topic, [partitions])
//!   user_data: bytes  // optional client data
//! ```

pub mod member_assignment;
pub mod strategies;
pub mod subscription;

#[cfg(test)]
mod tests;

// Re-export main types
pub use member_assignment::MemberAssignment;
pub use strategies::{
    create_strategy, select_common_strategy, AssignmentInput, AssignmentOutput, AssignmentStrategy,
};
pub use subscription::MemberSubscription;
