use krabka_client_admin::CreateTopicSpec;

/// How much of the contract one check compares against.
///
/// The split exists because the partition count has exactly one definition.
/// The deployment step states it; a role reads it back. A role that carried
/// its own copy to compare would be the second, independently configured
/// shard count that this contract is here to remove.
pub(crate) enum TopicExpectation<'a> {
    /// The deployment's own numbers: partition counts, replication factor and
    /// WAL retention, as `CreateTopics` specs.
    Deployment(&'a [CreateTopicSpec]),

    /// Structure only: the topic exists, and a state topic is compacted.
    /// Both are true or false regardless of how the deployment is sized.
    Structure,
}
