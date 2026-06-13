use crate::{
    application::consumer::MultiHandler, infrastructure::bootstrap::AppState,
    key_shared::run_key_shared_consumer, model::SubscriptionType, shared::run_shared_consumer,
};
use std::{collections::HashMap, error::Error, sync::Arc};

//RUN APPLICATION -------------------------------------------------------------------------------------
pub async fn run<L>(
    state: Arc<AppState>,
    multi_handler: L,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    L: MultiHandler + Send + Sync + 'static,
{
    // 1. Data Retrieval: Clone topic and type configurations from the shared state.
    let topics: Vec<String> = state.config.topics.clone();
    let types: Vec<String> = state.config.topics_type.clone();

    // 2. Grouping Logic: Categorize topics by their subscription type using a HashMap.
    let mut grouped: HashMap<SubscriptionType, Vec<String>> = HashMap::new();

    for (topic, t) in topics.iter().zip(types.iter()) {
        let sub_type: SubscriptionType = SubscriptionType::parse(t);
        grouped.entry(sub_type).or_default().push(topic.clone());
    }

    // 3. Resource Preparation: Wrap external handlers in Arc for safe concurrent access across tasks.
    let m_handler: Arc<L> = Arc::new(multi_handler);

    // 4. Task Spawning: Initialize asynchronous consumer tasks for each grouped subscription.
    let mut task_handles: Vec<tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>> =
        vec![];

    for (sub_type, topics) in grouped {
        let state: Arc<AppState> = state.clone();
        let handlers: Arc<L> = m_handler.clone();

        let handle: tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> =
            tokio::spawn(async move {
                match sub_type {
                    SubscriptionType::KeyShared => {
                        run_key_shared_consumer(state, topics, handlers).await
                    }
                    SubscriptionType::Shared => run_shared_consumer(state, topics, handlers).await,
                }
            });
        task_handles.push(handle);
    }

    // 5. Synchronization: Await all task handles and propagate both join and internal errors.
    for handle in task_handles {
        // ? Tokio errors
        // ?? Logic errors
        handle.await??;
    }

    Ok(())
}
