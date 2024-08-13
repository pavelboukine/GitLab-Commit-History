use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription};
use models::{
    data::{
        mapper::{CompressedDataStorage, StoredData, StoredDataCompressed},
        query::get_data_path,
    },
    sessions::{Visit, SessionData},
};
use nutils::{async_threadpool, graceful_shutdown::ShutdownToken};
use metrics::{Gauge, Labels, MetricsClient};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

use crate::CompressArg;

const NUM_OF_WORKERS: usize = 3;
const NUM_OF_MESSAGES: u16 = 10;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CloudStorageMessage {
    attributes: HashMap<String, String>,
}

impl FromPubSubMessage for CloudStorageMessage {
    fn from(message: EncodedMessage) -> Result<Self, cloud_pubsub::error::Error> {
        match message.decode() {
            Ok(_bytes) => Ok(CloudStorageMessage {
                attributes: message.attributes.unwrap_or_default(),
            }),
            Err(e) => Err(cloud_pubsub::error::Error::from(e)),
        }
    }
}

/*
 * This is where we declare all the Prometheus metrics.
 * Key concepts and implementations in this program include:
 *
 * 1. `Arc` (Atomic Reference Counting):
 *    - `Arc` is used to enable multiple ownership of the `metrics` object across asynchronous tasks and workers.
 *    - Cloning an `Arc` is a cheap operation that only increments the reference count, ensuring thread-safe shared ownership.
 *    - By using `Arc`, we ensure that the underlying metrics object remains valid as long as any task holds a reference to it.
 *
 * 2. Trait Objects and `dyn` Keyword:
 *    - The `MetricsTrait` trait defines the interface for updating different types of metrics.
 *    - `dyn MetricsTrait` is used to allow for dynamic dispatch, enabling the use of different implementations (e.g., real metrics and mock metrics) interchangeably.
 *    - This approach provides flexibility and decouples the implementation details from the logic that uses the metrics.
 */
#[async_trait]
pub trait MetricsTrait: Send + Sync {
    fn update_fetch_data(&self, duration: f64);
    fn update_compress_data(&self, duration: f64);
    fn update_clean_data_paths(&self, duration: f64);
}

// Real Metrics implementation
#[derive(Debug, Clone)]
pub struct Metrics {
    pub time_fetch_data: Gauge,
    pub time_compress_data: Gauge,
    pub time_clean_data_paths: Gauge,
}

impl Metrics {
    // Initialize all metrics with a naming prefix. Since data_compress and data_dlq_compress will both use metrics.
    pub fn new(prefix: &str) -> Self {
        let mut metrics_client = MetricsClient::new_for_service()
            .expect("Unable to initialize metrics client")
            .with_periodic_push();

        let metric_name = |name: &str| format!("{}_{}", prefix, name);

        Metrics {
            time_fetch_data: metrics_client
                .register_gauge(
                    &metric_name("time_fetch_data"),
                    "Duration of fetch_data calls in seconds",
                    Labels::new(),
                )
                .expect("Unable to register time_fetch_data gauge"),

            time_compress_data: metrics_client
                .register_gauge(
                    &metric_name("time_compress_data"),
                    "Duration of compress_data calls in seconds",
                    Labels::new(),
                )
                .expect("Unable to register time_compress_data gauge"),

            time_clean_data_paths: metrics_client
                .register_gauge(
                    &metric_name("time_clean_data_paths"),
                    "Duration of clean_data_paths calls in seconds",
                    Labels::new(),
                )
                .expect("Unable to register time_clean_data_paths gauge"),
        }
    }
}

#[async_trait]
impl MetricsTrait for Metrics {
    fn update_fetch_data(&self, duration: f64) {
        self.time_fetch_data.set(duration);
    }

    fn update_compress_data(&self, duration: f64) {
        self.time_compress_data.set(duration);
    }

    fn update_clean_data_paths(&self, duration: f64) {
        self.time_clean_data_paths.set(duration);
    }
}

// Mock Metrics for testing purposes
pub struct MockMetrics;

impl MetricsTrait for MockMetrics {
    fn update_fetch_data(&self, _duration: f64) {
        // No operation
    }
    fn update_compress_data(&self, _duration: f64) {
        // No operation
    }
    fn update_clean_data_paths(&self, _duration: f64) {
        // No operation
    }
}

/// Runs DataCompressor with compress args. This is where we set the pubsub subscription,
/// service account and shutdown token. Spawns a Tokio task per worker.
pub(crate) async fn run_compress(compress_args: CompressArg) -> Result<()> {
    // Set up
    let workers = compress_args.workers.unwrap_or(NUM_OF_WORKERS);
    let subscription_name = std::env::var("PUBSUB_DATA_COMPRESSION_SUBSCRIPTION")
        .context("PUBSUB_DATA_COMPRESSION_SUBSCRIPTION must be set")?;
    let service_account = gcp::auth().context("Failed to authenticate with GCP")?;
    let pubsub_client = Client::new(service_account)
        .await
        .context("Failed to create PubSub client")?;
    let subscription = pubsub_client.subscribe(subscription_name);
    // Initialize shutdown token
    let shutdown_token = ShutdownToken::new().context("Failed to create shutdown token")?;

    // Once set up is done, log that we are starting up
    log::info!("Running {} DataCompressor workers...", workers);

    // Need to refresh the JWT token with pubsub every 15 mins
    pubsub_client.spawn_token_renew(Duration::from_secs(15 * 60));

    // Initialize the metrics system.
    // The combination of `Arc` and `dyn` provides flexibility, thread safety, and testability by allowing the metrics implementation to be shared and swapped as needed.
    let metrics = Arc::new(Metrics::new("compress")) as Arc<dyn MetricsTrait>;

    // Initialize and collect all tasks
    let tasks = (0..workers)
        .map(|_| {
            // Cloning the Arc in each task ensures that each task has its own reference, maintaining thread safety.
            let metrics_clone = Arc::clone(&metrics);
            tokio::spawn(pubsub_streamer(
                metrics_clone,
                subscription.clone(),
                shutdown_token.clone(),
            ))
        })
        .collect::<Vec<_>>();

    // Signal readiness after launching workers
    let path = Path::new("/tmp/ready");
    let mut file = File::create(path)?;
    file.write_all(b"ready")?;
    log::info!("All DataCompressor workers have been launched. System is ready.");

    // Use try_join_all to wait for all tasks to complete. This is where it will shut down due to bubbling errors.
    match nutils::try_join_all(tasks).await {
        Ok(_) => log::info!("DataCompressor is shutting down."),
        Err(e) => {
            log::error!("Shutting down DataCompressor due to fatal error: {}", e);
        }
    }
    Ok(())
}

/// Processes batches of messages from a Pub/Sub subscription asynchronously.
/// For each message, it spawns a task to process the file event, leading to data deletion of legacy paths being returned.
/// Successful message processing results in acknowledgment back to Pub/Sub.
/// Old data associated with successfully acknowledged messages is deleted in batch after acknowledgment.
/// The loop continues until a shutdown signal is received, ensuring graceful termination.
pub async fn pubsub_streamer(
    metrics: Arc<dyn MetricsTrait>,
    subscription: Subscription,
    shutdown_token: ShutdownToken,
) -> Result<()> {
    log::info!("Spawned 1 DataCompressor worker!");
    while !shutdown_token.is_shutdown() {
        let result = subscription.get_messages(NUM_OF_MESSAGES).await;
        if let Ok((file_events, acks)) = result {
            // Spawn tasks to process each file event
            let tasks = file_events
                .into_iter()
                .zip(acks)
                .map(|(file_event, ack_id)| {
                    let metrics_clone = Arc::clone(&metrics);
                    let ack_clone = ack_id.clone(); // Clone ack_id for use in the spawned task
                    tokio::spawn(async move {
                        // Attempt to process the file event and return paths for deletion if successful
                        handle_one_file_event(metrics_clone, file_event)
                            .await
                            .map(|paths| (ack_clone, paths))
                    })
                })
                .collect::<Vec<_>>();

            // Use try_join_all to await all tasks and process the outcomes
            match nutils::try_join_all(tasks).await {
                Ok(results) => {
                    // Separate successful ack_ids and paths to delete
                    let (ack_ids, paths_to_delete): (Vec<String>, Vec<Vec<PathBuf>>) =
                        results.into_iter().unzip();

                    // Flatten the list of path vectors into a single vector of PathBuf here
                    let flattened_paths_to_delete: Vec<PathBuf> =
                        paths_to_delete.into_iter().flatten().collect();

                    if !ack_ids.is_empty() {
                        // Acknowledge successful messages
                        subscription.acknowledge_messages(ack_ids).await;
                        // Delete old data after successful acknowledgment
                        let metrics_clone = Arc::clone(&metrics);
                        delete_data_paths(metrics_clone, flattened_paths_to_delete)
                            .await?;
                    }
                }
                Err(e) => {
                    log::error!("Batch processing failed with error: {:?}", e);
                }
            }
        }
    }

    Ok(())
}

/// Handles individual file events from Pub/Sub messages. Validates the event type,
/// processes the associated session data, and returns a list of file paths
/// that should be deleted based on the processing outcome.
///
/// - If the event type is not `OBJECT_FINALIZE`, it logs an error and returns an empty list,
///   indicating no action is required.
/// - If the `objectId` attribute is missing, it returns an error.
/// - On successful processing of the session, it returns paths for data deletion.
pub async fn handle_one_file_event(
    metrics: Arc<dyn MetricsTrait>,
    file_event: CloudStorageMessage,
) -> Result<Vec<PathBuf>> {
    // Check for the correct event type
    if let Some(event_type) = file_event.attributes.get("eventType") {
        if event_type != "OBJECT_FINALIZE" {
            log::error!(
                "Expected OBJECT_FINALIZE event type, received: {}",
                event_type
            );
            // Return an empty Vec to indicate no deletion needed for incorrect event types
            return Ok(vec![]);
        }
    }

    let relpath = file_event
        .attributes
        .get("objectId")
        .ok_or_else(|| anyhow!("Missing objectId in message: {:?}", file_event))?;

    // Process the file event. In this example, we parse the session from GS.
    let session_data = SessionData::parse_from_gs(relpath).await.map_err(|e| {
        log::error!("Could not parse {}: {:?}", relpath, e);
        e
    })?;

    // Extract only the variables we need from the session
    let visits = session_data.visits.clone();
    let session_id = session_data.id;
    let session_domain = session_data.on_domain.clone();
    let session_datetime = session_data.created_at;

    // Explicitly drop session_data after extracting the things we need.
    drop(session_data);

    // Once we get the session varibles we can send it here, where it'll do the actual compress work and then return a vec of raw data paths to delete.
    let paths_to_delete =
        compress_data(metrics, visits, session_id, session_domain, session_datetime).await?;

    Ok(paths_to_delete)
}

/// Processes the session data for a given session. This involves aggregating stored data for each visit,
/// compressing the aggregated data, and writing the compressed data to a designated storage location.
///
/// Paths of the original raw data files are collected to be deleted after successful processing and acknowledgment.
/// Each visit's data is attempted to be read and processed; failures in reading data are logged, and processing continues.
///
/// Returns a list of paths for the original raw data files that should be deleted post-processing.
/// If the session data is empty or processing fails, the returned list might be empty, indicating no data deletion is required.
async fn compress_data(
    metrics: Arc<dyn MetricsTrait>,
    visits: Vec<Visit>,
    session_id: Uuid,
    session_domain: String,
    session_datetime: DateTime<Utc>,
) -> Result<Vec<PathBuf>> {
    let mut paths_to_delete = Vec::new();
    let mut compressed_data = StoredDataCompressed::new();
    let start_time = std::time::Instant::now();

    for visit in visits {
        let raw_data_paths = vec![
            get_data_path(visit.started_at.naive_utc(), &visit.on_domain, visit.id, true).await?,
            get_data_path(visit.started_at.naive_utc(), &visit.on_domain, visit.id, false).await?,
        ];

        for raw_data_path in raw_data_paths {
            if raw_data_path.is_file() {
                if let Ok(stored_data) =
                    StoredData::from_file_no_lock(&raw_data_path).await
                {
                    log::debug!(
                        "Found data for session with session_id: {} and visit_id: {}",
                        session_id,
                        visit.id
                    );
                    compressed_data.insert(visit.id, stored_data);

                    // Directly push the PathBuf to paths_to_delete, no conversion needed
                    paths_to_delete.push(raw_data_path);
                } else {
                    log::debug!("No stored data found for session: {}", session_id);
                }
            } else {
                log::debug!("Path does not point to a file for visit_id: {}", visit.id);
            }
        }
    }

    // Send metric
    let elapsed_time = start_time.elapsed().as_secs_f64(); // Capture the elapsed time in seconds
    metrics.update_fetch_data(elapsed_time); // Set the gauge to the elapsed time

    // After aggregating all visits data, serialize and compress the map
    if !compressed_data.is_empty() {
        let start_time = std::time::Instant::now();
        // Compress and store the map using the new struct's method
        compressed_data
            .to_compressed_file(session_datetime.naive_utc(), &session_domain, session_id)
            .await?;
        // Send metric
        let elapsed_time = start_time.elapsed().as_secs_f64(); // Capture the elapsed time in seconds
        metrics.update_compress_data(elapsed_time); // Set the gauge to the elapsed time
    }

    // This return statement is placed outside the `if` block, ensuring that
    // `Ok(paths_to_delete)` is returned whether the `if` condition is true or false.
    Ok(paths_to_delete)
}

/// Asynchronously deletes each file specified in `paths_to_delete`.
pub async fn delete_data_paths(
    metrics: Arc<dyn MetricsTrait>,
    paths_to_delete: Vec<PathBuf>,
) -> Result<()> {
    let start_time = std::time::Instant::now();

    // Spawn a deletion task for each path
    async_threadpool::run(move || {
        for path in paths_to_delete {
            if let Err(e) = std::fs::remove_file(&path) {
                log::error!("Failed to delete file at path {:?}: {}", path, e);
            }
        }
    })
    .await?;

    // Send metric
    let elapsed_time = start_time.elapsed().as_secs_f64(); // Capture the elapsed time in seconds
    metrics.update_clean_data_paths(elapsed_time); // Set the gauge to the elapsed time

    Ok(())
}
