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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use chrono::Utc;
    use models::{
        data::mapper::from_compressed_session_file,
        sessions::{Visit, SessionData, TEST_USER_AGENT_IPHONE},
    };
    use uuid::Uuid;

    /// This test simulates the data_compressor service's processing of a Pub/Sub message, representing a new SessionData needing compression.
    /// It validates the service's ability to parse the message, retrieve and decompress the SessionData from cloud storage,
    /// compress the session data after processing, and write the compressed data to the expected location, ensuring end-to-end functionality.
    #[tokio::test]
    async fn test_data_compressor_streaming() {
        // Authenticate with cloud service and enable the Pub/Sub emulator
        let service_account = gcp::auth().expect("Failed to authenticate with cloud service");

        let _pubsub_emulator_host = gcp::pubsub::enable_emulator();

        // Setup Pub/Sub client, topic, and subscription for the test
        let pubsub_client = cloud_pubsub::Client::new(service_account)
            .await
            .expect("Failed to create Pub/Sub client");

        let topic_name = format!("test_data_compressor-{}", Uuid::new_v4());

        gcp::pubsub::create_test_topic(
            &topic_name,
            &pubsub_client.project(),
            &_pubsub_emulator_host,
        )
        .await
        .expect("Failed to create test topic");

        let topic = pubsub_client.topic(topic_name);
        let subscription = topic
            .subscribe()
            .await
            .expect("Failed to create subscription");

        // Setup a shutdown mechanism for gracefully ending the test
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        let shutdown_token = ShutdownToken::new_test(shutdown_receiver);
        let mock_metrics = Arc::new(MockMetrics) as Arc<dyn MetricsTrait>;
        // Start the data_compressor service
        tokio::spawn(async move {
            pubsub_streamer(mock_metrics, subscription, shutdown_token)
                .await
                .expect("Service failed");
        });

        // Create a mock SessionData object
        let mut session_data = SessionData::new("www.example.com", TEST_USER_AGENT_IPHONE);
        let visit1 = Visit::new(
            "https://www.example.com/checkout/success",
            TEST_USER_AGENT_IPHONE,
        );
        let visit2 = Visit::new(
            "https://www.example.com/checkout/other",
            TEST_USER_AGENT_IPHONE,
        );

        // Add visits to the session data
        session_data.add_visit(&visit1);
        session_data.add_visit(&visit2);

        // Define the path where the session data will be saved in cloud storage
        let storage_path = format!("2024/3/8/www.example.com/{}.jsonz", Uuid::new_v4());
        // Define the path where the uncompressed data will be saved (mocking the existing storage structure)
        let now = Utc::now().naive_utc();
        for visit in session_data.visits.iter() {
            let raw_data_dir = models::data::query::raw_data_dir(now, &session_data.on_domain, true);
            // Ensure the directory structure exists for the legacy path
            if let Err(e) = fs::create_dir_all(&raw_data_dir) {
                log::error!("Failed to create directory for raw data: {}", e);
                continue; // Skip this Visit and move to the next one
            }

            let raw_data_path = raw_data_dir.join(format!("{}.json", visit.id));

            // Create dummy JSON data representing the data for the Visit
            let dummy_data = serde_json::json!({
                "data": {
                    "0": {
                        "payload": "Legacy payload",
                        "response": "Legacy response",
                        "request_headers": [],
                        "response_headers": []
                    }
                }
            })
            .to_string();

            // Write the mock data to the raw data path
            match fs::File::create(&raw_data_path) {
                Ok(mut file) => {
                    if let Err(e) = file.write_all(dummy_data.as_bytes()) {
                        log::error!("Failed to write mock data for visit_id {}: {}", visit.id, e);
                    }
                }
                Err(e) => {
                    log::error!("Failed to create file for visit_id {}: {}", visit.id, e);
                }
            }
        }

        // Save the mock session data to cloud storage using the save_to_storage function
        session_data
            .save_to_storage(&storage_path)
            .await
            .expect("Failed to save mock session data to cloud storage");

        // Publish a test message to the topic to simulate receiving a message
        let attributes: HashMap<String, String> = [
            ("eventType", "OBJECT_FINALIZE"),
            ("objectId", &storage_path),
            // Include other necessary attributes for your service here
        ]
        .iter()
        .cloned()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        topic
            .publish("none".as_bytes(), Some(attributes))
            .await
            .expect("Failed to publish test message");

        // Allow some time for the message to be processed
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Trigger a graceful shutdown to end the test
        shutdown_sender
            .send(())
            .expect("Failed to send shutdown signal");

        let mock_session_id = session_data.id; // Capture the session ID used by the service

        // Ensure the expected output path uses the same session ID
        let expected_output_path = models::data::query::compressed_data_dir(
            now, // Use the current date as it seems the service does
            "www.example.com",
            true,
        )
        .join(format!("{mock_session_id}.jsonz"));

        // Use the from_compressed_session_file method to directly get the decompressed data
        let decompressed_data = from_compressed_session_file(&expected_output_path)
            .await
            .expect("Failed to decompress and deserialize the data");

        // Ensure all Visits are included in the compressed data
        for visit in session_data.visits.iter() {
            assert!(
                decompressed_data.contains_key(&visit.id),
                "Aggregated data missing for Visit ID: {}",
                visit.id
            );
        }

        // Cleanup logic at the end of the test
        if fs::metadata(&expected_output_path).is_ok() {
            fs::remove_file(&expected_output_path)
                .expect("Failed to clean up the compressed data file");
        }
    }

    /// Tests the delete function in the compressor
    #[tokio::test]
    async fn test_delete_old_data() {
        // Create a temporary directory to act as the output directory for this test
        let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let temp_dir_path = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert temp dir path to string");

        // Set the OUTPUT_DIR environment variable to the path of the temporary directory
        std::env::set_var("OUTPUT_DIR", temp_dir_path);

        // Generate UUIDs for visits and create a unique directory for each
        let visit_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let datetime = Utc::now().naive_utc();
        let domain_name = "testdomain.com";
        let mut paths_to_delete = Vec::new();

        for &visit_id in &visit_ids {
            let path = get_raw_data_path(datetime, domain_name, visit_id, true)
                .await
                .unwrap();
            // Write dummy data to simulate existing files
            std::fs::write(&path, b"dummy data").expect("Failed to write dummy data");
            paths_to_delete.push(path);
        }

        // Add a non-existent file path to induce a deletion failure
        let non_existent_path = temp_dir.path().join("non_existent_file.txt");
        paths_to_delete.push(non_existent_path);
        let mock_metrics = Arc::new(MockMetrics) as Arc<dyn MetricsTrait>;
        // Call the function to delete the data
        let result = delete_data_paths(mock_metrics, paths_to_delete).await;

        // Since the updated function does not return an error for failed deletions, check for Ok(())
        assert!(
            result.is_ok(),
            "Expected Ok(()) even if some file deletions fail"
        );

        // Clean up by removing the temporary directory
        drop(temp_dir);

        // Reset the OUTPUT_DIR environment variable to its original state if necessary
        std::env::remove_var("OUTPUT_DIR");
    }

    /// This tests that fs::write does truly overwrite data
    #[tokio::test]
    async fn test_overwrite_file_content() {
        // Dummy data for path
        let visit_id = Uuid::new_v4();
        let datetime = Utc::now().naive_utc();
        let domain_name = "testdomain.com";
        // Step 1: Get the file path
        let file_path = get_raw_data_path(datetime, domain_name, visit_id, true)
            .await
            .unwrap();

        // Step 2: Write bogus data to the file
        let bogus_data = "Sample data";
        std::fs::write(&file_path, bogus_data).expect("Failed to write bogus data");

        // Step 3: Write real (not bogus) data to the same file
        let real_data = "Correct data";
        std::fs::write(&file_path, real_data).expect("Failed to write correct data");

        // Step 4: Read back the content of the file
        let file_content =
            std::fs::read_to_string(&file_path).expect("Failed to read file content");

        // Step 5: Assert that the file content matches the real data
        assert_eq!(
            file_content, real_data,
            "The file content does not match the expected real data"
        );

        // Cleanup: Remove the test file
        std::fs::remove_file(&file_path).expect("Failed to clean up test file");
    }
}
