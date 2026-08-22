use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. Define connection details for the remote 3-node cluster
    let bootstrap_servers = "192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096";
    let topic_name = "remote-test-topic";

    println!("⚡ Connecting to remote cluster at: {}", bootstrap_servers);

    // 2. Build the Kafka Producer client configuration
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000") // Fail quickly if network is unreachable
        .create()
        .expect("❌ Failed to create Kafka producer client");

    println!("🚀 Sending test message to topic '{}'...", topic_name);

    // 3. Construct a test payload
    let record = FutureRecord::to(topic_name)
        .key("test-key")
        .payload("Hello from my local Rust application over the network!");

    // 4. Publish asynchronously with a timeout anchor
    match producer.send(record, Duration::from_secs(5)).await {
        Ok(delivery) => {
            println!("✅ Success! Message delivered safely.");
            println!("   📍 Partition: {}", delivery.partition);
            println!("   🔢 Offset:    {}", delivery.offset);
        }
        Err((error, _original_message)) => {
            println!("❌ Delivery failed! Handshake or metadata routing broke.");
            println!("   Detailed Error: {:?}", error);
        }
    }
}

// Test consumer
// kcat -C -b 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096 -t remote-test-topic
//
// Test producer
// kcat -P -b 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096 -t remote-test-topics
// Type your message and hit Enter. Press Ctrl+D to exit.
//
// List broker metadata
// kcat -L -b 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096
