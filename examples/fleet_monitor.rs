use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use aurora_db::workers::{WorkerSystem, WorkerConfig, Job};
use std::time::Instant;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    echo_banner("AURORA AUTONOMOUS FLEET MONITORING SYSTEM");

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("fleet.db");
    
    // 1. High-Performance Configuration
    let db = Aurora::with_config(AuroraConfig {
        db_path: db_path.clone(),
        enable_write_buffering: true,
        ..Default::default()
    }).await?;
    
    // 2. Background Healing System (Workers)
    let worker_config = WorkerConfig {
        storage_path: temp_dir.path().join("repair_queue").to_str().unwrap().to_string(),
        concurrency: 8,
        ..Default::default()
    };
    let worker_system = WorkerSystem::new(worker_config)?;
    
    // Task: "Dispatch Repair Drone"
    worker_system.register_handler("dispatch_repair", |job| {
        async move {
            let drone_id = job.payload.get("drone_id").unwrap().as_str().unwrap();
            println!("  [REPAIR] Dispatching repair technician to Drone: {}", drone_id);
            Ok(())
        }
    }).await;
    worker_system.start().await?;

    // 3. Telemetry Schema
    db.new_collection("telemetry", vec![
        ("drone_id", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ("temp", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_FLOAT, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ("battery", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ("status", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ]).await?;

    // 4. Incident Dashboard (PubSub)
    let mut dashboard_stream = db.pubsub.listen("telemetry");
    tokio::spawn(async move {
        while let Ok(event) = dashboard_stream.recv().await {
            // Live stream of telemetry events
            if matches!(event.change_type, aurora_db::pubsub::events::ChangeType::Insert) {
                // Check the status field for critical events (event._sid is a UUID, not status)
                // println!("  [DASHBOARD]  New telemetry event: {}", event._sid);
            }
        }
    });

    // 5. Reactive Safety Logic (The "Safety Guard")
    // Watch for any drone with temp > 95 degrees
    let mut safety_watcher = db.query("telemetry")
        .filter(|f: &aurora_db::query::FilterBuilder| f.gt("temp", Value::Float(95.0)))
        .watch()
        .await?;

    tokio::spawn(async move {
        while let Some(update) = safety_watcher.next().await {
            match update {
                aurora_db::reactive::updates::QueryUpdate::Added(doc) => {
                    if let (Some(aurora_db::Value::String(drone_id)), Some(aurora_db::Value::Float(temp))) =
                        (doc.data.get("drone_id"), doc.data.get("temp"))
                    {
                        println!("  [ALARM] Critical Heat! Drone {} is at {:.1}C", drone_id, temp);
                    }
                }
                _ => {}
            }
        }
    });

    // 6. Large Scale Simulation (100,000 heartbeats)
    println!("Simulating Global Fleet Telemetry: 100,000 heartbeats...");
    let start = Instant::now();
    let num_heartbeats = 100_000;
    let batch_size = 10_000;

    for i in 0..(num_heartbeats / batch_size) {
        let mut batch = Vec::with_capacity(batch_size);
        for j in 0..batch_size {
            let id = i * batch_size + j;
            let drone_id = format!("DRONE-{:05}", id % 1000); // 1000 unique drones
            let temp = 40.0 + (id as f64 % 60.0); // Temps between 40-100
            
            let mut heartbeat = HashMap::new();
            heartbeat.insert("drone_id".to_string(), Value::String(drone_id.clone()));
            heartbeat.insert("temp".to_string(), Value::Float(temp));
            heartbeat.insert("battery".to_string(), Value::Int((100 - (id % 100)) as i64));
            heartbeat.insert("status".to_string(), Value::String(if temp > 90.0 { "CRITICAL" } else { "OK" }.to_string()));
            
            batch.push(heartbeat);
        }
        db.batch_insert("telemetry", batch).await?;

        // Enqueue repair jobs only after successful persistence
        for j in 0..batch_size {
            let id = i * batch_size + j;
            let temp = 40.0 + (id as f64 % 60.0);
            if temp > 98.0 {
                let drone_id = format!("DRONE-{:05}", id % 1000);
                let mut payload = HashMap::new();
                payload.insert("drone_id".to_string(), serde_json::Value::String(drone_id));
                worker_system.enqueue(Job::new("dispatch_repair").with_payload(payload)).await?;
            }
        }
    }

    db.sync().await?;
    let duration = start.elapsed();
    println!("\nSimulation Finished in {:?}", duration);
    println!("Throughput: {:.0} heartbeats/sec", num_heartbeats as f64 / duration.as_secs_f64());

    // 7. Audit Log via AQL Cursor Pagination
    println!("\nFetching Critical Incident Audit Logs (Page 1):");
    let aql = r#"
        query {
            telemetry(first: 5, filter: { status: { eq: "CRITICAL" } }) {
                edges {
                    cursor
                    node {
                        drone_id
                        temp
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    "#;

    let result = db.execute(aql).await?;
    if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        let conn = &res.documents[0];
        let edges = conn.data.get("edges").unwrap().as_array().unwrap();
        for edge in edges {
            let node = edge.as_object().unwrap().get("node").unwrap().as_object().unwrap();
            println!("  - {} | Temp: {:.1}C", node.get("drone_id").unwrap(), node.get("temp").unwrap().as_f64().unwrap());
        }
        
        let pi = conn.data.get("pageInfo").unwrap().as_object().unwrap();
        println!("\n  Next Incident Page Available: {}", pi.get("hasNextPage").unwrap().as_bool().unwrap());
    }

    worker_system.stop().await?;
    println!("\nFleet monitoring complete. Aurora optimized telemetry ingestion, automated repairs, and provided paginated audit trails.");
    Ok(())
}

fn echo_banner(msg: &str) {
    println!("==========================================================");
    println!("{}", msg);
    println!("==========================================================");
}
