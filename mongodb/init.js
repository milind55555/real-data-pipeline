// MongoDB initialization for the IoT real-time pipeline.
// Runs automatically when the mongo container starts.
// Creates the 'events' collection and indexes used by Spark writes
// and ad-hoc queries.

use pipeline;

// Events collection holds raw, validated IoT telemetry.
db.createCollection("events", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["device_id", "timestamp_ms"],
            properties: {
                device_id: { bsonType: "string" },
                timestamp_ms: { bsonType: "long" },
                voltage_v: { bsonType: "double" },
                current_a: { bsonType: "double" },
                power_w: { bsonType: "double" },
                temperature_c: { bsonType: "double" },
                grid_frequency_hz: { bsonType: "double" }
            }
        }
    }
});

// Indexes for time-series queries and per-device lookups.
db.events.createIndex({ "timestamp_ms": 1 }, { name: "ts_idx" });
db.events.createIndex({ "device_id": 1, "timestamp_ms": -1 }, { name: "device_ts_idx" });
db.events.createIndex({ "device_id": 1 }, { name: "device_idx" });

// TTL index to auto-expire raw events older than 30 days (optional).
// db.events.createIndex({ "timestamp_ms": 1 }, { expireAfterSeconds: 2592000, name: "ttl_idx" });

print("MongoDB pipeline database initialized successfully.");