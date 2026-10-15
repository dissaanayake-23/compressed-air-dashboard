import mqtt from 'mqtt';
import { ingestReading } from '../controllers/sensorController.js';
import { query, inMemoryStore, isPostgresConnected } from '../config/db.js';

let mqttClient = null;
let mqttConnected = false;
let ioRef = null;

// ─────────────────────────────────────────────────────────────────────────────
//  MQTT TOPIC STRUCTURE (matches ESP32 firmware)
//
//  compressed-air/sensor        → raw reading { nodeId, soundLevel, leakDetected }
//  compressed-air/node/+/status → node heartbeat  { nodeId, status, soundLevel }
//  compressed-air/alert         → alert event     { nodeId, title, severity, description }
// ─────────────────────────────────────────────────────────────────────────────

export const TOPICS = {
  TELEMETRY:   process.env.MQTT_TOPIC || 'acoustic-leak-detection/sensor-data',
  SENSOR:      'compressed-air/sensor',
  NODE_STATUS: 'compressed-air/node/+/status',
  ALERT:       'compressed-air/alert',
};

/**
 * Connect to HiveMQ broker and wire up message handlers.
 * Call once from server.js, passing the Socket.io instance.
 */
export function initMqtt(io) {
  ioRef = io;

  const BROKER_URL  = process.env.HIVEMQ_BROKER_URL || 'broker.hivemq.com';
  const MQTT_USER   = process.env.HIVEMQ_USERNAME || null;
  const MQTT_PASS   = process.env.HIVEMQ_PASSWORD || null;
  const MQTT_PORT   = parseInt(process.env.HIVEMQ_PORT || '1883', 10);

  const brokerUri = BROKER_URL.startsWith('mqtts://') || BROKER_URL.startsWith('mqtt://')
    ? BROKER_URL
    : `mqtt://${BROKER_URL}:${MQTT_PORT}`;

  console.log(`\n🔌 [MQTT] Connecting to HiveMQ broker: ${brokerUri}`);

  const connectOptions = {
    clientId: `compressed-air-server-${Math.random().toString(16).slice(2, 8)}`,
    clean: true,
    connectTimeout: 8000,
    reconnectPeriod: 5000,
    rejectUnauthorized: false,
  };

  // Only add credentials if provided (private broker)
  if (MQTT_USER) connectOptions.username = MQTT_USER;
  if (MQTT_PASS) connectOptions.password = MQTT_PASS;

  mqttClient = mqtt.connect(brokerUri, connectOptions);

  // ─── Connection Events ──────────────────────────────────────────────────
  mqttClient.on('connect', () => {
    mqttConnected = true;
    console.log('✅ [MQTT] Connected to HiveMQ!');

    // Subscribe to all topics (single topic + legacy)
    mqttClient.subscribe(Object.values(TOPICS), { qos: 0 }, (err) => {
      if (err) {
        console.error('❌ [MQTT] Subscribe error:', err.message);
      } else {
        console.log('📡 [MQTT] Subscribed to topic:', TOPICS.TELEMETRY);
      }
    });

    // Broadcast MQTT status to all dashboard clients
    ioRef?.emit('mqtt:status', { connected: true });
  });

  mqttClient.on('reconnect', () => {
    console.log('🔄 [MQTT] Reconnecting to HiveMQ...');
    mqttConnected = false;
    ioRef?.emit('mqtt:status', { connected: false });
  });

  mqttClient.on('offline', () => {
    console.log('⚠️  [MQTT] Client went offline.');
    mqttConnected = false;
    ioRef?.emit('mqtt:status', { connected: false });
  });

  mqttClient.on('error', (err) => {
    console.error('❌ [MQTT] Connection error:', err.message);
    mqttConnected = false;
  });

  // ─── Message Handler ────────────────────────────────────────────────────
  mqttClient.on('message', async (topic, payload) => {
    let data;
    try {
      data = JSON.parse(payload.toString());
    } catch {
      console.warn(`[MQTT] Invalid JSON on topic "${topic}":`, payload.toString());
      return;
    }

    console.log(`[MQTT] 📨 ${topic}:`, JSON.stringify(data));

    // ── 1. Unified Single Telemetry Topic ────────────────────────────────
    if (topic === TOPICS.TELEMETRY || topic === TOPICS.SENSOR) {
      await handleSensorReading(data);
    }

    // ── 2. Node status heartbeat ────────────────────────────────────────
    else if (topic.startsWith('compressed-air/node/') && topic.endsWith('/status')) {
      await handleNodeStatus(data, topic);
    }

    // ── 3. Alert event ──────────────────────────────────────────────────
    else if (topic === TOPICS.ALERT) {
      await handleAlertEvent(data);
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
//  HANDLER: Sensor reading  { nodeId, soundLevel, threshold, leakDetected }
// ─────────────────────────────────────────────────────────────────────────────
async function handleSensorReading(data) {
  const { nodeId, soundLevel, threshold = 70.0, leakDetected } = data;

  if (!nodeId || soundLevel === undefined) {
    console.warn('[MQTT] Sensor reading missing nodeId or soundLevel:', data);
    return;
  }

  // Reuse the existing ingestReading controller
  try {
    await ingestReading({ nodeId, soundLevel, threshold, leakDetected });

    // Real-time push to dashboard
    ioRef?.emit('sensor:update', { nodeId, soundLevel, threshold, leakDetected });

    // If leak detected, build + push alert
    if (leakDetected) {
      const alert = buildAlert(nodeId, soundLevel, threshold);
      ioRef?.emit('alert:new', alert);
    }
  } catch (err) {
    console.error('[MQTT] Error ingesting sensor reading:', err.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  HANDLER: Node heartbeat  { nodeId, status, soundLevel }
// ─────────────────────────────────────────────────────────────────────────────
async function handleNodeStatus(data, topic) {
  const { nodeId, status = 'normal', soundLevel } = data;
  if (!nodeId) return;

  try {
    // Update PostgreSQL node record
    const result = await query(
      `UPDATE nodes SET status = $1, sound_level = $2, last_seen = NOW() WHERE id = $3 RETURNING *`,
      [status, soundLevel ?? 0, nodeId]
    );

    // If node not in DB yet, insert it
    if (result && result.rowCount === 0) {
      await query(
        `INSERT INTO nodes (id, name, status, sound_level, last_seen) VALUES ($1, $2, $3, $4, NOW())`,
        [nodeId, nodeId, status, soundLevel ?? 0]
      );
    }
  } catch {
    // In-memory fallback
    const node = inMemoryStore.nodes.find(n => n.id === nodeId);
    if (node) {
      node.status = status;
      if (soundLevel !== undefined) node.sound_level = soundLevel;
      node.last_seen = new Date();
    }
  }

  // Push live update to dashboard
  ioRef?.emit('node:status', { id: nodeId, status, soundLevel });
}

// ─────────────────────────────────────────────────────────────────────────────
//  HANDLER: Alert event  { nodeId, title, severity, description, soundLevel }
// ─────────────────────────────────────────────────────────────────────────────
async function handleAlertEvent(data) {
  const { nodeId, title, severity = 'warning', description, soundLevel = 0, threshold = 70.0 } = data;
  if (!nodeId) return;

  const alert = {
    id: `alert_${Date.now()}`,
    node: nodeId,
    title: title || 'Leak Detected',
    severity,
    soundLevel,
    threshold,
    description: description || `Acoustic anomaly detected on ${nodeId}`,
    state: 'active',
    createdAt: new Date().toISOString(),
  };

  try {
    await query(
      `INSERT INTO alerts (id, node_id, title, severity, sound_level, threshold, state, description, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
      [alert.id, nodeId, alert.title, severity, soundLevel, threshold, 'active', alert.description]
    );
  } catch {
    inMemoryStore.alerts.unshift({
      id: alert.id,
      node_id: nodeId,
      title: alert.title,
      severity,
      sound_level: soundLevel,
      threshold,
      state: 'active',
      description: alert.description,
      created_at: new Date(),
      resolved_at: null,
    });
  }

  ioRef?.emit('alert:new', alert);
}

// ─────────────────────────────────────────────────────────────────────────────
//  Utility: build a consistent alert object
// ─────────────────────────────────────────────────────────────────────────────
function buildAlert(nodeId, soundLevel, threshold) {
  const severity = soundLevel >= 85 ? 'critical' : 'warning';
  return {
    id: `alert_${Date.now()}`,
    node: nodeId,
    title: severity === 'critical' ? 'Critical Air Leak' : 'Air Leak Warning',
    severity,
    soundLevel,
    threshold,
    description: `Acoustic activity of ${soundLevel.toFixed(1)} dB exceeded ${threshold} dB threshold on ${nodeId}`,
    state: 'active',
    createdAt: new Date().toISOString(),
  };
}

/**
 * Publish a message to HiveMQ (for future use, e.g. sending commands to ESP32)
 */
export function mqttPublish(topic, payload) {
  if (!mqttClient || !mqttConnected) {
    console.warn('[MQTT] Cannot publish — not connected.');
    return;
  }
  mqttClient.publish(topic, JSON.stringify(payload), { qos: 1 });
}

export { mqttConnected };
