import { isPostgresConnected, pool, inMemoryStore } from '../config/db.js';
import { broadcastSensorUpdate, broadcastAlertNew, broadcastNodeStatus } from '../services/socketService.js';
import { deletedNodeIds } from './nodeController.js';

let latestReading = null;

export async function getLatestSensor(req, res) {
  try {
    let settings = inMemoryStore.settings;

    if (isPostgresConnected && pool) {
      const configRes = await pool.query('SELECT value FROM system_settings WHERE key = $1', ['config']);
      if (configRes.rows.length > 0) {
        settings = configRes.rows[0].value;
      }
    }

    return res.json({
      sensor: latestReading,
      device: settings
    });
  } catch (err) {
    console.error('Error fetching sensor state:', err);
    res.status(500).json({ error: 'Failed to fetch sensor state' });
  }
}

export async function ingestReading(req, res) {
  const body = req?.body || req || {};
  const nodeId = body.nodeId || body.node_id || body.id || body.node;

  if (!nodeId) {
    if (res && res.status) {
      return res.status(400).json({ error: 'nodeId is required' });
    }
    return;
  }

  // If node was explicitly deleted by user, do not resurrect it into database or store
  if (deletedNodeIds && deletedNodeIds.has(String(nodeId).toLowerCase())) {
    if (res && res.json) {
      return res.json({ message: 'Ignored reading for deleted node' });
    }
    return;
  }

  // Parse binary leak status from hardware (handles boolean true/false, "leak"/"normal", 1/0, "true"/"false")
  const isLeak = body.leak !== undefined
    ? (body.leak === true || body.leak === 1 || body.leak === 'leak' || body.leak === 'true')
    : (body.leakDetected !== undefined
        ? Boolean(body.leakDetected)
        : (body.status === 'leak' || (body.soundLevel !== undefined && parseFloat(body.soundLevel) >= (parseFloat(body.threshold) || 70.0))));

  const currentThreshold = latestReading?.threshold ?? inMemoryStore.settings?.threshold ?? 70.0;
  const numericThreshold = body.threshold !== undefined ? parseFloat(body.threshold) : currentThreshold;

  // Sound level: use passed soundLevel if present, otherwise assign representative level (88.5 dB on leak, 54.0 dB on normal)
  const hasRawSound = body.soundLevel !== undefined && body.soundLevel !== null && !isNaN(parseFloat(body.soundLevel));
  const numericSound = hasRawSound ? parseFloat(body.soundLevel) : (isLeak ? 88.5 : 54.0);

  const leakDetected = isLeak;
  const status = leakDetected ? (numericSound >= numericThreshold + 15 ? "critical" : "warning") : "normal";
  const now = Date.now();

  latestReading = {
    soundLevel: numericSound,
    threshold: numericThreshold,
    leakDetected,
    status,
    nodeId,
    timestamp: now,
  };

  try {
    if (isPostgresConnected && pool) {
      // Store in sensor_readings
      await pool.query(
        `INSERT INTO sensor_readings (node_id, sound_level, threshold, leak_detected, status, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [nodeId, numericSound, numericThreshold, leakDetected, status]
      );

      // Dynamically register or update node
      await pool.query(
        `INSERT INTO nodes (id, name, status, sound_level, last_seen)
         VALUES ($1, $1, $2, $3, NOW())
         ON CONFLICT (id) DO UPDATE
         SET sound_level = EXCLUDED.sound_level,
             status = EXCLUDED.status,
             last_seen = NOW()`,
        [nodeId, leakDetected ? 'leak' : 'normal', numericSound]
      );

      // If leak detected, always create a new unique alert
      if (leakDetected) {
        const alertId = `alert${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
        const title = status === "critical" ? "Critical Air Leak" : "Air Leak Warning";
        const desc = `Acoustic level reached ${numericSound} dB (Threshold: ${numericThreshold} dB)`;

        const newAlertRes = await pool.query(
          `INSERT INTO alerts (id, node_id, title, severity, sound_level, threshold, state, description, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, NOW())
           RETURNING *`,
          [alertId, nodeId, title, status, numericSound, numericThreshold, desc]
        );

        const alertRow = newAlertRes.rows[0];
        broadcastAlertNew({
          id: alertRow.id,
          node: alertRow.node_id,
          title: alertRow.title,
          severity: alertRow.severity,
          soundLevel: parseFloat(alertRow.sound_level),
          threshold: parseFloat(alertRow.threshold),
          state: alertRow.state,
          description: alertRow.description,
          createdAt: alertRow.created_at,
        });

        // Log to history
        const nowStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        await pool.query(
          `INSERT INTO history (id, time, node, event, status, created_at)
           VALUES ($1, $2, $3, $4, $5, NOW())`,
          [`h${Date.now()}`, `Today, ${nowStr}`, nodeId, 'Leak Event Detected', 'leak']
        );
      }
    } else {
      // In-memory fallback: dynamically register or update node
      const nodeObj = inMemoryStore.nodes.find(n => n.id === nodeId);
      if (nodeObj) {
        nodeObj.sound_level = numericSound;
        nodeObj.status = leakDetected ? 'leak' : 'normal';
        nodeObj.last_seen = new Date();
      } else {
        inMemoryStore.nodes.push({
          id: nodeId,
          name: nodeId,
          status: leakDetected ? 'leak' : 'normal',
          sound_level: numericSound,
          last_seen: new Date(),
        });
      }

      if (leakDetected) {
        const alertId = `alert${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
        const newAlert = {
          id: alertId,
          node_id: nodeId,
          node: nodeId,
          title: status === "critical" ? "Critical Air Leak" : "Air Leak Warning",
          severity: status,
          sound_level: numericSound,
          soundLevel: numericSound,
          threshold: numericThreshold,
          state: "active",
          description: `Acoustic level reached ${numericSound} dB (Threshold: ${numericThreshold} dB)`,
          created_at: new Date(),
          createdAt: new Date(),
        };
        inMemoryStore.alerts.unshift(newAlert);
        broadcastAlertNew(newAlert);

        const nowStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        inMemoryStore.history.unshift({
          id: `h${Date.now()}`,
          time: `Today, ${nowStr}`,
          node: nodeId,
          event: 'Leak Event Detected',
          status: 'leak',
          created_at: new Date(),
        });
      }
    }

    // Broadcast real-time sensor updates to all connected clients
    broadcastSensorUpdate(latestReading);
    broadcastNodeStatus({ id: nodeId, status: leakDetected ? 'leak' : 'normal', soundLevel: numericSound });

    if (res && res.status) {
      return res.status(200).json({
        success: true,
        data: latestReading,
      });
    }
  } catch (err) {
    console.error('Error ingesting reading:', err);
    if (res && res.status) {
      res.status(500).json({ error: 'Failed to process sensor reading' });
    }
  }
}

export async function getDeviceStatus(req, res) {
  try {
    let settings = inMemoryStore.settings;

    if (isPostgresConnected && pool) {
      const configRes = await pool.query('SELECT value FROM system_settings WHERE key = $1', ['config']);
      if (configRes.rows.length > 0) {
        settings = configRes.rows[0].value;
      }
    }

    return res.json(settings);
  } catch (err) {
    console.error('Error fetching device status:', err);
    res.status(500).json({ error: 'Failed to fetch device status' });
  }
}

export async function updateThreshold(req, res) {
  const { threshold } = req.body;
  if (threshold === undefined || isNaN(threshold)) {
    return res.status(400).json({ error: 'Valid threshold number is required' });
  }

  const numThreshold = parseFloat(threshold);
  latestReading.threshold = numThreshold;

  try {
    if (isPostgresConnected && pool) {
      await pool.query(
        `UPDATE system_settings 
         SET value = jsonb_set(value, '{threshold}', $1::jsonb) 
         WHERE key = 'config'`,
        [JSON.stringify(numThreshold)]
      );
    } else {
      inMemoryStore.settings.threshold = numThreshold;
    }

    broadcastSensorUpdate(latestReading);

    return res.json({
      success: true,
      threshold: numThreshold,
    });
  } catch (err) {
    console.error('Error updating threshold:', err);
    res.status(500).json({ error: 'Failed to update threshold' });
  }
}
