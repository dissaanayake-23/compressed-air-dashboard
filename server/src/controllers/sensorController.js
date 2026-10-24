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

function normalizeNodeId(id) {
  if (!id) return '';
  const s = String(id).toLowerCase().replace(/[-_\s]/g, '');
  return s.replace(/^node0*([0-9]+)$/, 'node$1');
}

function isSameNode(id1, id2) {
  if (!id1 || !id2) return false;
  if (String(id1).toLowerCase().trim() === String(id2).toLowerCase().trim()) return true;
  return normalizeNodeId(id1) === normalizeNodeId(id2);
}

export async function ingestReading(req, res) {
  const body = req?.body || req || {};
  const rawNodeId = body.nodeId || body.node_id || body.id || body.node;

  if (!rawNodeId) {
    if (res && res.status) {
      return res.status(400).json({ error: 'nodeId is required' });
    }
    return;
  }

  // Check if node is registered (flexible ID matching)
  let matchedNode = null;
  try {
    if (isPostgresConnected && pool) {
      const nodeCheck = await pool.query('SELECT * FROM nodes');
      matchedNode = nodeCheck.rows.find(r => isSameNode(r.id, rawNodeId) || isSameNode(r.name, rawNodeId));
    } else {
      matchedNode = inMemoryStore.nodes.find(n => isSameNode(n.id, rawNodeId) || isSameNode(n.name, rawNodeId));
    }
  } catch (err) {
    console.error('[Ingest] Error verifying node registration:', err);
  }

  if (!matchedNode) {
    console.warn(`🛑 [Ingest] Blocked reading: Node "${rawNodeId}" is not registered.`);
    if (res && res.status) {
      return res.status(403).json({ error: `Node "${rawNodeId}" is not registered. Message blocked.` });
    }
    return;
  }

  const nodeId = matchedNode.id;

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

      // Update registered node
      await pool.query(
        `UPDATE nodes
         SET sound_level = $1,
             status = $2,
             last_seen = NOW()
         WHERE LOWER(id) = LOWER($3)`,
        [numericSound, leakDetected ? 'leak' : 'normal', nodeId]
      );

      // If leak detected, update existing active alert for this node or create one if none exists
      if (leakDetected) {
        const title = body.alert?.title || (status === "critical" ? "Critical Air Leak" : "Air Leak Warning");
        const desc = body.alert?.description || `High acoustic level detected: ${numericSound} dB on ${nodeId}`;

        const existingAlertRes = await pool.query(
          `SELECT * FROM alerts WHERE LOWER(node_id) = LOWER($1) AND state = 'active' ORDER BY created_at DESC LIMIT 1`,
          [nodeId]
        );

        let alertRow;
        if (existingAlertRes.rows.length > 0) {
          // Update existing active alert
          const existingAlert = existingAlertRes.rows[0];
          const updatedAlertRes = await pool.query(
            `UPDATE alerts
             SET sound_level = $1,
                 threshold = $2,
                 title = $3,
                 severity = $4,
                 description = $5,
                 created_at = NOW()
             WHERE id = $6
             RETURNING *`,
            [numericSound, numericThreshold, title, status, desc, existingAlert.id]
          );
          alertRow = updatedAlertRes.rows[0];
        } else {
          // Create new alert only if no active alert exists for this node
          const alertId = `alert${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
          const newAlertRes = await pool.query(
            `INSERT INTO alerts (id, node_id, title, severity, sound_level, threshold, state, description, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, NOW())
             RETURNING *`,
            [alertId, nodeId, title, status, numericSound, numericThreshold, desc]
          );
          alertRow = newAlertRes.rows[0];

          // Log to history only when new alert starts
          const nowStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
          await pool.query(
            `INSERT INTO history (id, time, node, event, status, created_at)
             VALUES ($1, $2, $3, $4, $5, NOW())`,
            [`h${Date.now()}`, `Today, ${nowStr}`, nodeId, 'Leak Event Detected', 'leak']
          );
        }

        broadcastAlertNew({
          id: alertRow.id,
          node: alertRow.node_id,
          node_id: alertRow.node_id,
          title: alertRow.title,
          severity: alertRow.severity,
          soundLevel: parseFloat(alertRow.sound_level),
          threshold: parseFloat(alertRow.threshold),
          state: alertRow.state,
          description: alertRow.description,
          createdAt: alertRow.created_at,
        });
      }
    } else {
      // In-memory fallback: update registered node
      const nodeObj = inMemoryStore.nodes.find(n => n.id.toLowerCase() === nodeId.toLowerCase());
      if (nodeObj) {
        nodeObj.sound_level = numericSound;
        nodeObj.status = leakDetected ? 'leak' : 'normal';
        nodeObj.last_seen = new Date();
      }

      if (leakDetected) {
        const title = body.alert?.title || (status === "critical" ? "Critical Air Leak" : "Air Leak Warning");
        const desc = body.alert?.description || `High acoustic level detected: ${numericSound} dB on ${nodeId}`;

        const existingAlert = inMemoryStore.alerts.find(
          (a) => (a.node_id?.toLowerCase() === nodeId.toLowerCase() || a.node?.toLowerCase() === nodeId.toLowerCase()) && a.state === 'active'
        );

        let alertObj;
        if (existingAlert) {
          existingAlert.sound_level = numericSound;
          existingAlert.soundLevel = numericSound;
          existingAlert.threshold = numericThreshold;
          existingAlert.title = title;
          existingAlert.severity = body.alert?.severity || status;
          existingAlert.description = desc;
          existingAlert.created_at = new Date();
          existingAlert.createdAt = new Date();
          alertObj = existingAlert;
        } else {
          const alertId = `alert${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
          alertObj = {
            id: alertId,
            node_id: nodeId,
            node: nodeId,
            title,
            severity: body.alert?.severity || status,
            sound_level: numericSound,
            soundLevel: numericSound,
            threshold: numericThreshold,
            state: "active",
            description: desc,
            created_at: new Date(),
            createdAt: new Date(),
          };
          inMemoryStore.alerts.unshift(alertObj);

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

        broadcastAlertNew(alertObj);
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
