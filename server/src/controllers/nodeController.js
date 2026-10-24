import { isPostgresConnected, pool, inMemoryStore } from '../config/db.js';

// Set of explicitly deleted node IDs to prevent telemetry from immediately resurrecting them
export const deletedNodeIds = new Set();

export async function getNodes(req, res) {
  try {
    if (isPostgresConnected && pool) {
      const result = await pool.query('SELECT * FROM nodes ORDER BY id ASC');
      const nodes = result.rows.map(row => ({
        id: row.id,
        name: row.name,
        status: row.status,
        soundLevel: parseFloat(row.sound_level) || 0,
        lastSeen: row.last_seen,
        location: row.location || '',
        locationImage: row.location_image || '',
        description: row.description || '',
      }));
      return res.json(nodes);
    }

    const nodes = inMemoryStore.nodes.map(n => ({
      id: n.id,
      name: n.name,
      status: n.status,
      soundLevel: n.sound_level,
      lastSeen: n.last_seen,
      location: n.location || '',
      locationImage: n.locationImage || '',
      description: n.description || '',
    }));
    return res.json(nodes);
  } catch (err) {
    console.error('Error fetching nodes:', err);
    res.status(500).json({ error: 'Failed to fetch nodes' });
  }
}

export async function getNodeById(req, res) {
  const { id } = req.params;
  try {
    if (isPostgresConnected && pool) {
      const result = await pool.query('SELECT * FROM nodes WHERE id = $1', [id]);
      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'Node not found' });
      }
      const row = result.rows[0];
      return res.json({
        id: row.id,
        name: row.name,
        status: row.status,
        soundLevel: parseFloat(row.sound_level) || 0,
        lastSeen: row.last_seen,
        location: row.location || '',
        locationImage: row.location_image || '',
        description: row.description || '',
      });
    }

    const node = inMemoryStore.nodes.find(n => n.id.toLowerCase() === id.toLowerCase());
    if (!node) {
      return res.status(404).json({ error: 'Node not found' });
    }
    return res.json(node);
  } catch (err) {
    console.error('Error fetching node:', err);
    res.status(500).json({ error: 'Failed to fetch node' });
  }
}

export async function createNode(req, res) {
  const { id, name, location = '', locationImage = '', description = '' } = req.body;

  if (!id || !name) {
    return res.status(400).json({ error: 'id and name are required' });
  }

  // Clear tombstone if previously deleted
  deletedNodeIds.delete(String(id).toLowerCase());

  try {
    if (isPostgresConnected && pool) {
      // Check if node already exists
      const existing = await pool.query('SELECT id FROM nodes WHERE id = $1', [id]);
      if (existing.rows.length > 0) {
        return res.status(409).json({ error: `Node with ID "${id}" already exists` });
      }

      await pool.query(
        `INSERT INTO nodes (id, name, status, sound_level, last_seen, location, location_image, description)
         VALUES ($1, $2, 'normal', 0, NOW(), $3, $4, $5)`,
        [id, name, location, locationImage, description]
      );

      return res.status(201).json({
        success: true,
        node: { id, name, status: 'normal', soundLevel: 0, lastSeen: new Date().toISOString(), location, locationImage, description },
      });
    }

    // In-memory
    const exists = inMemoryStore.nodes.some(n => n.id.toLowerCase() === id.toLowerCase());
    if (exists) {
      return res.status(409).json({ error: `Node with ID "${id}" already exists` });
    }

    const newNode = {
      id,
      name,
      status: 'normal',
      sound_level: 0,
      soundLevel: 0,
      last_seen: new Date(),
      lastSeen: new Date().toISOString(),
      location,
      locationImage,
      description,
    };
    inMemoryStore.nodes.push(newNode);

    return res.status(201).json({ success: true, node: newNode });
  } catch (err) {
    console.error('Error creating node:', err);
    res.status(500).json({ error: 'Failed to create node' });
  }
}

export async function updateNode(req, res) {
  const { id } = req.params;
  const { name, location, locationImage, description } = req.body;

  try {
    if (isPostgresConnected && pool) {
      const existing = await pool.query('SELECT * FROM nodes WHERE LOWER(id) = LOWER($1)', [id]);
      
      if (existing.rows.length === 0) {
        // Node does not exist in database yet (e.g. streaming from MQTT) - auto-insert (upsert)
        await pool.query(
          `INSERT INTO nodes (id, name, status, sound_level, last_seen, location, location_image, description)
           VALUES ($1, $2, 'normal', 0, NOW(), $3, $4, $5)`,
          [id, name || id, location || '', locationImage || '', description || '']
        );

        return res.json({
          success: true,
          node: {
            id,
            name: name || id,
            status: 'normal',
            soundLevel: 0,
            lastSeen: new Date().toISOString(),
            location: location || '',
            locationImage: locationImage || '',
            description: description || '',
          },
        });
      }

      const updates = [];
      const values = [];
      let paramIdx = 1;

      if (name !== undefined) {
        updates.push(`name = $${paramIdx++}`);
        values.push(name);
      }
      if (location !== undefined) {
        updates.push(`location = $${paramIdx++}`);
        values.push(location);
      }
      if (locationImage !== undefined) {
        updates.push(`location_image = $${paramIdx++}`);
        values.push(locationImage);
      }
      if (description !== undefined) {
        updates.push(`description = $${paramIdx++}`);
        values.push(description);
      }

      if (updates.length === 0) {
        return res.status(400).json({ error: 'No fields to update' });
      }

      values.push(id);
      await pool.query(
        `UPDATE nodes SET ${updates.join(', ')} WHERE LOWER(id) = LOWER($${paramIdx})`,
        values
      );

      const updated = await pool.query('SELECT * FROM nodes WHERE LOWER(id) = LOWER($1)', [id]);
      const row = updated.rows[0];
      return res.json({
        success: true,
        node: {
          id: row.id,
          name: row.name,
          status: row.status,
          soundLevel: parseFloat(row.sound_level) || 0,
          lastSeen: row.last_seen,
          location: row.location || '',
          locationImage: row.location_image || '',
          description: row.description || '',
        },
      });
    }

    // In-memory
    let node = inMemoryStore.nodes.find(n => n.id.toLowerCase() === id.toLowerCase());
    if (!node) {
      node = {
        id,
        name: name || id,
        status: 'normal',
        sound_level: 0,
        soundLevel: 0,
        last_seen: new Date(),
        lastSeen: new Date().toISOString(),
        location: location || '',
        locationImage: locationImage || '',
        description: description || '',
      };
      inMemoryStore.nodes.push(node);
      return res.json({ success: true, node });
    }

    if (name !== undefined) node.name = name;
    if (location !== undefined) node.location = location;
    if (locationImage !== undefined) node.locationImage = locationImage;
    if (description !== undefined) node.description = description;

    return res.json({ success: true, node });
  } catch (err) {
    console.error('Error updating node:', err);
    res.status(500).json({ error: 'Failed to update node' });
  }
}

export async function deleteNode(req, res) {
  const { id } = req.params;

  // Add to deleted tombstone to prevent telemetry auto-resurrecting this node
  if (id) {
    deletedNodeIds.add(String(id).toLowerCase());
  }

  try {
    if (isPostgresConnected && pool) {
      const existing = await pool.query('SELECT id FROM nodes WHERE id = $1', [id]);
      if (existing.rows.length === 0) {
        return res.status(404).json({ error: 'Node not found' });
      }

      // Cascade will handle related alerts
      await pool.query('DELETE FROM nodes WHERE id = $1', [id]);
      return res.json({ success: true, message: `Node "${id}" deleted` });
    }

    // In-memory
    const idx = inMemoryStore.nodes.findIndex(n => n.id.toLowerCase() === id.toLowerCase());
    if (idx === -1) {
      return res.status(404).json({ error: 'Node not found' });
    }
    inMemoryStore.nodes.splice(idx, 1);

    // Also remove any alerts tied to this node
    inMemoryStore.alerts = inMemoryStore.alerts.filter(a => a.node_id !== id);

    return res.json({ success: true, message: `Node "${id}" deleted` });
  } catch (err) {
    console.error('Error deleting node:', err);
    res.status(500).json({ error: 'Failed to delete node' });
  }
}
