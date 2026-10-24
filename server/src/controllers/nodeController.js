import { isPostgresConnected, pool, inMemoryStore } from '../config/db.js';

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
  const { id, name, location = '', description = '' } = req.body;

  if (!id || !name) {
    return res.status(400).json({ error: 'id and name are required' });
  }

  try {
    if (isPostgresConnected && pool) {
      // Check if node already exists
      const existing = await pool.query('SELECT id FROM nodes WHERE id = $1', [id]);
      if (existing.rows.length > 0) {
        return res.status(409).json({ error: `Node with ID "${id}" already exists` });
      }

      await pool.query(
        `INSERT INTO nodes (id, name, status, sound_level, last_seen, location, description)
         VALUES ($1, $2, 'normal', 0, NOW(), $3, $4)`,
        [id, name, location, description]
      );

      return res.status(201).json({
        success: true,
        node: { id, name, status: 'normal', soundLevel: 0, lastSeen: new Date().toISOString(), location, description },
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
  const { name, location, description } = req.body;

  try {
    if (isPostgresConnected && pool) {
      const existing = await pool.query('SELECT * FROM nodes WHERE id = $1', [id]);
      if (existing.rows.length === 0) {
        return res.status(404).json({ error: 'Node not found' });
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
      if (description !== undefined) {
        updates.push(`description = $${paramIdx++}`);
        values.push(description);
      }

      if (updates.length === 0) {
        return res.status(400).json({ error: 'No fields to update' });
      }

      values.push(id);
      await pool.query(
        `UPDATE nodes SET ${updates.join(', ')} WHERE id = $${paramIdx}`,
        values
      );

      const updated = await pool.query('SELECT * FROM nodes WHERE id = $1', [id]);
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
          description: row.description || '',
        },
      });
    }

    // In-memory
    const node = inMemoryStore.nodes.find(n => n.id.toLowerCase() === id.toLowerCase());
    if (!node) {
      return res.status(404).json({ error: 'Node not found' });
    }

    if (name !== undefined) node.name = name;
    if (location !== undefined) node.location = location;
    if (description !== undefined) node.description = description;

    return res.json({ success: true, node });
  } catch (err) {
    console.error('Error updating node:', err);
    res.status(500).json({ error: 'Failed to update node' });
  }
}

export async function deleteNode(req, res) {
  const { id } = req.params;

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
