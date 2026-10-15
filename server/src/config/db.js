import pkg from 'pg';
const { Pool } = pkg;
import dotenv from 'dotenv';
dotenv.config();

let pool = null;
let isPostgresConnected = false;

// In-memory fallback store if PostgreSQL is not connected yet
const inMemoryStore = {
  nodes: [],
  alerts: [],
  history: [],
  sensorReadings: [],
  settings: {
    threshold: 70,
    esp32: { online: false },
    microphone: { name: "INMP441", active: false },
    gateway: { status: "Awaiting connection", protocol: "MQTT / WebSockets" }
  }
};

/**
 * Initialize PostgreSQL Database Connection and Schema
 */
export async function initDb() {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl || databaseUrl.trim() === '') {
    console.log('\n⚠️ [Database Notice]: No DATABASE_URL found in environment.');
    console.log('👉 Running in-memory mode with full functionality.');
    console.log('👉 To connect Railway PostgreSQL, set DATABASE_URL in server/.env or Railway variables.\n');
    isPostgresConnected = false;
    return;
  }

  try {
    const isProduction = process.env.NODE_ENV === 'production' || databaseUrl.includes('railway');
    pool = new Pool({
      connectionString: databaseUrl,
      ssl: isProduction ? { rejectUnauthorized: false } : false,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });

    const client = await pool.connect();
    console.log(' Connected successfully to PostgreSQL database!');

    // Initialize Schema
    await client.query(`
      CREATE TABLE IF NOT EXISTS nodes (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        status VARCHAR(20) DEFAULT 'normal',
        sound_level NUMERIC(5, 2) DEFAULT 0.0,
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS alerts (
        id VARCHAR(50) PRIMARY KEY,
        node_id VARCHAR(50) REFERENCES nodes(id) ON DELETE CASCADE,
        title VARCHAR(255) NOT NULL,
        severity VARCHAR(20) NOT NULL DEFAULT 'warning',
        sound_level NUMERIC(5, 2) NOT NULL,
        threshold NUMERIC(5, 2) NOT NULL DEFAULT 70.0,
        state VARCHAR(20) NOT NULL DEFAULT 'active',
        description TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        resolved_at TIMESTAMPTZ
      );

      CREATE TABLE IF NOT EXISTS history (
        id VARCHAR(50) PRIMARY KEY,
        time VARCHAR(100) NOT NULL,
        node VARCHAR(50) NOT NULL,
        event VARCHAR(255) NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'normal',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS sensor_readings (
        id SERIAL PRIMARY KEY,
        node_id VARCHAR(50) NOT NULL,
        sound_level NUMERIC(5, 2) NOT NULL,
        threshold NUMERIC(5, 2) NOT NULL DEFAULT 70.0,
        leak_detected BOOLEAN DEFAULT FALSE,
        status VARCHAR(20) DEFAULT 'normal',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS system_settings (
        key VARCHAR(50) PRIMARY KEY,
        value JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // Ensure default system configuration exists without injecting mock records
    await client.query(
      `INSERT INTO system_settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
      ['config', JSON.stringify(inMemoryStore.settings)]
    );

    client.release();
    isPostgresConnected = true;
  } catch (err) {
    console.error('❌ Failed to connect to PostgreSQL:', err.message);
    console.log('👉 Falling back to in-memory store so server continues running.');
    isPostgresConnected = false;
  }
}

export { isPostgresConnected, pool, inMemoryStore };

/**
 * Execute query safely against PostgreSQL or in-memory store
 */
export async function query(text, params = []) {
  if (isPostgresConnected && pool) {
    return pool.query(text, params);
  }
  return null;
}
