import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';

import { initDb, isPostgresConnected } from './config/db.js';
import { initSocket } from './services/socketService.js';
import { initMqtt, mqttConnected } from './services/mqttService.js';
import nodeRoutes from './routes/nodeRoutes.js';
import alertRoutes from './routes/alertRoutes.js';
import historyRoutes from './routes/historyRoutes.js';
import sensorRoutes from './routes/sensorRoutes.js';
import { ingestReading } from './controllers/sensorController.js';

dotenv.config();

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 5000;
const CLIENT_URL = process.env.CLIENT_URL || 'http://localhost:5173';

// Setup Socket.io
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST', 'PATCH'],
  },
});

initSocket(io);

// Middleware
app.use(cors({ origin: '*' }));
app.use(express.json());

// Request logging in development
app.use((req, res, next) => {
  if (req.path !== '/api/health') {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  }
  next();
});

// API Routes
app.use('/api/nodes', nodeRoutes);
app.use('/api/alerts', alertRoutes);
app.use('/api/history', historyRoutes);
app.use('/api/sensor', sensorRoutes);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    database: isPostgresConnected ? 'PostgreSQL (Railway/Connected)' : 'In-Memory Fallback',
    isPostgresConnected,
    mqtt: mqttConnected ? 'HiveMQ Connected' : (process.env.HIVEMQ_BROKER_URL ? 'Connecting...' : 'Not Configured'),
    mqttConnected,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

// MQTT status endpoint
app.get('/api/mqtt/status', (req, res) => {
  res.json({
    connected: mqttConnected,
    broker: process.env.HIVEMQ_BROKER_URL || null,
    status: mqttConnected ? 'Connected to HiveMQ' : (process.env.HIVEMQ_BROKER_URL ? 'Connecting / Reconnecting' : 'HIVEMQ_BROKER_URL not set in .env'),
    topics: [
      'compressed-air/sensor',
      'compressed-air/node/+/status',
      'compressed-air/alert',
    ],
  });
});

// Root welcome endpoint
app.get('/', (req, res) => {
  res.json({
    name: 'Compressed Air Acoustic Leak Detection API',
    version: '1.0.0',
    documentation: {
      health: 'GET /api/health',
      nodes: 'GET /api/nodes',
      alerts: 'GET /api/alerts (or ?state=active|resolved)',
      history: 'GET /api/history',
      sensorLatest: 'GET /api/sensor/latest',
      sensorIngest: 'POST /api/sensor/reading (from ESP32)',
      deviceStatus: 'GET /api/sensor/device',
    },
    databaseStatus: isPostgresConnected ? 'PostgreSQL Connected' : 'Waiting for DATABASE_URL'
  });
});

// Optional realistic acoustic simulator for demonstration (off by default)
let simulatorInterval = null;
function startSimulator() {
  if (process.env.ENABLE_SIMULATOR !== 'true') return;

  console.log('⚡ Acoustic simulator active (generating realistic ambient noise and periodic leak tests)');

  let step = 0;
  simulatorInterval = setInterval(async () => {
    step++;
    // Simulate ambient acoustic sensor variations (between 41dB and 46dB)
    let soundLevel = 42.0 + Math.sin(step * 0.4) * 3.5 + (Math.random() * 2 - 1);
    let nodeId = "Node 03";

    // Periodically simulate a transient high acoustic spike (every 25 cycles)
    if (step % 25 === 0) {
      soundLevel = 87.5 + (Math.random() * 4); // Trigger leak
      nodeId = Math.random() > 0.5 ? "Node 03" : "Node 05";
    }

    soundLevel = Math.round(soundLevel * 10) / 10;

    // Ingest without HTTP overhead
    const mockReq = {
      body: {
        nodeId,
        soundLevel,
        threshold: 70
      }
    };
    const mockRes = {
      status: () => ({ json: () => {} }),
      json: () => {}
    };

    try {
      await ingestReading(mockReq, mockRes);
    } catch (e) {
      // ignore simulator cycle errors
    }
  }, 3500);
}

// Start Server
async function startServer() {
  await initDb();

  // Connect to HiveMQ MQTT broker
  initMqtt(io);

  server.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 Compressed Air Leak Detection Server running on port ${PORT}`);
    console.log(`📡 WebSocket ready on ws://localhost:${PORT}`);
    console.log(`🗄️  Database: ${isPostgresConnected ? 'PostgreSQL Connected' : 'In-Memory (Awaiting DATABASE_URL)'}`);
    console.log(`🔌 MQTT: ${process.env.HIVEMQ_BROKER_URL ? 'Connecting to HiveMQ...' : 'Not configured (add HIVEMQ_BROKER_URL to .env)'}`);
    console.log(`======================================================\n`);

    startSimulator();
  });
}

startServer();

export { app, server };
