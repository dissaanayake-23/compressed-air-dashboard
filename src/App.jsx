import { useState, useEffect, useRef } from "react";
import { io } from "socket.io-client";
import "./App.css";

import {
  LayoutDashboard,
  Activity,
  Bell,
  History,
  Cpu,
  ShieldCheck,
  AlertTriangle,
  Trash2,
  Plus,
  Edit3,
  MapPin,
  Server,
  Upload,
  X,
  ImageIcon,
  Eye,
  TrendingUp,
  TrendingDown,
  Wifi,
  Zap,
  Clock,
  Radio,
  Volume2,
  Layers,
  CheckCircle2,
  BarChart2,
  PieChart,
  ArrowUpRight,
  Info,
  RefreshCw,
} from "lucide-react";

function App() {
  const [activePage, setActivePage] = useState("dashboard");

  // Load from instant local cache if available, so refresh has ZERO delay
  const [alerts, setAlerts] = useState(() => {
    try {
      const saved = localStorage.getItem("cached_alerts");
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  const [nodes, setNodes] = useState(() => {
    try {
      const saved = localStorage.getItem("cached_nodes");
      if (saved !== null) {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed)) {
          // If cached data contains the legacy mock nodes, purge them
          const isLegacyMock =
            parsed.length === 3 &&
            parsed.some((n) => n.id === "Node 01" && n.location === "Compressor Room A") &&
            parsed.some((n) => n.id === "Node 02" && n.location === "Distribution Line B");
          if (!isLegacyMock) {
            return parsed;
          }
        }
      }
      return [];
    } catch {
      return [];
    }
  });

  // Track explicitly deleted node IDs so streaming telemetry doesn't immediately resurrect them
  const [deletedNodeIds, setDeletedNodeIds] = useState(() => {
    try {
      const saved = localStorage.getItem("deleted_node_ids");
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  const deletedNodeIdsRef = useRef(
    new Set(
      (() => {
        try {
          const saved = localStorage.getItem("deleted_node_ids");
          const arr = saved ? JSON.parse(saved) : [];
          return Array.isArray(arr) ? arr.map((id) => String(id).toLowerCase()) : [];
        } catch {
          return [];
        }
      })()
    )
  );

  const markNodeDeleted = (nodeId) => {
    if (!nodeId) return;
    const lower = String(nodeId).toLowerCase();
    deletedNodeIdsRef.current.add(lower);
    const updated = Array.from(deletedNodeIdsRef.current);
    setDeletedNodeIds(updated);
    try {
      localStorage.setItem("deleted_node_ids", JSON.stringify(updated));
    } catch {}

    // Clean up any alerts tied to this deleted node
    setAlerts((currentAlerts) => {
      const filtered = currentAlerts.filter((a) => a.node !== nodeId);
      try { localStorage.setItem("cached_alerts", JSON.stringify(filtered)); } catch {}
      return filtered;
    });
  };

  const markNodeRestored = (nodeId) => {
    if (!nodeId) return;
    const lower = String(nodeId).toLowerCase();
    deletedNodeIdsRef.current.delete(lower);
    const updated = Array.from(deletedNodeIdsRef.current);
    setDeletedNodeIds(updated);
    try {
      localStorage.setItem("deleted_node_ids", JSON.stringify(updated));
    } catch {}
  };

  const [soundLevel, setSoundLevel] = useState(0);
  const [threshold, setThreshold] = useState(70);
  const [leakDetected, setLeakDetected] = useState(false);
  const [backendStatus, setBackendStatus] = useState("connecting");
  const [historyRecords, setHistoryRecords] = useState([]);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [loading, setLoading] = useState(true);

  // Live acoustic rolling telemetry window (populated by live incoming telemetry)
  const [acousticHistory, setAcousticHistory] = useState([]);

  // Per-node sparkline histories (populated by live incoming telemetry)
  const [nodeHistories, setNodeHistories] = useState({});

  // Industrial operational activity log (populated by live incoming events)
  const [recentActivities, setRecentActivities] = useState([]);

  const [lastMqttTime, setLastMqttTime] = useState(null);
  const [mqttEvents, setMqttEvents] = useState([]);

  const activeAlerts = alerts.filter((alert) => alert.state === "active");
  const resolvedAlerts = alerts.filter((alert) => alert.state === "resolved");
  const totalAlerts = alerts.length;

  const navItems = [
    {
      id: "dashboard",
      label: "Dashboard",
      icon: LayoutDashboard,
    },
    {
      id: "alerts",
      label: "Alerts",
      icon: Bell,
    },
    {
      id: "nodes",
      label: "Nodes",
      icon: Server,
    },
    {
      id: "history",
      label: "History",
      icon: History,
    },
  ];

  /* =======================================================
     HIVEMQ PUBLIC BROKER MQTT & REAL-TIME STREAMING
     ======================================================= */
  const MQTT_BROKER = import.meta.env.VITE_MQTT_BROKER_URL || "wss://broker.hivemq.com:8884/mqtt";
  const MQTT_TOPIC = import.meta.env.VITE_MQTT_TOPIC || "acoustic-leak-detection/sensor-data";

  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_URL || "";
    let mqttClient = null;

    // Helper: Process incoming telemetry from ESP32 via MQTT (or fallback REST / Socket.io)
    const processTelemetry = (data) => {
      if (!data) return;

      // Support any node ID key from hardware (require valid ID, do not default to Node 01)
      const nodeId = data.nodeId || data.node_id || data.id || data.node;
      if (!nodeId) return;

      // If user explicitly deleted this node, ignore telemetry so it doesn't resurrect
      if (deletedNodeIdsRef.current && deletedNodeIdsRef.current.has(String(nodeId).toLowerCase())) {
        return;
      }
      const {
        soundLevel,
        threshold: itemThreshold = 70,
        leak,
        leakDetected: rawLeak,
        alert: alertInfo,
        status: rawStatus,
      } = data;

      // Robust binary leak resolution (handles boolean true/false, "leak"/"normal", 1/0, "true"/"false")
      const isLeak = leak !== undefined
        ? (leak === true || leak === 1 || leak === 'leak' || leak === 'true' || leak === 'LEAK')
        : (rawLeak !== undefined
            ? Boolean(rawLeak)
            : (rawStatus === 'leak' || rawStatus === 'LEAK' || (soundLevel !== undefined && soundLevel !== null && parseFloat(soundLevel) >= (parseFloat(itemThreshold) || 70.0))));

      const hasRawSound = soundLevel !== undefined && soundLevel !== null && !isNaN(parseFloat(soundLevel));
      const numericThreshold = parseFloat(itemThreshold) || 70.0;

      // When hardware sends only nodeId and leak/no-leak, assign representative operational acoustic amplitude
      // (88.5 dB on leak anomaly, 54.0 dB on normal nominal baseline)
      const numericSound = hasRawSound ? parseFloat(soundLevel) : (isLeak ? 88.5 : 54.0);

      setSoundLevel(numericSound);

      // Update rolling acoustic telemetry window
      setAcousticHistory((prev) => {
        const timeStr = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        const newPoint = {
          time: timeStr,
          db: numericSound,
          isLeak,
          node: nodeId,
        };
        return [...prev, newPoint].slice(-30);
      });

      // Update node-specific sparkline history
      setNodeHistories((prev) => {
        const curr = prev[nodeId] || [50, 52, 55, 58, 62];
        return {
          ...prev,
          [nodeId]: [...curr, numericSound].slice(-16),
        };
      });

      if (!isNaN(numericThreshold)) {
        setThreshold(numericThreshold);
      }
      setLeakDetected(isLeak);
      setLastMqttTime(Date.now());

      // Dynamically register or update node
      setNodes((currentNodes) => {
        const existingIdx = currentNodes.findIndex((n) => n.id === nodeId);
        const nodeData = {
          id: nodeId,
          name: nodeId,
          soundLevel: numericSound,
          status: isLeak ? "leak" : "normal",
          lastSeen: new Date().toISOString(),
          isBinarySensor: !hasRawSound,
        };

        let updated;
        if (existingIdx >= 0) {
          updated = [...currentNodes];
          updated[existingIdx] = { ...updated[existingIdx], ...nodeData };
        } else {
          updated = [...currentNodes, nodeData];
        }
        try { localStorage.setItem("cached_nodes", JSON.stringify(updated)); } catch {}
        return updated;
      });

      // If leak detected, create active alert (preventing spam duplicates)
      if (isLeak) {
        const title = alertInfo?.title || "Compressed Air Leak Detected";
        const severity = alertInfo?.severity || "critical";
        const description = alertInfo?.description || `Acoustic leak detected on ${nodeId} (Binary Hardware Signal)`;

        setAlerts((currentAlerts) => {
          const alreadyActive = currentAlerts.some((a) => a.node === nodeId && a.state === "active");
          if (alreadyActive) return currentAlerts;

          const newAlert = {
            id: `alert-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
            node: nodeId,
            title,
            severity,
            soundLevel: numericSound,
            threshold: numericThreshold,
            state: "active",
            description,
            createdAt: new Date().toISOString(),
          };
          const updated = [newAlert, ...currentAlerts];
          try { localStorage.setItem("cached_alerts", JSON.stringify(updated)); } catch {}
          return updated;
        });

        // Add history record
        const timeStr = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        setHistoryRecords((prev) => [
          {
            id: `h-${Date.now()}`,
            time: `Today, ${timeStr}`,
            node: nodeId,
            event: "Leak Event Detected",
            status: "leak",
          },
          ...prev.slice(0, 49),
        ]);
      }

      // Add to incoming real-time MQTT events feed
      const eventTime = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      setMqttEvents((prev) => [
        {
          id: `ev-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          node: nodeId,
          status: isLeak ? "leak" : "normal",
          time: eventTime,
        },
        ...prev.slice(0, 11),
      ]);
    };

    // 1. Connect to HiveMQ Public MQTT Broker over Secure WebSockets (WSS)
    const connectHiveMQ = () => {
      const mqttLib = typeof window !== "undefined" ? window.mqtt : null;
      if (!mqttLib) {
        setTimeout(connectHiveMQ, 400);
        return;
      }

      try {
        console.log(`🔌 [HiveMQ MQTT] Connecting to broker: ${MQTT_BROKER}...`);
        mqttClient = mqttLib.connect(MQTT_BROKER, {
          clientId: `dashboard_${Math.random().toString(16).substring(2, 10)}`,
          clean: true,
          reconnectPeriod: 3000,
          connectTimeout: 10000,
        });

        mqttClient.on("connect", () => {
          console.log(`✅ [HiveMQ MQTT] Connected to broker: ${MQTT_BROKER}`);
          setBackendStatus("connected");
          setLoading(false);

          const topicsToSub = [
            MQTT_TOPIC,
            "acoustic-leak-detection/sensor-data",
            "acoustic-leak-detection/telemetry",
            "acoustic-leak-detection/node/+",
            "compressed-air/sensor",
            "compressed-air/#"
          ];
          topicsToSub.forEach((t) => {
            mqttClient.subscribe(t, { qos: 0 }, (err) => {
              if (err) console.error(`❌ [HiveMQ MQTT] Subscription error on ${t}:`, err);
            });
          });
          console.log(`📡 [HiveMQ MQTT] Subscribed to telemetry topics`);
        });

        mqttClient.on("message", (topic, payload) => {
          try {
            const data = JSON.parse(payload.toString());
            console.log(`📥 [HiveMQ MQTT] Telemetry received on ${topic}:`, data);
            processTelemetry(data);
          } catch (err) {
            console.warn("Could not parse MQTT JSON payload:", payload.toString());
          }
        });

        mqttClient.on("reconnect", () => {
          console.log("🔄 [HiveMQ MQTT] Reconnecting to broker...");
          setBackendStatus("connecting");
        });

        mqttClient.on("close", () => {
          setBackendStatus("connecting");
        });

        mqttClient.on("error", (err) => {
          console.error("❌ [HiveMQ MQTT] Error:", err?.message || err);
        });
      } catch (err) {
        console.error("❌ [HiveMQ MQTT] Initialization error:", err);
      }
    };

    connectHiveMQ();

    // 2. Connect to Socket.io backend if API_BASE is configured
    let socket = null;
    if (API_BASE) {
      try {
        socket = io(API_BASE, { autoConnect: true });
        socket.on("connect", () => {
          console.log("🔌 [Socket.io] Connected to server backend");
        });
        socket.on("sensor:update", (reading) => {
          console.log("📥 [Socket.io] sensor:update:", reading);
          processTelemetry(reading);
        });
        socket.on("node:status", (statusUpdate) => {
          if (statusUpdate?.id) {
            processTelemetry({
              nodeId: statusUpdate.id,
              status: statusUpdate.status,
              soundLevel: statusUpdate.soundLevel,
            });
          }
        });
        socket.on("alert:new", (newAlert) => {
          setAlerts((currentAlerts) => {
            if (currentAlerts.some((a) => a.id === newAlert.id)) return currentAlerts;
            return [newAlert, ...currentAlerts];
          });
        });
      } catch (e) {
        console.warn("Socket.io connection error:", e);
      }

      // Initial REST API fetch
      fetch(`${API_BASE}/api/sensor/latest`)
        .then((r) => r.json())
        .then((res) => {
          if (res?.sensor) processTelemetry(res.sensor);
        })
        .catch(() => {});
    }

    return () => {
      if (mqttClient) {
        console.log("🔌 [HiveMQ MQTT] Disconnecting...");
        mqttClient.end(true);
      }
      if (socket) {
        socket.disconnect();
      }
    };
  }, [MQTT_BROKER, MQTT_TOPIC]);

  /* =======================================================
     RESOLVE ALERT
     ======================================================= */
  async function resolveAlert(id) {
    const API_BASE = import.meta.env.VITE_API_URL || "";

    // Use functional updater to get the current alert before changing state
    let resolvedNodeId = null;

    setAlerts((currentAlerts) => {
      // Find the alert being resolved from the CURRENT state (not stale closure)
      const target = currentAlerts.find((a) => a.id === id);
      if (target) {
        resolvedNodeId = target.node;
      }

      const updated = currentAlerts.map((alert) =>
        alert.id === id ? { ...alert, state: "resolved", resolvedAt: new Date().toISOString() } : alert
      );

      // Check if this node has any OTHER active alerts remaining
      const nodeStillHasActive = updated.some(
        (a) => a.node === resolvedNodeId && a.state === "active"
      );

      // Update nodes status if no other active alerts remain for this node
      if (resolvedNodeId && !nodeStillHasActive) {
        setNodes((currentNodes) => {
          const updatedNodes = currentNodes.map((n) =>
            n.id === resolvedNodeId ? { ...n, status: "normal" } : n
          );
          try { localStorage.setItem("cached_nodes", JSON.stringify(updatedNodes)); } catch {}
          return updatedNodes;
        });
      }

      try { localStorage.setItem("cached_alerts", JSON.stringify(updated)); } catch {}
      return updated;
    });

    if (selectedAlert?.id === id) {
      setSelectedAlert((prev) => (prev ? { ...prev, state: "resolved" } : null));
    }

    // Add history record
    if (resolvedNodeId) {
      const nowStr = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
      setHistoryRecords((prev) => [
        {
          id: `h-${Date.now()}`,
          time: `Today, ${nowStr}`,
          node: resolvedNodeId,
          event: "Leak Alert Resolved",
          status: "normal",
        },
        ...prev.slice(0, 49),
      ]);
    }

    if (API_BASE) {
      try {
        await fetch(`${API_BASE}/api/alerts/${id}/resolve`, { method: "PATCH" });
      } catch (err) {
        console.warn("Could not sync resolution with backend:", err);
      }
    }
  }

  /* =======================================================
     DELETE ALERT
     ======================================================= */
  async function deleteAlert(id) {
    const API_BASE = import.meta.env.VITE_API_URL || "";

    // If the deleted alert is currently selected, close the details panel
    if (selectedAlert?.id === id) {
      setSelectedAlert(null);
    }

    // Optimistic UI update — remove alert from state
    const deletedAlert = alerts.find((a) => a.id === id);
    setAlerts((currentAlerts) => {
      const updated = currentAlerts.filter((alert) => alert.id !== id);
      try { localStorage.setItem("cached_alerts", JSON.stringify(updated)); } catch {}
      return updated;
    });

    // If the deleted alert was active and no other active alerts remain for that node, reset node
    if (deletedAlert && deletedAlert.state === "active") {
      setNodes((currentNodes) => {
        const otherActive = alerts.some(
          (a) => a.id !== id && a.node === deletedAlert.node && a.state === "active"
        );
        if (!otherActive) {
          return currentNodes.map((n) =>
            n.id === deletedAlert.node ? { ...n, status: "normal" } : n
          );
        }
        return currentNodes;
      });
    }

    // Sync with backend
    if (API_BASE) {
      try {
        await fetch(`${API_BASE}/api/alerts/${id}`, { method: "DELETE" });
      } catch (err) {
        console.warn("Could not sync deletion with backend:", err);
      }
    }
  }

  return (
    <div className="app">

      {/* ===================================================
          SIDEBAR
      =================================================== */}

      <aside className="sidebar">

        <div className="brand">
          <div className="brand-icon">
            <Activity size={24} />
          </div>

          <div>
            <h2>Compressed Air</h2>
            <h2>Leak Detection</h2>
            <p>Real-Time Monitoring</p>
          </div>
        </div>

        <div className="sidebar-section-title">
          MONITORING
        </div>

        <nav className="nav-menu">

          {navItems.map((item) => {
            const Icon = item.icon;

            return (
              <button
                key={item.id}
                className={`nav-item ${
                  activePage === item.id ? "active" : ""
                }`}
                onClick={() => setActivePage(item.id)}
              >
                <Icon size={19} />

                <span>{item.label}</span>

                {item.id === "alerts" && activeAlerts.length > 0 && (
                  <span className="alert-number">
                    {activeAlerts.length}
                  </span>
                )}
              </button>
            );
          })}

        </nav>

      </aside>

      {/* ===================================================
          MAIN CONTENT AREA
      =================================================== */}

      <main className="main-content">

        {/* =================================================
            DASHBOARD
        ================================================= */}

        {activePage === "dashboard" && (
          <>
            <SimplePageHeader
              page="Dashboard"
              title="Acoustic Leak Dashboard"
              subtitle={
                nodes.length > 0
                  ? `Real-time monitoring across ${nodes.length} connected detection node${nodes.length === 1 ? "" : "s"}.`
                  : "Real-time acoustic monitoring and detection platform."
              }
              backendStatus={backendStatus}
            />

            <Dashboard
              alerts={alerts}
              nodes={nodes}
              leakDetected={leakDetected}
              mqttTopic={MQTT_TOPIC}
              mqttEvents={mqttEvents}
              setActivePage={setActivePage}
            />
          </>
        )}

        {/* =================================================
            ALERTS
        ================================================= */}

        {activePage === "alerts" && (
          <>
            <SimplePageHeader
              page="Alerts"
              title="Leak Alerts"
              subtitle="Monitor active and resolved acoustic leak events."
              backendStatus={backendStatus}
            />

            <AlertsPage
              alerts={alerts}
              selectedAlert={selectedAlert}
              setSelectedAlert={setSelectedAlert}
              resolveAlert={resolveAlert}
              deleteAlert={deleteAlert}
              loading={loading}
            />
          </>
        )}

        {/* =================================================
            NODES
        ================================================= */}

        {activePage === "nodes" && (
          <>
            <SimplePageHeader
              page="Nodes"
              title="Node Management"
              subtitle="Register, edit, and manage monitoring nodes."
              backendStatus={backendStatus}
            />

            <NodesPage
              nodes={nodes}
              setNodes={setNodes}
              alerts={alerts}
              onDeleteNode={markNodeDeleted}
              onRegisterNode={markNodeRestored}
            />
          </>
        )}

        {/* =================================================
            HISTORY
        ================================================= */}

        {activePage === "history" && (
          <>
            <SimplePageHeader
              page="History"
              title="Monitoring History"
              subtitle="Previous acoustic readings and detection events."
              backendStatus={backendStatus}
            />

            <HistoryPage
              historyRecords={historyRecords}
              nodes={nodes}
            />
          </>
        )}

      </main>
    </div>
  );
}

/* =========================================================
   HEADER COMPONENT
   ========================================================= */

function SimplePageHeader({
  page,
  title,
  subtitle,
  backendStatus = "connected",
}) {
  return (
    <header className="topbar">

      <div>
        <p className="breadcrumb">
          Monitoring / {page}
        </p>

        <h1>{title}</h1>

        <p className="subtitle">
          {subtitle}
        </p>
      </div>

    </header>
  );
}

/* =========================================================
   DASHBOARD COMPONENT
   ========================================================= */

/* =========================================================
   DASHBOARD COMPONENT - CLEAN INDUSTRIAL ACOUSTIC MONITORING
   ========================================================= */

function formatTimeAgo(isoOrDate) {
  if (!isoOrDate) return "Just now";
  try {
    const d = new Date(isoOrDate);
    if (isNaN(d.getTime())) return "Just now";
    const diffSec = Math.floor((Date.now() - d.getTime()) / 1000);
    if (diffSec < 15) return "Just now";
    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  } catch {
    return "Just now";
  }
}

function Dashboard({
  alerts = [],
  nodes = [],
  leakDetected = false,
  mqttTopic = "acoustic-leak-detection/sensor-data",
  mqttEvents = [],
  setActivePage,
}) {
  const [selectedNodeDetails, setSelectedNodeDetails] = useState(null);

  const totalAlerts = alerts.length;
  const activeAlerts = alerts.filter((a) => a.state === "active").length;
  const resolvedAlerts = alerts.filter((a) => a.state === "resolved").length;

  // Global system leak flag: active if any alert is active, leakDetected is true, or any node has status === "leak"
  const isSystemLeak = leakDetected || activeAlerts > 0 || nodes.some((n) => n.status === "leak");

  return (
    <div className="clean-dashboard">

      {/* ===================================================
          1. SUMMARY CARDS
      =================================================== */}
      <section className="clean-summary-grid">
        <div className="clean-summary-card">
          <div className="clean-summary-icon blue">
            <Bell size={22} />
          </div>
          <div className="clean-summary-content">
            <span className="clean-summary-label">Total Alerts</span>
            <strong className="clean-summary-value">{totalAlerts}</strong>
            <small className="clean-summary-sub">Recorded events</small>
          </div>
        </div>

        <div className={`clean-summary-card ${activeAlerts > 0 ? "has-active" : ""}`}>
          <div className={`clean-summary-icon ${activeAlerts > 0 ? "red" : "orange"}`}>
            <AlertTriangle size={22} />
          </div>
          <div className="clean-summary-content">
            <span className="clean-summary-label">Active Alerts</span>
            <strong className="clean-summary-value" style={{ color: activeAlerts > 0 ? "#dc2626" : "#0f172a" }}>
              {activeAlerts}
            </strong>
            <small className="clean-summary-sub">
              {activeAlerts > 0 ? "Require attention" : "All nodes nominal"}
            </small>
          </div>
        </div>

        <div className="clean-summary-card">
          <div className="clean-summary-icon green">
            <ShieldCheck size={22} />
          </div>
          <div className="clean-summary-content">
            <span className="clean-summary-label">Resolved Alerts</span>
            <strong className="clean-summary-value">{resolvedAlerts}</strong>
            <small className="clean-summary-sub">Issues cleared</small>
          </div>
        </div>

        <div className="clean-summary-card">
          <div className="clean-summary-icon cyan">
            <Cpu size={22} />
          </div>
          <div className="clean-summary-content">
            <span className="clean-summary-label">Monitored Nodes</span>
            <strong className="clean-summary-value">{nodes.length}</strong>
            <small className="clean-summary-sub">Active hardware units</small>
          </div>
        </div>
      </section>

      {/* ===================================================
          3. MAIN CONTENT: CONNECTED NODES + RECENT EVENTS PANEL
      =================================================== */}
      <section className="clean-main-split">

        {/* LEFT COLUMN: CONNECTED DETECTION NODES */}
        <div className="clean-nodes-col">
          <div className="clean-section-header">
            <div>
              <span className="clean-section-tag">MONITORING NODES</span>
              <h3 className="clean-section-title">Connected Detection Nodes</h3>
              <p className="clean-section-subtitle">Real-time leak status for all monitoring nodes across the system.</p>
            </div>
            {setActivePage && (
              <button
                type="button"
                className="clean-manage-btn"
                onClick={() => setActivePage("nodes")}
              >
                <Server size={14} />
                <span>Manage Nodes</span>
              </button>
            )}
          </div>

          {nodes.length > 0 ? (
            <div className="clean-nodes-grid">
              {nodes.map((node) => {
                const hasLeak = alerts.some((a) => a.node === node.id && a.state === "active") || node.status === "leak";
                const lastUpdatedText = formatTimeAgo(node.lastSeen);

                return (
                  <div
                    key={node.id}
                    className={`clean-node-card ${hasLeak ? "has-leak" : "normal"}`}
                    onClick={() => setSelectedNodeDetails(node)}
                  >
                    <div className="clean-node-top">
                      <div className={`clean-node-icon ${hasLeak ? "leak" : "normal"}`}>
                        <Cpu size={18} />
                      </div>

                      <span className={`clean-node-badge ${hasLeak ? "leak" : "normal"}`}>
                        <span className="clean-node-dot" />
                        {hasLeak ? "LEAK DETECTED" : "NORMAL"}
                      </span>
                    </div>

                    <div className="clean-node-body">
                      <h4 className="clean-node-title">{node.name || node.id}</h4>
                      <div className="clean-node-loc">
                        <MapPin size={12} />
                        <span>{node.location || "Default Zone"}</span>
                      </div>
                    </div>

                    {/* Location Image preview if present */}
                    {node.locationImage && (
                      <div className="clean-node-img-preview">
                        <img src={node.locationImage} alt={node.name} />
                      </div>
                    )}

                    <div className="clean-node-footer">
                      <div className="clean-node-state-row">
                        <span className="clean-node-state-label">Status:</span>
                        <strong className={hasLeak ? "text-leak" : "text-normal"}>
                          {hasLeak ? "Leak Active" : "Normal"}
                        </strong>
                      </div>
                      <div className="clean-node-time-row" title={node.lastSeen ? new Date(node.lastSeen).toLocaleString() : "Just now"}>
                        <Clock size={11} />
                        <span>{lastUpdatedText}</span>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="clean-empty-nodes">
              <Cpu size={32} />
              <h4>No Monitoring Nodes Connected</h4>
            </div>
          )}
        </div>

        {/* RIGHT COLUMN: RECENT MQTT EVENTS PANEL */}
        <div className="clean-events-col">
          <div className="clean-events-panel">
            <div className="clean-events-header">
              <div>
                <span className="clean-section-tag">LIVE STREAM</span>
                <h3 className="clean-events-title">Recent Events</h3>
              </div>
              <span className="clean-live-chip">
                <span className="clean-pulse-dot" />
                Live Feed
              </span>
            </div>

            <div className="clean-events-list">
              {mqttEvents && mqttEvents.length > 0 ? (
                mqttEvents.slice(0, 8).map((ev) => {
                  const isLeak = ev.status === "leak";
                  return (
                    <div key={ev.id} className={`clean-event-item ${isLeak ? "leak" : "normal"}`}>
                      <div className={`clean-event-icon-circle ${isLeak ? "leak" : "normal"}`}>
                        {isLeak ? <AlertTriangle size={13} /> : <CheckCircle2 size={13} />}
                      </div>
                      <div className="clean-event-content">
                        <div className="clean-event-row1">
                          <strong className="clean-event-nodename">{ev.node}</strong>
                          <span className={`clean-event-status-pill ${isLeak ? "leak" : "normal"}`}>
                            {isLeak ? "LEAK" : "NORMAL"}
                          </span>
                        </div>
                        <div className="clean-event-row2">
                          <span>{isLeak ? "Leak Detected" : "Status Normal"}</span>
                          <span className="clean-event-time">• {ev.time}</span>
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="clean-events-empty">
                  <Radio size={20} />
                  <p>No recent events</p>
                </div>
              )}
            </div>
          </div>
        </div>

      </section>

      {/* Node Details Inspection Modal */}
      {selectedNodeDetails && (
        <NodeDetailsModal
          node={selectedNodeDetails}
          onClose={() => setSelectedNodeDetails(null)}
          onNavigateToNodes={() => {
            setSelectedNodeDetails(null);
            if (setActivePage) setActivePage("nodes");
          }}
        />
      )}

    </div>
  );
}

/* =========================================================
   1. ACOUSTIC STATUS CARD (HERO)
   ========================================================= */

function AcousticStatusCard({
  soundLevel,
  threshold = 70,
  leakDetected = false,
  mqttTopic = "acoustic-leak-detection/sensor-data",
  lastMqttTime,
  peakDb = 88.5,
}) {
  const currentVal = soundLevel !== null ? soundLevel : (leakDetected ? 88.5 : 54.0);
  // Calculate percentage along 30 - 110 dB range
  const pct = Math.min(100, Math.max(0, ((currentVal - 30) / (110 - 30)) * 100));
  const thresholdPct = ((threshold - 30) / (110 - 30)) * 100;

  return (
    <div className={`iot-acoustic-hero ${leakDetected ? "is-leak" : "is-normal"}`}>
      <div className="hero-status-top">
        <div className="hero-status-tag">
          <Activity size={14} />
          <span>LEAK DETECTION STATUS</span>
        </div>
        <div className="hero-topic-chip" title={`MQTT Topic: ${mqttTopic}`}>
          <Radio size={12} />
          <span>{mqttTopic.length > 28 ? `${mqttTopic.substring(0, 26)}...` : mqttTopic}</span>
        </div>
      </div>

      <div className="hero-meter-center">
        <div className="hero-db-block">
          <span className="hero-db-number" style={{ fontSize: "38px", letterSpacing: "-0.5px" }}>
            {leakDetected ? "LEAK" : "NORMAL"}
          </span>
          <span className="hero-db-unit" style={{ fontSize: "14px", fontWeight: 600, color: leakDetected ? "#ef4444" : "#10b981" }}>
            ({currentVal.toFixed(1)} dB)
          </span>
        </div>

        {/* Animated Audio Equalizer Visualizer */}
        <div className="hero-visualizer-bars">
          <span className="vbar bar-1" />
          <span className="vbar bar-2" />
          <span className="vbar bar-3" />
          <span className="vbar bar-4" />
          <span className="vbar bar-5" />
          <span className="vbar bar-6" />
        </div>
      </div>

      <div className="hero-badge-row">
        <div className={`hero-status-pill ${leakDetected ? "leak" : "normal"}`}>
          <span className="hero-status-dot" />
          {leakDetected ? "LEAK ANOMALY DETECTED" : "NOMINAL — NO LEAK"}
        </div>
        <div className="hero-rate-pill">
          <ShieldCheck size={12} />
          <span>Hardware Sensor Stream</span>
        </div>
      </div>

      {/* Threshold Progress Bar Gauge */}
      <div className="hero-gauge-container">
        <div className="hero-gauge-track">
          <div
            className={`hero-gauge-fill ${leakDetected ? "leak" : "normal"}`}
            style={{ width: `${pct}%` }}
          />
          {/* Threshold Flag Line at 70 dB */}
          <div className="hero-threshold-marker" style={{ left: `${thresholdPct}%` }}>
            <span className="marker-line" />
            <span className="marker-label">{threshold} dB Threshold</span>
          </div>
        </div>
        <div className="hero-gauge-scale">
          <span>30 dB</span>
          <span>50 dB</span>
          <span className="scale-threshold">{threshold} dB</span>
          <span>90 dB</span>
          <span>110 dB</span>
        </div>
      </div>

      {/* Micro-telemetry footer */}
      <div className="hero-footer-row">
        <span><Clock size={12} /> Sync: Just now</span>
        <span><Zap size={12} /> Max Today: {peakDb.toFixed(1)} dB</span>
        <span><Wifi size={12} /> Stream: 1.0 Hz</span>
      </div>
    </div>
  );
}

/* =========================================================
   2. LIVE ACOUSTIC TREND CHART (SVG)
   ========================================================= */

function LiveAcousticTrendChart({
  data = [],
  threshold = 70,
  currentDb = 88.5,
  leakDetected = false,
}) {
  const [timeRange, setTimeRange] = useState("30s");
  const [hoveredPoint, setHoveredPoint] = useState(null);

  // SVG Chart geometry
  const width = 740;
  const height = 240;
  const padL = 48;
  const padR = 24;
  const padT = 24;
  const padB = 34;
  const plotW = width - padL - padR;
  const plotH = height - padT - padB;

  const minDb = 30;
  const maxDb = 110;

  const getY = (val) => {
    const clamped = Math.max(minDb, Math.min(maxDb, val));
    return padT + plotH - ((clamped - minDb) / (maxDb - minDb)) * plotH;
  };

  const getX = (idx, total) => {
    if (total <= 1) return padL + plotW / 2;
    return padL + (idx / (total - 1)) * plotW;
  };

  const thresholdY = getY(threshold);

  // Fallback points if data is empty
  const points = data.length > 0 ? data : [
    { time: "10:45:00", db: 54 },
    { time: "10:45:05", db: 58 },
    { time: "10:45:10", db: 62 },
    { time: "10:45:15", db: 72 },
    { time: "10:45:20", db: 88.5 },
  ];

  const dbValues = points.map((p) => p.db);
  const minVal = Math.min(...dbValues);
  const maxVal = Math.max(...dbValues);
  const avgVal = (dbValues.reduce((a, b) => a + b, 0) / dbValues.length).toFixed(1);

  // Compute smooth bezier curve
  const coords = points.map((p, i) => ({
    x: getX(i, points.length),
    y: getY(p.db),
    db: p.db,
    time: p.time,
    isLeak: p.db >= threshold,
  }));

  // Build SVG path
  let pathD = "";
  if (coords.length > 0) {
    pathD = `M ${coords[0].x} ${coords[0].y}`;
    for (let i = 0; i < coords.length - 1; i++) {
      const p0 = coords[i === 0 ? i : i - 1];
      const p1 = coords[i];
      const p2 = coords[i + 1];
      const p3 = coords[i + 2] || p2;

      const cp1x = p1.x + (p2.x - p0.x) / 6;
      const cp1y = p1.y + (p2.y - p0.y) / 6;
      const cp2x = p2.x - (p3.x - p1.x) / 6;
      const cp2y = p2.y - (p3.y - p1.y) / 6;

      pathD += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${p2.x} ${p2.y}`;
    }
  }

  // Area fill under curve
  const areaD = coords.length > 0
    ? `${pathD} L ${coords[coords.length - 1].x} ${padT + plotH} L ${coords[0].x} ${padT + plotH} Z`
    : "";

  const latestCoord = coords[coords.length - 1];

  return (
    <div className="iot-trend-panel">
      {/* Panel Toolbar */}
      <div className="trend-toolbar">
        <div>
          <div className="trend-title-row">
            <span className="trend-live-dot" />
            <h4>Live Acoustic Trend</h4>
            <span className="trend-live-tag">REAL-TIME MQTT</span>
          </div>
          <p className="trend-subtitle">Acoustic waveform with 70 dB leak threshold trigger boundary.</p>
        </div>

        <div className="trend-controls">
          <div className="trend-stat-pills">
            <span className="tstat">Min: <strong>{minVal} dB</strong></span>
            <span className="tstat">Avg: <strong>{avgVal} dB</strong></span>
            <span className="tstat peak">Peak: <strong>{maxVal} dB</strong></span>
          </div>

          <div className="trend-range-selector">
            {["30s", "5m", "15m", "1h"].map((r) => (
              <button
                key={r}
                type="button"
                className={`range-pill ${timeRange === r ? "active" : ""}`}
                onClick={() => setTimeRange(r)}
              >
                {r === "30s" ? "Live (30s)" : r}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* SVG Responsive Chart */}
      <div className="trend-svg-wrap">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="trend-svg"
          preserveAspectRatio="none"
          onMouseLeave={() => setHoveredPoint(null)}
        >
          <defs>
            {/* Area Fill Gradient */}
            <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={leakDetected ? "#ef4444" : "#10b981"} stopOpacity="0.28" />
              <stop offset="60%" stopColor={leakDetected ? "#ef4444" : "#0ea5e9"} stopOpacity="0.08" />
              <stop offset="100%" stopColor="#2563eb" stopOpacity="0.00" />
            </linearGradient>

            {/* Stroke Line Gradient */}
            <linearGradient id="lineGradient" x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor="#0ea5e9" />
              <stop offset="70%" stopColor={leakDetected ? "#ef4444" : "#10b981"} />
              <stop offset="100%" stopColor={leakDetected ? "#dc2626" : "#22c55e"} />
            </linearGradient>

            {/* Danger Zone Pattern */}
            <linearGradient id="dangerFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#ef4444" stopOpacity="0.06" />
              <stop offset="100%" stopColor="#ef4444" stopOpacity="0.01" />
            </linearGradient>
          </defs>

          {/* Danger Zone Shading (above 70 dB) */}
          <rect
            x={padL}
            y={padT}
            width={plotW}
            height={thresholdY - padT}
            fill="url(#dangerFill)"
          />

          {/* Horizontal Gridlines */}
          {[110, 90, 70, 50, 30].map((db) => {
            const y = getY(db);
            const isThresh = db === threshold;
            return (
              <g key={db}>
                <line
                  x1={padL}
                  y1={y}
                  x2={width - padR}
                  y2={y}
                  stroke={isThresh ? "#ef4444" : "#e2e8f0"}
                  strokeWidth={isThresh ? 1.5 : 1}
                  strokeDasharray={isThresh ? "6,4" : "2,4"}
                />
                <text
                  x={padL - 8}
                  y={y + 4}
                  textAnchor="end"
                  fontSize="10"
                  fill={isThresh ? "#ef4444" : "#94a3b8"}
                  fontWeight={isThresh ? "700" : "500"}
                  fontFamily="system-ui, -apple-system, sans-serif"
                >
                  {db} dB
                </text>
              </g>
            );
          })}

          {/* Threshold Flag Label */}
          <g>
            <rect
              x={width - padR - 104}
              y={thresholdY - 11}
              width="104"
              height="20"
              rx="4"
              fill="#ef4444"
            />
            <text
              x={width - padR - 52}
              y={thresholdY + 3}
              textAnchor="middle"
              fontSize="9"
              fontWeight="700"
              fill="#ffffff"
              fontFamily="system-ui, -apple-system, sans-serif"
            >
              70 dB THRESHOLD
            </text>
          </g>

          {/* Area Fill */}
          {areaD && <path d={areaD} fill="url(#areaGradient)" />}

          {/* Main Curve Line */}
          {pathD && (
            <path
              d={pathD}
              fill="none"
              stroke="url(#lineGradient)"
              strokeWidth="2.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          )}

          {/* Data Points */}
          {coords.map((c, i) => (
            <g
              key={i}
              className="chart-point-group"
              onMouseEnter={() => setHoveredPoint(c)}
              style={{ cursor: "pointer" }}
            >
              {/* Invisible large touch target */}
              <circle cx={c.x} cy={c.y} r="14" fill="transparent" />
              {/* Visible dot */}
              <circle
                cx={c.x}
                cy={c.y}
                r={c.isLeak ? "4.5" : "3"}
                fill={c.isLeak ? "#ef4444" : "#0ea5e9"}
                stroke="#ffffff"
                strokeWidth="1.5"
              />
            </g>
          ))}

          {/* Highlighted Latest Pulse Point */}
          {latestCoord && (
            <g>
              <circle
                cx={latestCoord.x}
                cy={latestCoord.y}
                r="11"
                fill={latestCoord.isLeak ? "#ef4444" : "#10b981"}
                opacity="0.3"
                className="live-beacon-pulse"
              />
              <circle
                cx={latestCoord.x}
                cy={latestCoord.y}
                r="5"
                fill={latestCoord.isLeak ? "#dc2626" : "#16a34a"}
                stroke="#ffffff"
                strokeWidth="2"
              />
            </g>
          )}

          {/* Interactive Hover Crosshair & Tooltip */}
          {hoveredPoint && (
            <g>
              <line
                x1={hoveredPoint.x}
                y1={padT}
                x2={hoveredPoint.x}
                y2={padT + plotH}
                stroke="#6366f1"
                strokeWidth="1.5"
                strokeDasharray="3,3"
              />
              <circle
                cx={hoveredPoint.x}
                cy={hoveredPoint.y}
                r="6"
                fill="#6366f1"
                stroke="#ffffff"
                strokeWidth="2"
              />
              {/* Tooltip Card */}
              <g
                transform={`translate(${
                  hoveredPoint.x > width - 120 ? hoveredPoint.x - 110 : hoveredPoint.x + 10
                }, ${Math.max(padT, hoveredPoint.y - 45)})`}
              >
                <rect
                  width="100"
                  height="42"
                  rx="6"
                  fill="#0f172a"
                  opacity="0.92"
                />
                <text x="8" y="16" fill="#94a3b8" fontSize="9" fontWeight="600">
                  {hoveredPoint.time}
                </text>
                <text x="8" y="32" fill={hoveredPoint.isLeak ? "#f87171" : "#38bdf8"} fontSize="12" fontWeight="800">
                  {hoveredPoint.db} dB {hoveredPoint.isLeak ? "(LEAK)" : ""}
                </text>
              </g>
            </g>
          )}

          {/* X-Axis Timestamps */}
          {coords.length > 0 && [
            coords[0],
            coords[Math.floor(coords.length / 3)],
            coords[Math.floor((coords.length * 2) / 3)],
            coords[coords.length - 1],
          ].filter(Boolean).map((c, i) => (
            <text
              key={i}
              x={c.x}
              y={height - 12}
              textAnchor={i === 0 ? "start" : i === 3 ? "end" : "middle"}
              fontSize="10"
              fill="#94a3b8"
              fontWeight="500"
              fontFamily="system-ui, -apple-system, sans-serif"
            >
              {c.time}
            </text>
          ))}
        </svg>
      </div>
    </div>
  );
}

/* =========================================================
   3. ENHANCED SUMMARY CARD (WITH SPARKLINES)
   ========================================================= */

function EnhancedSummaryCard({
  title,
  value,
  subtitle,
  trend,
  trendDir = "up",
  trendLabel,
  icon,
  sparkPoints = [2, 4, 3, 6, 5, 7],
  color = "#2563eb",
  isAlert = false,
}) {
  return (
    <div className={`iot-kpi-card ${isAlert ? "kpi-alert-glow" : ""}`}>
      <div className="kpi-card-header">
        <div className="kpi-icon-container" style={{ color, background: `${color}14` }}>
          {icon}
        </div>
        <div className="kpi-spark-wrap">
          <MiniSparkline points={sparkPoints} color={color} />
        </div>
      </div>

      <div className="kpi-card-body">
        <span className="kpi-title">{title}</span>
        <strong className="kpi-value">{value}</strong>
        <p className="kpi-subtitle">{subtitle}</p>
      </div>

      <div className="kpi-card-footer">
        <span className={`kpi-trend-chip ${trendDir}`}>
          {trendDir === "up" && <TrendingUp size={11} />}
          {trendDir === "down" && <TrendingDown size={11} />}
          {trendDir === "danger" && <AlertTriangle size={11} />}
          {trendDir === "safe" && <CheckCircle2 size={11} />}
          <strong>{trend}</strong>
        </span>
        <span className="kpi-trend-label">{trendLabel}</span>
      </div>
    </div>
  );
}

function MiniSparkline({ points = [1, 3, 2, 5, 4, 6], color = "#2563eb", width = 64, height = 24 }) {
  if (points.length < 2) return null;
  const min = Math.min(...points);
  const max = Math.max(...points) || 1;
  const range = max - min || 1;

  const pts = points.map((val, idx) => {
    const x = (idx / (points.length - 1)) * width;
    const y = height - 2 - ((val - min) / range) * (height - 6);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");

  return (
    <svg width={width} height={height} className="mini-sparkline" viewBox={`0 0 ${width} ${height}`}>
      <polyline
        fill="none"
        stroke={color}
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={pts}
      />
    </svg>
  );
}

/* =========================================================
   4. CONNECTED NODE CARD (RICH TELEMETRY)
   ========================================================= */

function ConnectedNodeCard({
  node,
  hasLeak,
  history = [50, 52, 55, 60, 68, 75, 88.5],
  threshold = 70,
  onViewDetails,
}) {
  const currentSound = node.soundLevel !== undefined && node.soundLevel !== null
    ? parseFloat(node.soundLevel)
    : 88.5;
  const soundPct = Math.min(100, Math.max(0, ((currentSound - 30) / (110 - 30)) * 100));

  return (
    <div className={`iot-node-card ${hasLeak ? "has-leak-anomaly" : ""}`}>
      {/* Node Header */}
      <div className="node-card-topbar">
        <div className="node-id-group">
          <div className="node-avatar">
            <Cpu size={18} />
          </div>
          <div>
            <h4 className="node-heading">{node.name || node.id}</h4>
            <div className="node-loc-pill">
              <MapPin size={10} />
              <span>{node.location || "Default Zone"}</span>
            </div>
          </div>
        </div>

        <span className={`node-live-status ${hasLeak ? "leak" : "normal"}`}>
          <span className="node-pulse-circle" />
          {hasLeak ? "LEAK" : "NORMAL"}
        </span>
      </div>

      {/* Main Metric & Sound Bar */}
      <div className="node-metric-section">
        <div className="node-metric-row">
          <div>
            <span className="node-metric-label">STATUS / LEVEL</span>
            <div className="node-metric-val">
              <strong style={{ color: hasLeak ? "#ef4444" : "#10b981", marginRight: "6px" }}>
                {hasLeak ? "LEAK" : "NORMAL"}
              </strong>
              <small style={{ fontSize: "12px", color: "#64748b" }}>
                ({currentSound.toFixed(1)} dB)
              </small>
            </div>
          </div>

          {/* Mini Sparkline for this Node */}
          <div className="node-spark-container">
            <MiniSparkline
              points={history}
              color={hasLeak ? "#ef4444" : "#10b981"}
              width={90}
              height={30}
            />
          </div>
        </div>

        {/* Level meter bar */}
        <div className="node-level-bar">
          <div
            className={`node-level-fill ${hasLeak ? "leak" : "normal"}`}
            style={{ width: `${soundPct}%` }}
          />
        </div>
      </div>

      {/* Industrial Hardware Telemetry Strip */}
      <div className="node-telemetry-strip">
        <div className="tele-item" title="WiFi Signal Strength">
          <Wifi size={12} />
          <span>-62 dBm</span>
        </div>
        <div className="tele-item" title="Power Source">
          <Zap size={12} />
          <span>24V Line</span>
        </div>
        <div className="tele-item" title="MQTT Heartbeat">
          <Radio size={12} />
          <span>1.0 Hz Live</span>
        </div>
      </div>

      {/* Card Footer */}
      <div className="node-card-footer">
        <span className="node-sync-time">
          <Clock size={11} /> Updated just now
        </span>

        <button
          type="button"
          className="node-details-btn"
          onClick={onViewDetails}
        >
          <Eye size={12} />
          <span>View Details</span>
        </button>
      </div>
    </div>
  );
}

/* =========================================================
   5. ALERT SEVERITY DONUT CHART (SVG)
   ========================================================= */

function AlertSeverityDonutChart({ alerts = [] }) {
  const normalCount = alerts.filter((a) => (a.soundLevel || 0) < 65).length || 14;
  const warningCount = alerts.filter((a) => (a.soundLevel || 0) >= 65 && (a.soundLevel || 0) < 75).length || 6;
  const criticalCount = alerts.filter((a) => (a.soundLevel || 0) >= 75).length || (alerts.length > 0 ? alerts.length : 3);

  const total = normalCount + warningCount + criticalCount;
  const normalPct = (normalCount / total) * 100;
  const warningPct = (warningCount / total) * 100;
  const criticalPct = (criticalCount / total) * 100;

  // Donut geometry (Radius 64, Circumference ~402.12)
  const radius = 64;
  const circumference = 2 * Math.PI * radius;

  const normalStroke = (normalPct / 100) * circumference;
  const warningStroke = (warningPct / 100) * circumference;
  const criticalStroke = (criticalPct / 100) * circumference;

  const normalOffset = 0;
  const warningOffset = -normalStroke;
  const criticalOffset = -(normalStroke + warningStroke);

  return (
    <div className="donut-chart-container">
      <div className="donut-svg-box">
        <svg viewBox="0 0 180 180" className="donut-svg">
          {/* Base track */}
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="transparent"
            stroke="#f1f5f9"
            strokeWidth="20"
          />

          {/* Normal Segment (Green) */}
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="transparent"
            stroke="#10b981"
            strokeWidth="20"
            strokeDasharray={`${normalStroke} ${circumference}`}
            strokeDashoffset={normalOffset}
            strokeLinecap="round"
            transform="rotate(-90 90 90)"
          />

          {/* Warning Segment (Amber) */}
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="transparent"
            stroke="#f59e0b"
            strokeWidth="20"
            strokeDasharray={`${warningStroke} ${circumference}`}
            strokeDashoffset={warningOffset}
            strokeLinecap="round"
            transform="rotate(-90 90 90)"
          />

          {/* Critical Segment (Red) */}
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="transparent"
            stroke="#ef4444"
            strokeWidth="20"
            strokeDasharray={`${criticalStroke} ${circumference}`}
            strokeDashoffset={criticalOffset}
            strokeLinecap="round"
            transform="rotate(-90 90 90)"
          />

          {/* Center text */}
          <text x="90" y="86" textAnchor="middle" className="donut-center-num">
            {total}
          </text>
          <text x="90" y="104" textAnchor="middle" className="donut-center-sub">
            TOTAL EVENTS
          </text>
        </svg>
      </div>

      {/* Industrial Legend */}
      <div className="donut-legend">
        <div className="legend-row">
          <div className="legend-label">
            <span className="dot dot-normal" />
            <span>Normal (&lt;65 dB)</span>
          </div>
          <span className="legend-badge normal">{normalCount} ({normalPct.toFixed(0)}%)</span>
        </div>

        <div className="legend-row">
          <div className="legend-label">
            <span className="dot dot-warning" />
            <span>Warning (65-75 dB)</span>
          </div>
          <span className="legend-badge warning">{warningCount} ({warningPct.toFixed(0)}%)</span>
        </div>

        <div className="legend-row">
          <div className="legend-label">
            <span className="dot dot-critical" />
            <span>Critical (&gt;75 dB)</span>
          </div>
          <span className="legend-badge critical">{criticalCount} ({criticalPct.toFixed(0)}%)</span>
        </div>
      </div>
    </div>
  );
}

/* =========================================================
   6. ALERTS OVER TIME BAR CHART (SVG)
   ========================================================= */

function AlertsOverTimeBarChart({ alerts = [] }) {
  // 8 hourly buckets
  const bars = [
    { label: "00:00", count: 1 },
    { label: "03:00", count: 0 },
    { label: "06:00", count: 2 },
    { label: "09:00", count: 1 },
    { label: "12:00", count: 4 },
    { label: "15:00", count: Math.max(3, alerts.length) },
    { label: "18:00", count: 2 },
    { label: "Live", count: alerts.filter((a) => a.state === "active").length || 1 },
  ];

  const maxVal = Math.max(...bars.map((b) => b.count), 4);
  const chartHeight = 110;

  return (
    <div className="hourly-bar-container">
      <div className="hourly-bars-flex">
        {bars.map((bar, idx) => {
          const barHeight = Math.max(8, (bar.count / maxVal) * chartHeight);
          const isPeak = bar.count === maxVal;
          return (
            <div key={idx} className="hourly-col">
              <span className={`bar-count-label ${isPeak ? "peak" : ""}`}>{bar.count}</span>
              <div className="bar-track">
                <div
                  className={`bar-fill ${isPeak ? "is-peak" : ""}`}
                  style={{ height: `${barHeight}px` }}
                />
              </div>
              <span className="bar-time-label">{bar.label}</span>
            </div>
          );
        })}
      </div>
      <div className="hourly-footer">
        <span><Clock size={11} /> Distribution across 24h operational window</span>
        <span>Peak frequency at 15:00</span>
      </div>
    </div>
  );
}

/* =========================================================
   7. RECENT ACTIVITY PANEL (TIMELINE)
   ========================================================= */

function RecentActivityPanel({ activities = [] }) {
  const events = activities || [];

  if (events.length === 0) {
    return (
      <div style={{ padding: "28px 16px", textAlign: "center", color: "#94a3b8", fontSize: "13px" }}>
        No operational activity recorded yet.
      </div>
    );
  }

  return (
    <div className="iot-activity-timeline">
      {events.slice(0, 5).map((ev) => (
        <div key={ev.id} className="timeline-item">
          <div className={`timeline-node-dot ${ev.type || "system"}`} />
          <div className="timeline-content">
            <div className="timeline-meta">
              <span className="timeline-time">{ev.time}</span>
              <span className="timeline-node-chip">{ev.node}</span>
            </div>
            <p className="timeline-text">{ev.event}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

/* =========================================================
   8. SYSTEM HEALTH CARD
   ========================================================= */

function SystemHealthCard({ connectedNodesCount = 3, lastMqttTime }) {
  return (
    <div className="iot-health-card">
      <div className="health-metric-row">
        <div className="health-metric-label">
          <Radio size={14} className="metric-icon broker" />
          <span>HiveMQ Broker</span>
        </div>
        <span className="health-status-badge online">
          <span className="health-pulse-dot" /> Online (99.98%)
        </span>
      </div>

      <div className="health-metric-row">
        <div className="health-metric-label">
          <Activity size={14} className="metric-icon latency" />
          <span>MQTT Latency</span>
        </div>
        <strong className="health-metric-val">24 ms <small>(&lt;3ms jitter)</small></strong>
      </div>

      <div className="health-metric-row">
        <div className="health-metric-label">
          <Cpu size={14} className="metric-icon nodes" />
          <span>Active Nodes</span>
        </div>
        <strong className="health-metric-val">{connectedNodesCount} Units Active</strong>
      </div>

      <div className="health-metric-row">
        <div className="health-metric-label">
          <RefreshCw size={14} className="metric-icon rate" />
          <span>Sampling Rate</span>
        </div>
        <strong className="health-metric-val">1.0 Hz (1,000 ms)</strong>
      </div>

      <div className="health-metric-row">
        <div className="health-metric-label">
          <Zap size={14} className="metric-icon uptime" />
          <span>System SLA Uptime</span>
        </div>
        <strong className="health-metric-val">99.98% (14d 6h)</strong>
      </div>
    </div>
  );
}

/* =========================================================
   9. NODE DETAILS MODAL
   ========================================================= */

function NodeDetailsModal({ node, onClose, onNavigateToNodes }) {
  if (!node) return null;

  const isLeak = node.status === "leak";

  return (
    <div className="iot-modal-overlay" onClick={onClose}>
      <div className="iot-modal-box" onClick={(e) => e.stopPropagation()}>
        <div className="iot-modal-header">
          <div className="modal-title-group">
            <div className="modal-avatar">
              <Cpu size={22} />
            </div>
            <div>
              <h3>{node.name || node.id}</h3>
              <p>Node ID: <code>{node.id}</code> • Status: <span className={isLeak ? "text-danger" : "text-success"}>{isLeak ? "LEAK DETECTED" : "NORMAL"}</span></p>
            </div>
          </div>
          <button type="button" className="modal-close-btn" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="iot-modal-body">
          {/* Location Photo Preview if available */}
          {node.locationImage && (
            <div className="modal-location-photo">
              <img src={node.locationImage} alt="Node Location" />
              <span className="photo-tag"><MapPin size={12} /> {node.location || "Facility Location"}</span>
            </div>
          )}

          {/* Primary State Card */}
          <div style={{
            background: isLeak ? "#fff5f5" : "#f0fdf4",
            border: isLeak ? "1.5px solid #fecaca" : "1.5px solid #bbf7d0",
            borderRadius: "12px",
            padding: "16px 20px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: "16px",
          }}>
            <div>
              <span style={{ fontSize: "11px", fontWeight: "700", color: "#64748b", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                HARDWARE DETECTION STATUS
              </span>
              <div style={{ fontSize: "20px", fontWeight: "800", color: isLeak ? "#dc2626" : "#16a34a", marginTop: "2px" }}>
                {isLeak ? "LEAK ANOMALY ACTIVE" : "STATUS NORMAL (NO LEAK)"}
              </div>
            </div>
            <span style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "6px",
              padding: "6px 14px",
              borderRadius: "20px",
              fontSize: "12px",
              fontWeight: "700",
              background: isLeak ? "#ef4444" : "#10b981",
              color: "#ffffff",
            }}>
              <span style={{ width: "7px", height: "7px", borderRadius: "50%", background: "#fff" }} />
              {isLeak ? "LEAK" : "NORMAL"}
            </span>
          </div>

          {/* Clean Hardware Specs Grid */}
          <div className="modal-specs-grid">
            <div className="spec-item">
              <span className="spec-label">Device Type</span>
              <strong>Acoustic Leak Detector</strong>
            </div>
            <div className="spec-item">
              <span className="spec-label">Communication</span>
              <strong>MQTT (broker.hivemq.com)</strong>
            </div>
            <div className="spec-item">
              <span className="spec-label">Location Zone</span>
              <strong>{node.location || "Default Zone"}</strong>
            </div>
            <div className="spec-item">
              <span className="spec-label">Last Telemetry</span>
              <strong>{node.lastSeen ? new Date(node.lastSeen).toLocaleTimeString() : "Just now"}</strong>
            </div>
          </div>

          {node.description && (
            <div className="modal-notes" style={{ marginTop: "14px" }}>
              <span className="spec-label">Description / Notes</span>
              <p>{node.description}</p>
            </div>
          )}
        </div>

        <div className="iot-modal-footer">
          <button type="button" className="modal-btn-secondary" onClick={onClose}>
            Close
          </button>
          <button type="button" className="modal-btn-primary" onClick={onNavigateToNodes}>
            <Edit3 size={14} />
            Edit in Node Management
          </button>
        </div>
      </div>
    </div>
  );
}

/* =========================================================
   ALERTS PAGE
   ========================================================= */

function AlertsPage({
  alerts,
  selectedAlert,
  setSelectedAlert,
  resolveAlert,
  deleteAlert,
  loading = false,
}) {
  const [filterState, setFilterState] = useState("all");

  const totalAlerts = alerts.length;
  const activeAlerts = alerts.filter((alert) => alert.state === "active").length;
  const resolvedAlerts = alerts.filter((alert) => alert.state === "resolved").length;

  const filteredAlerts = alerts.filter((alert) => {
    if (filterState === "active") return alert.state === "active";
    if (filterState === "resolved") return alert.state === "resolved";
    return true;
  });

  const formatCreatedAt = (value) => {
    if (!value) return "Just now";
    if (typeof value === "string" && value.startsWith("Today")) return value;
    return new Date(value).toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  return (
    <div className="alerts-page">

      {/* STATISTICS */}
      <div className="alert-stats">
        <div className="alert-stat-card">
          <span>Total Alerts</span>
          <strong>{loading ? "..." : totalAlerts}</strong>
        </div>

        <div className="alert-stat-card active-stat">
          <span>Active Leaks</span>
          <strong>{loading ? "..." : activeAlerts}</strong>
        </div>

        <div className="alert-stat-card resolved-stat">
          <span>Resolved</span>
          <strong>{loading ? "..." : resolvedAlerts}</strong>
        </div>
      </div>

      {/* TABS FILTER */}
      <div className="history-filter-bar" style={{ marginBottom: "20px" }}>
        <div className="filter-group">
          <label>Show Alerts:</label>
          <select
            value={filterState}
            onChange={(e) => setFilterState(e.target.value)}
          >
            <option value="all">All Alerts ({totalAlerts})</option>
            <option value="active">Active Leaks ({activeAlerts})</option>
            <option value="resolved">Resolved ({resolvedAlerts})</option>
          </select>
        </div>
      </div>

      {/* ALERT LIST + DETAILS */}
      <div className={`alerts-content ${selectedAlert ? "has-details" : ""}`}>
        <div className="alerts-list">
          {filteredAlerts.length > 0 ? (
            filteredAlerts.map((alert) => (
              <div
                key={alert.id}
                onClick={() => setSelectedAlert(alert)}
                className={`alert-card ${
                  selectedAlert?.id === alert.id ? "selected" : ""
                } ${
                  alert.state === "active"
                    ? "alert-active"
                    : "alert-resolved"
                }`}
              >
                <div className="alert-icon">
                  {alert.state === "active" ? <AlertTriangle size={18} /> : <ShieldCheck size={18} />}
                </div>

                <div className="alert-info">
                  <strong>
                    {alert.node} — {alert.title}
                  </strong>
                  <span>{alert.description}</span>
                </div>

                <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                  <div
                    className={`status-badge ${
                      alert.state === "active"
                        ? "status-active"
                        : "status-resolved"
                    }`}
                  >
                    ● {alert.state === "active" ? "LEAK DETECTED" : "RESOLVED"}
                  </div>

                  <button
                    type="button"
                    className="delete-alert-btn"
                    title="Delete this alert"
                    onClick={(e) => {
                      e.stopPropagation();
                      if (window.confirm("Are you sure you want to delete this alert?")) {
                        deleteAlert(alert.id);
                      }
                    }}
                  >
                    <Trash2 size={15} />
                  </button>
                </div>
              </div>
            ))
          ) : (
            <div style={{ padding: "40px", textAlign: "center", color: "#64748b" }}>
              {loading ? "Loading alerts..." : "No alerts match the selected filter."}
            </div>
          )}
        </div>

        {/* DETAILS PANEL ONLY APPEARS AFTER CLICKING AN ALERT */}
        {selectedAlert && (
          <aside className="alert-details">
            <div className="details-header">
              <div>
                <p className="small-title">ALERT DETAILS</p>
                <h2>{selectedAlert.node}</h2>
                <p>{selectedAlert.title}</p>
              </div>

              <button
                type="button"
                className="close-details"
                onClick={() => setSelectedAlert(null)}
                aria-label="Close alert details"
              >
                ×
              </button>
            </div>

            <div
              className={`details-status ${
                selectedAlert.state === "active"
                  ? "details-active"
                  : "details-resolved"
              }`}
            >
              <span>
                {selectedAlert.state === "active" ? "⚠" : "✓"}
              </span>
              <div>
                <strong>
                  {selectedAlert.state === "active"
                    ? "Active Alert"
                    : "Alert Resolved"}
                </strong>
                <small>
                  {selectedAlert.state === "active"
                    ? "Requires attention"
                    : "No action required"}
                </small>
              </div>
            </div>

            <div className="details-grid">
              <div className="detail-item">
                <span>Node</span>
                <strong>{selectedAlert.node}</strong>
              </div>

              <div className="detail-item">
                <span>Severity</span>
                <strong>
                  {selectedAlert.severity
                    ? selectedAlert.severity.charAt(0).toUpperCase() + selectedAlert.severity.slice(1)
                    : "Warning"}
                </strong>
              </div>

              <div className="detail-item">
                <span>Status</span>
                <strong>
                  {selectedAlert.state
                    ? selectedAlert.state.charAt(0).toUpperCase() + selectedAlert.state.slice(1)
                    : "Active"}
                </strong>
              </div>

              <div className="detail-item">
                <span>Created</span>
                <strong>{formatCreatedAt(selectedAlert.createdAt)}</strong>
              </div>
            </div>

            <div className="detail-message">
              <strong>Detection Information</strong>
              <p>{selectedAlert.description}</p>
            </div>

            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
              {selectedAlert.state === "active" && (
                <button
                  type="button"
                  className="resolve-alert-button"
                  onClick={() => resolveAlert(selectedAlert.id)}
                >
                  ✓ Resolve Alert
                </button>
              )}

              <button
                type="button"
                className="delete-alert-button"
                onClick={() => {
                  if (window.confirm("Are you sure you want to delete this alert?")) {
                    deleteAlert(selectedAlert.id);
                  }
                }}
              >
                <Trash2 size={15} /> Delete Alert
              </button>
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}

/* =========================================================
   HISTORY PAGE
   ========================================================= */

function HistoryPage({ historyRecords = [], nodes = [] }) {
  const [filterNode, setFilterNode] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");

  const filteredRows = historyRecords.filter((item) => {
    if (filterNode !== "all" && item.node !== filterNode) return false;
    if (filterStatus !== "all" && item.status !== filterStatus) return false;
    return true;
  });

  return (
    <section className="history-section">

      {/* FILTER BAR */}
      <div className="history-filter-bar">
        <div className="filter-group">
          <label htmlFor="history-node-filter">Filter by Node:</label>
          <select
            id="history-node-filter"
            value={filterNode}
            onChange={(e) => setFilterNode(e.target.value)}
          >
            <option value="all">All Nodes</option>
            {nodes.map((node) => (
              <option key={node.id} value={node.id}>
                {node.name}
              </option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label htmlFor="history-status-filter">Filter by Status:</label>
          <select
            id="history-status-filter"
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value)}
          >
            <option value="all">All Statuses</option>
            <option value="leak">Leak Detected</option>
            <option value="normal">Normal</option>
          </select>
        </div>
      </div>

      {/* TABLE PANEL */}
      <div className="panel history-panel">

        <div className="history-row history-header">
          <span>Timestamp</span>
          <span>Node</span>
          <span>Event / Activity</span>
          <span>Status</span>
        </div>

        {filteredRows.length > 0 ? (
          filteredRows.map((row) => {
            const isLeak = row.status === "leak";
            return (
              <div className="history-row" key={row.id}>
                <span className="history-time">{row.time}</span>
                <span className="history-node-badge">
                  <Cpu size={14} />
                  <strong>{row.node}</strong>
                </span>
                <span className="history-event">{row.event}</span>
                <span>
                  <span
                    className={`node-status-badge ${
                      isLeak ? "leak" : "normal"
                    }`}
                  >
                    <span className="node-status-dot"></span>
                    {isLeak ? "LEAK DETECTED" : "NORMAL"}
                  </span>
                </span>
              </div>
            );
          })
        ) : (
          <div className="history-empty">
            <p>No history records match the selected filters.</p>
          </div>
        )}

      </div>

    </section>
  );
}

/* =========================================================
   NODES MANAGEMENT PAGE
   ========================================================= */

function NodesPage({ nodes = [], setNodes, alerts = [], onDeleteNode, onRegisterNode }) {
  const [showForm, setShowForm] = useState(false);
  const [editingNode, setEditingNode] = useState(null);
  const [formData, setFormData] = useState({
    id: "",
    name: "",
    location: "",
    locationImage: "",
    description: "",
  });
  const [formError, setFormError] = useState("");
  const [successMsg, setSuccessMsg] = useState("");
  const [lightboxImage, setLightboxImage] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef(null);

  const API_BASE = import.meta.env.VITE_API_URL || "";

  /* --- Image processing helper --- */
  function processImageFile(file) {
    if (!file) return;
    if (!file.type.startsWith("image/")) {
      setFormError("Please upload a valid image file (JPEG, PNG, WebP, etc.).");
      return;
    }
    if (file.size > 10 * 1024 * 1024) {
      setFormError("Image must be smaller than 10 MB.");
      return;
    }
    setFormError("");

    const reader = new FileReader();
    reader.onload = (e) => {
      const img = new window.Image();
      img.onload = () => {
        // Compress to max 800px width to keep size manageable
        const MAX_W = 800;
        let w = img.width;
        let h = img.height;
        if (w > MAX_W) {
          h = Math.round((h * MAX_W) / w);
          w = MAX_W;
        }
        const canvas = document.createElement("canvas");
        canvas.width = w;
        canvas.height = h;
        const ctx = canvas.getContext("2d");
        ctx.drawImage(img, 0, 0, w, h);
        const compressed = canvas.toDataURL("image/jpeg", 0.75);
        setFormData((prev) => ({ ...prev, locationImage: compressed }));
      };
      img.src = e.target.result;
    };
    reader.readAsDataURL(file);
  }

  function handleDragOver(e) {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }

  function handleDragLeave(e) {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  }

  function handleDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    const file = e.dataTransfer?.files?.[0];
    processImageFile(file);
  }

  function handleFileSelect(e) {
    const file = e.target.files?.[0];
    processImageFile(file);
    // Reset input so re-selecting the same file works
    if (fileInputRef.current) fileInputRef.current.value = "";
  }

  function removeImage() {
    setFormData((prev) => ({ ...prev, locationImage: "" }));
  }

  function resetForm() {
    setFormData({ id: "", name: "", location: "", locationImage: "", description: "" });
    setFormError("");
    setEditingNode(null);
  }

  function openAddForm() {
    resetForm();
    setShowForm(true);
  }

  function openEditForm(node) {
    setFormData({
      id: node.id,
      name: node.name || node.id,
      location: node.location || "",
      locationImage: node.locationImage || "",
      description: node.description || "",
    });
    setEditingNode(node);
    setShowForm(true);
    setFormError("");
  }

  function closeForm() {
    setShowForm(false);
    resetForm();
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setFormError("");
    setSuccessMsg("");

    const trimmedId = formData.id.trim();
    const trimmedName = formData.name.trim();

    if (!trimmedId) {
      setFormError("Node ID is required.");
      return;
    }
    if (!trimmedName) {
      setFormError("Node name is required.");
      return;
    }

    if (!editingNode) {
      // Check for duplicate ID
      const exists = nodes.some((n) => n.id.toLowerCase() === trimmedId.toLowerCase());
      if (exists) {
        setFormError(`A node with ID "${trimmedId}" already exists.`);
        return;
      }
    }

    const nodeData = {
      id: trimmedId,
      name: trimmedName,
      location: formData.location.trim(),
      locationImage: formData.locationImage || "",
      description: formData.description.trim(),
      status: editingNode ? editingNode.status : "normal",
      soundLevel: editingNode ? editingNode.soundLevel : 0,
      lastSeen: editingNode ? editingNode.lastSeen : new Date().toISOString(),
    };

    let backendSaved = false;
    // Try to save to backend
    if (API_BASE) {
      try {
        const method = editingNode ? "PUT" : "POST";
        const url = editingNode
          ? `${API_BASE}/api/nodes/${encodeURIComponent(editingNode.id)}`
          : `${API_BASE}/api/nodes`;
        const resp = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(nodeData),
        });
        if (resp.ok) {
          backendSaved = true;
        } else {
          const err = await resp.json().catch(() => ({}));
          console.warn("Backend save responded with status", resp.status, err);
        }
      } catch (err) {
        console.warn("Backend save network error, saving locally:", err);
      }
    }

    // Always update local state & cache so edits are never lost
    setNodes((current) => {
      let updated;
      if (editingNode) {
        updated = current.map((n) =>
          n.id === editingNode.id ? { ...n, ...nodeData } : n
        );
      } else {
        updated = [...current, nodeData];
      }
      try {
        localStorage.setItem("cached_nodes", JSON.stringify(updated));
      } catch (e) {
        console.warn("Failed to persist to localStorage:", e);
      }
      return updated;
    });

    if (onRegisterNode) {
      onRegisterNode(trimmedId);
    }

    const actionText = editingNode ? "updated" : "registered";
    const statusText = API_BASE && !backendSaved ? `(saved locally)` : `successfully!`;
    setSuccessMsg(`Node "${trimmedName}" ${actionText} ${statusText}`);
    closeForm();

    setTimeout(() => setSuccessMsg(""), 4000);
  }

  async function handleDelete(nodeId) {
    if (!window.confirm(`Are you sure you want to remove node "${nodeId}"? This cannot be undone.`)) {
      return;
    }

    if (API_BASE) {
      try {
        await fetch(`${API_BASE}/api/nodes/${encodeURIComponent(nodeId)}`, { method: "DELETE" });
      } catch (err) {
        console.warn("Backend delete failed:", err);
      }
    }

    setNodes((current) => {
      const updated = current.filter((n) => n.id !== nodeId);
      try {
        localStorage.setItem("cached_nodes", JSON.stringify(updated));
      } catch {}
      return updated;
    });

    if (onDeleteNode) {
      onDeleteNode(nodeId);
    }

    setSuccessMsg(`Node "${nodeId}" removed.`);
    setTimeout(() => setSuccessMsg(""), 4000);
  }

  return (
    <div className="nodes-page">
      {/* Success Banner */}
      {successMsg && (
        <div className="nodes-success-banner">
          <ShieldCheck size={18} />
          <span>{successMsg}</span>
        </div>
      )}

      {/* Action Bar */}
      <div className="nodes-action-bar">
        <div className="nodes-stats-row">
          <div className="nodes-stat-chip">
            <Server size={16} />
            <span>{nodes.length} Total Node{nodes.length !== 1 ? "s" : ""}</span>
          </div>
          <div className="nodes-stat-chip online">
            <span className="nodes-stat-dot" />
            <span>{nodes.filter((n) => !alerts.some((a) => a.node === n.id && a.state === "active")).length} Normal</span>
          </div>
          <div className="nodes-stat-chip leak">
            <span className="nodes-stat-dot" />
            <span>{nodes.filter((n) => alerts.some((a) => a.node === n.id && a.state === "active")).length} With Leaks</span>
          </div>
        </div>

        <button
          type="button"
          className="nodes-add-btn"
          onClick={openAddForm}
        >
          <Plus size={18} />
          Register Node
        </button>
      </div>

      {/* Registration / Edit Form */}
      {showForm && (
        <div className="nodes-form-overlay" onClick={closeForm}>
          <div className="nodes-form-modal" onClick={(e) => e.stopPropagation()}>
            <div className="nodes-form-header">
              <div>
                <h3>{editingNode ? "Edit Node" : "Register New Node"}</h3>
                <p>{editingNode ? "Update the node details below." : "Add a new monitoring node to the system."}</p>
              </div>
              <button type="button" className="nodes-form-close" onClick={closeForm}>×</button>
            </div>

            {formError && (
              <div className="nodes-form-error">
                <AlertTriangle size={16} />
                <span>{formError}</span>
              </div>
            )}

            <form onSubmit={handleSubmit} className="nodes-form-body">
              <div className="nodes-form-group">
                <label htmlFor="node-id">Node ID *</label>
                <input
                  id="node-id"
                  type="text"
                  placeholder="e.g. Node 04"
                  value={formData.id}
                  onChange={(e) => setFormData({ ...formData, id: e.target.value })}
                  disabled={!!editingNode}
                  autoFocus={!editingNode}
                />
                {!editingNode && (
                  <span className="nodes-form-hint">Unique identifier. Must match ESP32 node ID.</span>
                )}
              </div>

              <div className="nodes-form-group">
                <label htmlFor="node-name">Display Name *</label>
                <input
                  id="node-name"
                  type="text"
                  placeholder="e.g. Compressor Room A"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  autoFocus={!!editingNode}
                />
              </div>

              <div className="nodes-form-group">
                <label htmlFor="node-location">
                  <MapPin size={14} style={{ marginRight: "6px", verticalAlign: "middle" }} />
                  Location
                </label>
                <input
                  id="node-location"
                  type="text"
                  placeholder="e.g. Building 2, Floor 3"
                  value={formData.location}
                  onChange={(e) => setFormData({ ...formData, location: e.target.value })}
                />
              </div>

              {/* Location Image Upload */}
              <div className="nodes-form-group">
                <label>
                  <ImageIcon size={14} style={{ marginRight: "6px", verticalAlign: "middle" }} />
                  Location Photo
                </label>

                {formData.locationImage ? (
                  <div className="location-image-preview">
                    <img
                      src={formData.locationImage}
                      alt="Location preview"
                      onClick={() => setLightboxImage(formData.locationImage)}
                    />
                    <div className="location-image-actions">
                      <button
                        type="button"
                        className="location-image-view"
                        onClick={() => setLightboxImage(formData.locationImage)}
                      >
                        <Eye size={14} /> View
                      </button>
                      <button
                        type="button"
                        className="location-image-remove"
                        onClick={removeImage}
                      >
                        <X size={14} /> Remove
                      </button>
                    </div>
                  </div>
                ) : (
                  <div
                    className={`location-upload-zone ${isDragging ? "dragging" : ""}`}
                    onDragOver={handleDragOver}
                    onDragLeave={handleDragLeave}
                    onDrop={handleDrop}
                    onClick={() => fileInputRef.current?.click()}
                  >
                    <Upload size={28} className="upload-icon" />
                    <p className="upload-primary">
                      <strong>Click to upload</strong> or drag and drop
                    </p>
                    <p className="upload-hint">JPEG, PNG or WebP (max 10 MB)</p>
                    <input
                      ref={fileInputRef}
                      type="file"
                      accept="image/*"
                      className="upload-hidden-input"
                      onChange={handleFileSelect}
                    />
                  </div>
                )}
              </div>

              <div className="nodes-form-group">
                <label htmlFor="node-desc">Description</label>
                <textarea
                  id="node-desc"
                  rows={3}
                  placeholder="Optional notes about this node..."
                  value={formData.description}
                  onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                />
              </div>

              <div className="nodes-form-actions">
                <button type="button" className="nodes-form-cancel" onClick={closeForm}>
                  Cancel
                </button>
                <button type="submit" className="nodes-form-submit">
                  {editingNode ? "Update Node" : "Register Node"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Nodes List */}
      {nodes.length > 0 ? (
        <div className="nodes-table-container">
          <div className="nodes-table">
            <div className="nodes-table-header">
              <span>Node</span>
              <span>Status</span>
              <span>Sound Level</span>
              <span>Location</span>
              <span>Last Seen</span>
              <span>Actions</span>
            </div>

            {nodes.map((node) => {
              const hasActiveLeak = alerts.some(
                (a) => a.node === node.id && a.state === "active"
              );
              const lastSeenStr = node.lastSeen
                ? new Date(node.lastSeen).toLocaleString([], {
                    month: "short",
                    day: "numeric",
                    hour: "2-digit",
                    minute: "2-digit",
                  })
                : "Never";

              return (
                <div
                  key={node.id}
                  className={`nodes-table-row ${hasActiveLeak ? "row-leak" : ""}`}
                >
                  <span className="nodes-table-name">
                    <div className="nodes-table-icon-box">
                      <Cpu size={18} />
                    </div>
                    <div>
                      <strong>{node.name || node.id}</strong>
                      <small>{node.id}</small>
                    </div>
                  </span>

                  <span>
                    <span
                      className={`node-status-badge ${hasActiveLeak ? "leak" : "normal"}`}
                    >
                      <span className="node-status-dot" />
                      {hasActiveLeak ? "LEAK" : "NORMAL"}
                    </span>
                  </span>

                  <span className="nodes-table-db">
                    {node.soundLevel !== undefined && node.soundLevel !== null
                      ? `${node.soundLevel} dB`
                      : "—"}
                  </span>

                  <span className="nodes-table-location">
                    {node.locationImage ? (
                      <span className="location-with-image">
                        <img
                          src={node.locationImage}
                          alt="Location"
                          className="location-thumb"
                          onClick={(e) => {
                            e.stopPropagation();
                            setLightboxImage(node.locationImage);
                          }}
                        />
                        <span>{node.location || ""}</span>
                      </span>
                    ) : (
                      node.location || "—"
                    )}
                  </span>

                  <span className="nodes-table-lastseen">{lastSeenStr}</span>

                  <span className="nodes-table-actions">
                    <button
                      type="button"
                      className="nodes-edit-btn"
                      title="Edit node"
                      onClick={() => openEditForm(node)}
                    >
                      <Edit3 size={15} />
                    </button>
                    <button
                      type="button"
                      className="nodes-delete-btn"
                      title="Remove node"
                      onClick={() => handleDelete(node.id)}
                    >
                      <Trash2 size={15} />
                    </button>
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="nodes-empty-state">
          <div className="nodes-empty-icon">
            <Server size={42} />
          </div>
          <h4>No Monitoring Nodes Registered</h4>
          <p>
            Register your first node to start monitoring. Nodes will also appear
            automatically when ESP32 units begin streaming data via MQTT.
          </p>
          <button
            type="button"
            className="nodes-add-btn"
            onClick={openAddForm}
          >
            <Plus size={18} />
            Register Your First Node
          </button>
        </div>
      )}
      {/* Lightbox for viewing full-size location images */}
      <ImageLightbox src={lightboxImage} onClose={() => setLightboxImage(null)} />
    </div>
  );
}

/* =========================================================
   LOCATION IMAGE LIGHTBOX
   ========================================================= */

function ImageLightbox({ src, onClose }) {
  if (!src) return null;

  return (
    <div className="lightbox-overlay" onClick={onClose}>
      <div className="lightbox-content" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="lightbox-close" onClick={onClose}>
          <X size={20} />
        </button>
        <img src={src} alt="Location" />
      </div>
    </div>
  );
}

export default App;