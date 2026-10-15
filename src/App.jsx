import { useState, useEffect } from "react";
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
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  const [soundLevel, setSoundLevel] = useState(null);
  const [threshold, setThreshold] = useState(70);
  const [leakDetected, setLeakDetected] = useState(false);
  const [backendStatus, setBackendStatus] = useState("connecting");
  const [historyRecords, setHistoryRecords] = useState([]);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [loading, setLoading] = useState(true);

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
      id: "history",
      label: "History",
      icon: History,
    },
  ];

  /* =======================================================
     FETCH DATABASE DATA & CONNECT WEBSOCKET
     ======================================================= */
  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_URL || "";

    const fetchDatabaseData = async () => {
      try {
        const [sensorRes, nodesRes, alertsRes, histRes] = await Promise.all([
          fetch(`${API_BASE}/api/sensor/latest`).then((r) => r.json()),
          fetch(`${API_BASE}/api/nodes`).then((r) => r.json()),
          fetch(`${API_BASE}/api/alerts`).then((r) => r.json()),
          fetch(`${API_BASE}/api/history`).then((r) => r.json()),
        ]);

        if (sensorRes?.sensor) {
          setSoundLevel(sensorRes.sensor.soundLevel);
          setThreshold(sensorRes.sensor.threshold);
          setLeakDetected(sensorRes.sensor.leakDetected);
        }
        if (Array.isArray(nodesRes) && nodesRes.length > 0) {
          setNodes(nodesRes);
          try { localStorage.setItem("cached_nodes", JSON.stringify(nodesRes)); } catch {}
        }
        if (Array.isArray(alertsRes)) {
          setAlerts(alertsRes);
          try { localStorage.setItem("cached_alerts", JSON.stringify(alertsRes)); } catch {}
        }
        if (Array.isArray(histRes)) {
          setHistoryRecords(histRes);
        }
        setBackendStatus("connected");
        setLoading(false);
      } catch (err) {
        console.warn("Could not reach backend API:", err);
        setBackendStatus("offline");
        setLoading(false);
      }
    };

    fetchDatabaseData();

    // Setup Socket.io real-time streaming
    const socket = io(API_BASE || undefined, {
      transports: ["websocket", "polling"],
      reconnectionAttempts: 10,
    });

    socket.on("connect", () => {
      console.log("⚡ [WebSocket] Connected to Real-Time Leak Detection Server");
      setBackendStatus("connected");
    });

    socket.on("disconnect", () => {
      console.log("⚠️ [WebSocket] Disconnected from server");
      setBackendStatus("connecting");
    });

    socket.on("sensor:update", (data) => {
      if (data.soundLevel !== undefined) setSoundLevel(data.soundLevel);
      if (data.threshold !== undefined) setThreshold(data.threshold);
      if (data.leakDetected !== undefined) setLeakDetected(data.leakDetected);
    });

    socket.on("alert:new", (newAlert) => {
      setAlerts((currentAlerts) => [
        newAlert,
        ...currentAlerts.filter((a) => a.id !== newAlert.id),
      ]);
    });

    socket.on("alert:resolved", (resolvedAlert) => {
      setAlerts((currentAlerts) =>
        currentAlerts.map((alert) =>
          alert.id === resolvedAlert.id
            ? { ...alert, state: "resolved", resolvedAt: resolvedAlert.resolvedAt }
            : alert
        )
      );
    });

    socket.on("node:status", (updatedNode) => {
      setNodes((currentNodes) =>
        currentNodes.map((n) =>
          n.id === updatedNode.id ? { ...n, ...updatedNode } : n
        )
      );
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  /* =======================================================
     RESOLVE ALERT (PERSISTS TO RAILWAY POSTGRESQL)
     ======================================================= */
  async function resolveAlert(id) {
    const API_BASE = import.meta.env.VITE_API_URL || "";

    // Optimistic UI update
    setAlerts((currentAlerts) =>
      currentAlerts.map((alert) =>
        alert.id === id ? { ...alert, state: "resolved" } : alert
      )
    );

    if (selectedAlert?.id === id) {
      setSelectedAlert((prev) => (prev ? { ...prev, state: "resolved" } : null));
    }

    try {
      const response = await fetch(`${API_BASE}/api/alerts/${id}/resolve`, {
        method: "PATCH",
      });
      if (response.ok) {
        // Refresh history from DB to reflect resolution record
        const histRes = await fetch(`${API_BASE}/api/history`).then((r) => r.json());
        if (Array.isArray(histRes)) {
          setHistoryRecords(histRes);
        }
      }
    } catch (err) {
      console.error("Failed to resolve alert on backend:", err);
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

        {/* BACKEND CONNECTION BADGE */}
        <div style={{ marginTop: "auto", padding: "16px 20px" }}>
          <div
            style={{
              padding: "10px 14px",
              borderRadius: "10px",
              background: backendStatus === "connected" ? "#ecfdf5" : "#fffbeb",
              border: backendStatus === "connected" ? "1px solid #a7f3d0" : "1px solid #fde68a",
              display: "flex",
              alignItems: "center",
              gap: "10px",
            }}
          >
            <span
              style={{
                width: "9px",
                height: "9px",
                borderRadius: "50%",
                backgroundColor: backendStatus === "connected" ? "#10b981" : "#f59e0b",
                boxShadow: backendStatus === "connected" ? "0 0 8px #10b981" : "none",
              }}
            />
            <div style={{ lineHeight: 1.2 }}>
              <div style={{ fontSize: "12px", fontWeight: "700", color: backendStatus === "connected" ? "#065f46" : "#92400e" }}>
                {backendStatus === "connected" ? "Backend Online" : "Connecting..."}
              </div>
              <div style={{ fontSize: "11px", color: backendStatus === "connected" ? "#047857" : "#b45309" }}>
                {backendStatus === "connected" ? "Railway & Sockets Active" : "Reconnecting to Server"}
              </div>
            </div>
          </div>
        </div>

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
                  : "Real-time monitoring. Waiting for connected nodes."
              }
              backendStatus={backendStatus}
            />

            <Dashboard
              alerts={alerts}
              nodes={nodes}
              loading={loading}
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
              loading={loading}
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

      <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "8px",
            padding: "6px 14px",
            borderRadius: "20px",
            fontSize: "12px",
            fontWeight: "600",
            background: backendStatus === "connected" ? "rgba(46, 204, 113, 0.12)" : "rgba(245, 158, 11, 0.12)",
            color: backendStatus === "connected" ? "#27ae60" : "#d97706",
            border: backendStatus === "connected" ? "1px solid rgba(46, 204, 113, 0.3)" : "1px solid rgba(245, 158, 11, 0.3)",
          }}
        >
          <span
            style={{
              width: "8px",
              height: "8px",
              borderRadius: "50%",
              backgroundColor: backendStatus === "connected" ? "#2ecc71" : "#f59e0b",
              boxShadow: backendStatus === "connected" ? "0 0 8px #2ecc71" : "none",
            }}
          />
          {backendStatus === "connected" ? "PostgreSQL & Live Socket" : "Offline / Reconnecting"}
        </span>
      </div>

    </header>
  );
}

/* =========================================================
   DASHBOARD COMPONENT
   ========================================================= */

function Dashboard({
  alerts,
  nodes = [],
}) {
  const totalAlerts = alerts.length;
  const activeAlerts = alerts.filter((alert) => alert.state === "active").length;
  const resolvedAlerts = alerts.filter((alert) => alert.state === "resolved").length;

  return (
    <div className="dashboard">

      {/* =================================================
          SUMMARY STATISTICS
      ================================================= */}

      <section className="summary-grid">

        <SummaryCard
          icon={<Bell size={21} />}
          title="Total Alerts"
          value={totalAlerts}
          text="Recorded events"
        />

        <SummaryCard
          icon={<AlertTriangle size={21} />}
          title="Active Alerts"
          value={activeAlerts}
          text="Require attention"
          type="active"
        />

        <SummaryCard
          icon={<ShieldCheck size={21} />}
          title="Resolved Alerts"
          value={resolvedAlerts}
          text="Issues cleared"
          type="resolved"
        />

      </section>

      {/* =================================================
          CONNECTED NODES GRID (FROM DATABASE)
      ================================================= */}

      <section className="nodes-section">

        <div className="nodes-section-header">
          <p className="small-title">MONITORING NODES</p>
          <h3>Connected Detection Nodes</h3>
          <p>Real-time leak status for all monitoring nodes across the system.</p>
        </div>

        {nodes.length > 0 ? (
          <div className="nodes-grid">
            {nodes.map((node) => {
              const hasLeak =
                alerts.some((a) => a.node === node.id && a.state === "active") ||
                node.status === "leak";

              return (
                <div
                  key={node.id}
                  className={`node-card ${hasLeak ? "has-leak" : ""}`}
                >
                  <div className="node-card-top">
                    <div className="node-icon-box">
                      <Cpu size={20} />
                    </div>

                    <span
                      className={`node-status-badge ${
                        hasLeak ? "leak" : "normal"
                      }`}
                    >
                      <span className="node-status-dot"></span>
                      {hasLeak ? "LEAK DETECTED" : "NORMAL"}
                    </span>
                  </div>

                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                    <h4 className="node-title">{node.name}</h4>
                    {node.soundLevel !== undefined && (
                      <span style={{ fontSize: "13px", color: hasLeak ? "#ef4444" : "#64748b", fontWeight: "600" }}>
                        {node.soundLevel} dB
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div style={{
            background: "#ffffff",
            borderRadius: "14px",
            border: "1px dashed #cbd5e1",
            padding: "48px 24px",
            textAlign: "center",
            color: "#64748b",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: "12px",
          }}>
            <Cpu size={36} color="#94a3b8" />
            <h4 style={{ margin: 0, color: "#1e293b", fontSize: "16px" }}>No Monitoring Nodes Active Yet</h4>
            <p style={{ margin: 0, fontSize: "14px", maxWidth: "440px" }}>
              Nodes will appear here automatically in real time as soon as ESP32 detection units begin streaming acoustic readings.
            </p>
          </div>
        )}

      </section>

    </div>
  );
}

/* =========================================================
   SUMMARY CARD
   ========================================================= */

function SummaryCard({
  icon,
  title,
  value,
  text,
  type = "",
}) {
  return (
    <div className={`summary-card ${type}`}>
      <div className="summary-icon">
        {icon}
      </div>

      <div className="summary-content">
        <span>{title}</span>
        <strong>{value}</strong>
        <small>{text}</small>
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

                <div
                  className={`status-badge ${
                    alert.state === "active"
                      ? "status-active"
                      : "status-resolved"
                  }`}
                >
                  ● {alert.state === "active" ? "LEAK DETECTED" : "RESOLVED"}
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

            {selectedAlert.state === "active" && (
              <button
                type="button"
                className="resolve-alert-button"
                onClick={() => resolveAlert(selectedAlert.id)}
              >
                ✓ Resolve Alert
              </button>
            )}
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

export default App;