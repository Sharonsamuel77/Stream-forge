import { useEffect, useState } from "react";
import { API } from "../api";

function MetricCards() {
  const [summary, setSummary] = useState({});
  const [metrics, setMetrics] = useState({});

  useEffect(() => {
    const loadSummary = async () => {
      try {
        const response = await API.get("/summary");
        setSummary(response.data);
      } catch (error) {
        console.error("Failed to load summary:", error);
      }
    };

    const loadMetrics = async () => {
      try {
        const response = await API.get("/metrics");
        setMetrics(response.data);
      } catch (error) {
        console.error("Failed to load metrics:", error);
      }
    };

    loadSummary();
    loadMetrics();

    const timer = setInterval(() => {
      loadSummary();
      loadMetrics();
    }, 3000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="metrics-grid">

      <div className="metric-card">
        <h3>Total Trucks</h3>
        <h2>{summary.total_trucks || 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Active Workers</h3>
        <h2>{summary.active_workers || 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Total Readings</h3>
        <h2>{summary.total_readings || 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Avg Temperature</h3>
        <h2>
          {summary.avg_temperature || 0}°C
        </h2>
      </div>

      <div className="metric-card">
        <h3>Throughput</h3>
        <h2>
          {metrics.throughput || 0} events/sec
        </h2>
      </div>

      <div className="metric-card">
        <h3>Total Lag</h3>
        <h2>
          {metrics.total_lag || 0}
        </h2>
      </div>

      <div className="metric-card">
        <h3>Partitions</h3>
        <h2>
          {metrics.partitions || 0}
        </h2>
      </div>

      <div className="metric-card">
        <h3>Active Alerts</h3>
        <h2>{summary.active_alerts || 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Status</h3>
        <h2
          style={{
            color:
              metrics.total_lag === 0
                ? "#00ff88"
                : "#ffcc00",
          }}
        >
          {metrics.total_lag === 0
            ? "HEALTHY"
            : "CATCHING UP"}
        </h2>
      </div>

    </div>
  );
}

export default MetricCards;