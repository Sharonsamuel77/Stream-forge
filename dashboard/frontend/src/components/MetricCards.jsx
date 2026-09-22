import { useEffect, useState } from "react";
import { API } from "../api";

function MetricCards() {
  const [summary, setSummary] = useState({});
  const [metrics, setMetrics] = useState({
    throughput: 0,
    total_lag: 0,
    partitions: 0,
  });

  useEffect(() => {
    let mounted = true;

    const loadData = async () => {
      try {
        const [summaryResponse, metricsResponse] =
          await Promise.all([
            API.get("/summary"),
            API.get("/metrics"),
          ]);

        if (!mounted) return;

        setSummary(summaryResponse.data || {});

        const data = metricsResponse.data || {};

        setMetrics({
          throughput: Number(data.throughput ?? 0),
          total_lag: Number(data.total_lag ?? 0),
          partitions: Number(
            data.partitions ?? data.partition_count ?? 0
          ),
        });
      } catch (error) {
        console.error("Failed to load dashboard metrics:", error);
      }
    };

    loadData();

    const timer = setInterval(loadData, 2000);

    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, []);

  const healthy = metrics.total_lag === 0;

  return (
    <div className="metrics-grid">

      <div className="metric-card">
        <h3>Total Trucks</h3>
        <h2>{summary.total_trucks ?? 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Active Workers</h3>
        <h2>{summary.active_workers ?? 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Total Readings</h3>
        <h2>{summary.total_readings ?? 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Avg Temperature</h3>
        <h2>
          {Number(summary.avg_temperature ?? 0).toFixed(2)}°C
        </h2>
      </div>

      <div className="metric-card">
        <h3>Throughput</h3>
        <h2>
          {Number(metrics.throughput).toLocaleString(undefined, {
            maximumFractionDigits: 2,
          })} events/sec
        </h2>
      </div>

      <div className="metric-card">
        <h3>Total Lag</h3>
        <h2>{metrics.total_lag.toLocaleString()}</h2>
      </div>

      <div className="metric-card">
        <h3>Partitions</h3>
        <h2>{metrics.partitions}</h2>
      </div>

      <div className="metric-card">
        <h3>Active Alerts</h3>
        <h2>{summary.active_alerts ?? 0}</h2>
      </div>

      <div className="metric-card">
        <h3>Status</h3>
        <h2
          style={{
            color: healthy ? "#00ff88" : "#ffcc00",
          }}
        >
          {healthy ? "HEALTHY" : "CATCHING UP"}
        </h2>
      </div>

    </div>
  );
}

export default MetricCards;

