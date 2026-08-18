import { useEffect, useState } from "react";
import { API } from "../api";

function MetricCards() {

  const [summary, setSummary] = useState({});

  useEffect(() => {

    const loadData = async () => {
      const response = await API.get("/summary");
      setSummary(response.data);
    };

    loadData();

    const timer = setInterval(loadData, 3000);

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
        <h2>{summary.avg_temperature || 0}°C</h2>
      </div>

      <div className="metric-card">
        <h3>Active Alerts</h3>
        <h2>{summary.active_alerts || 0}</h2>
      </div>

    </div>
  );
}

export default MetricCards;