import { useEffect, useState } from "react";
import { API } from "../api";

export default function AlertsPanel() {
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    const loadAlerts = async () => {
      try {
        const res = await API.get("/alerts");

        const data = res.data || {};

        setAlerts(
          Array.isArray(data.alerts)
            ? data.alerts
            : []
        );
      } catch (err) {
        console.error("Alerts:", err);
      }
    };

    loadAlerts();

    const timer = setInterval(loadAlerts, 3000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="alerts-section">
      <h2>System Alerts</h2>

      {alerts.length === 0 ? (
        <p>No active alerts</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Truck</th>
              <th>Temperature</th>
              <th>Severity</th>
            </tr>
          </thead>

          <tbody>
            {alerts.map((alert, index) => (
              <tr key={`${alert.truck_id}-${index}`}>
                <td>{alert.truck_id}</td>

                <td>
                  {Number(alert.temperature).toFixed(2)} °C
                </td>

                <td
                  style={{
                    color:
                      alert.severity === "HIGH"
                        ? "red"
                        : alert.severity === "CRITICAL"
                        ? "red"
                        : "orange",
                    fontWeight: "bold",
                  }}
                >
                  {alert.severity}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
