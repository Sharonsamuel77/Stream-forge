import { useEffect, useState } from "react";
import { API } from "../api";

export default function AlertsPanel() {
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    const loadAlerts = async () => {
      try {
        const res = await API.get("/summary");

        const summary = res.data;

        if (summary.active_alerts > 0) {
          setAlerts([
            {
              truck_id: "Kafka",
              temperature: "-",
              severity: "HIGH",
              message: `${summary.active_alerts} partition(s) have consumer lag`,
            },
          ]);
        } else {
          setAlerts([]);
        }
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
              <th>Source</th>
              <th>Condition</th>
              <th>Severity</th>
            </tr>
          </thead>

          <tbody>
            {alerts.map((alert, index) => (
              <tr key={index}>
                <td>{alert.truck_id}</td>
                <td>{alert.message}</td>
                <td
                  style={{
                    color:
                      alert.severity === "CRITICAL"
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