import { useEffect, useState } from "react";
import { API } from "../api";

export default function AlertsPanel() {

  const [alerts, setAlerts] = useState([]);

  useEffect(() => {

    const loadAlerts = async () => {
      try {
        const res = await API.get("/alerts");
        setAlerts(res.data.alerts);
      } catch (err) {
        console.error(err);
      }
    };

    loadAlerts();

    const timer = setInterval(loadAlerts, 3000);

    return () => clearInterval(timer);

  }, []);

  return (
    <div className="alerts-section">

      <h2>Temperature Alerts</h2>

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

              <tr key={index}>

                <td>{alert.truck_id}</td>

                <td>{alert.temperature}°C</td>

                <td
                  style={{
                    color:
                      alert.severity === "CRITICAL"
                        ? "red"
                        : alert.severity === "High"
                        ? "orange"
                        : "yellow",
                    fontWeight: "bold"
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