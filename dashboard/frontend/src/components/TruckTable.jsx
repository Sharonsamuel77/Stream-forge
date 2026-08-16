import { useEffect, useState } from "react";
import { API } from "../api";

export default function TruckTable() {
  const [trucks, setTrucks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const response = await API.get("/state");
        setTrucks(response.data);
      } catch (err) {
        console.error("Failed to load truck data:", err);
      } finally {
        setLoading(false);
      }
    };

    loadData();

    const timer = setInterval(loadData, 3000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="truck-section">
      <h2>Truck Telemetry</h2>

      {loading ? (
        <p>Loading...</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Truck</th>
              <th>Avg Temp</th>
              <th>Readings</th>
            </tr>
          </thead>

          <tbody>
            {trucks.map((truck) => (
              <tr key={truck.truck_id}>
                <td>{truck.truck_id}</td>

                <td
                  style={{
                    color:
                      truck.avg_temperature > 35
                        ? "#ff6b6b"
                        : "#00d4ff",
                    fontWeight: "bold",
                  }}
                >
                  {truck.avg_temperature}°C
                </td>

                <td>{truck.readings}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}