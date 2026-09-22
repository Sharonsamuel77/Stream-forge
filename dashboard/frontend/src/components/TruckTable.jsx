import { useEffect, useState } from "react";
import { API } from "../api";

export default function TruckTable() {
  const [trucks, setTrucks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    const loadData = async () => {
      try {
        const response = await API.get("/state");

        const truckList = response.data?.trucks
          ? Object.entries(response.data.trucks).map(([truck_id, truck]) => ({
              truck_id,
              readings: Number(truck.readings || 0),
              avg_temperature: Number(truck.avg_temperature || 0),
            }))
          : [];

        if (mounted) {
          setTrucks(truckList);
        }
      } catch (error) {
        console.error("Failed to load truck data:", error);

        if (mounted) {
          setTrucks([]);
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    };

    loadData();

    const timer = setInterval(loadData, 3000);

    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, []);

  return (
    <div className="truck-section">
      <h2>Truck Telemetry</h2>

      {loading ? (
        <p>Loading...</p>
      ) : trucks.length === 0 ? (
        <p>No truck state available</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Truck ID</th>
              <th>Readings</th>
              <th>Average Temperature</th>
              <th>Status</th>
            </tr>
          </thead>

          <tbody>
            {trucks.map((truck) => {
              const temperature = truck.avg_temperature;

              let status = "NORMAL";
              let statusColor = "#22c55e";

              if (temperature >= 40) {
                status = "CRITICAL";
                statusColor = "#ef4444";
              } else if (temperature >= 35) {
                status = "HIGH";
                statusColor = "#f97316";
              }

              return (
                <tr key={truck.truck_id}>
                  <td>Truck {truck.truck_id}</td>
                  <td>{truck.readings.toLocaleString()}</td>
                  <td>{temperature.toFixed(2)} °C</td>
                  <td
                    style={{
                      color: statusColor,
                      fontWeight: "bold",
                    }}
                  >
                    {status}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}


