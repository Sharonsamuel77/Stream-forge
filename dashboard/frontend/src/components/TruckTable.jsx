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

        console.log("State:", response.data);

        /*
         * Backend returns:
         *
         * {
         *   trucks: {
         *     "1001": {
         *       readings: 200,
         *       avg_temperature: 32.5
         *     },
         *     ...
         *   },
         *   total_trucks: 10,
         *   total_readings: 2260,
         *   avg_temperature: 32.69
         * }
         */

        const truckState = response.data?.trucks || {};

        /*
         * Convert the trucks object into an array
         * so that we can safely use .map().
         */
        const truckList = Object.entries(truckState).map(
          ([truckId, truck]) => ({
            truck_id: truckId,
            readings: Number(truck?.readings || 0),
            avg_temperature: Number(
              truck?.avg_temperature || 0
            ),
          })
        );

        if (mounted) {
          setTrucks(truckList);
        }
      } catch (err) {
        console.error(
          "Failed to load truck data:",
          err
        );

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

    const timer = setInterval(
      loadData,
      3000
    );

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

              const temperature =
                truck.avg_temperature;

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
                <tr
                  key={truck.truck_id}
                >

                  <td>
                    Truck {truck.truck_id}
                  </td>

                  <td>
                    {truck.readings.toLocaleString()}
                  </td>

                  <td>
                    {temperature.toFixed(2)} °C
                  </td>

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