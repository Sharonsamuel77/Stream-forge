import { useEffect, useState } from "react";
import { API } from "../api";

export default function WorkerTable() {
  const [workers, setWorkers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const response = await API.get("/workers");

        console.log("Workers:", response.data);

        // Backend returns an array directly
        setWorkers(Array.isArray(response.data)
          ? response.data
          : response.data.workers || []
        );
      } catch (err) {
        console.error("Failed to load workers:", err);
        setWorkers([]);
      } finally {
        setLoading(false);
      }
    };

    loadData();

    const timer = setInterval(loadData, 3000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="worker-section">
      <h2>Workers</h2>

      {loading ? (
        <p>Loading...</p>
      ) : workers.length === 0 ? (
        <p>No active workers</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Worker</th>
              <th>Status</th>
              <th>Last Seen</th>
            </tr>
          </thead>

          <tbody>
            {workers.map((worker) => (
              <tr key={worker.id}>
                <td>{worker.id}</td>

                <td
                  style={{
                    color:
                      String(worker.status).toLowerCase() === "active"
                        ? "#00ff88"
                        : "#ff6b6b",
                    fontWeight: "bold",
                  }}
                >
                  {String(worker.status).toUpperCase()}
                </td>

                <td>
                  {worker.last_seen || "N/A"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}