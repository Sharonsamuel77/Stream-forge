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

        // Backend returns: { workers: [...] }
        setWorkers(response.data.workers || []);
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
              <th>PID</th>
              <th>Status</th>
            </tr>
          </thead>

          <tbody>
            {workers.map((worker) => (
              <tr key={worker.id}>
                <td>Worker #{worker.id}</td>
                <td>{worker.pid}</td>

                <td
                  style={{
                    color:
                      worker.status === "HEALTHY"
                        ? "#00d4ff"
                        : "#ff6b6b",
                    fontWeight: "bold",
                  }}
                >
                  {worker.status}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}