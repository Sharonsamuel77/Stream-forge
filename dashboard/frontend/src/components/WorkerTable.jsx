import { useEffect, useState } from "react";
import { API } from "../api";

function WorkerTable() {
  const [workers, setWorkers] = useState([]);

  useEffect(() => {
    const fetchWorkers = () => {
      API.get("/workers")
        .then((res) => {
          console.log("Workers:", res.data);
          setWorkers(res.data);
        })
        .catch((err) => console.error(err));
    };

    fetchWorkers();

    const interval = setInterval(fetchWorkers, 3000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="workers-section">
      <h2>Workers</h2>

      <table>
        <thead>
          <tr>
            <th>Worker</th>
            <th>Status</th>
            <th>Partition</th>
          </tr>
        </thead>

        <tbody>
          {workers.map((worker) => (
            <tr key={worker.id}>
              <td>{worker.id}</td>

              <td
                style={{
                  color:
                    worker.status === "active"
                      ? "lime"
                      : "red",
                  fontWeight:"bold"
                }}
              >
                {worker.status}
              </td>

              <td>{worker.partition}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default WorkerTable;