import { useEffect, useState } from "react";
import { API } from "../api";

function WorkerTable() {
  const [workers, setWorkers] = useState([]);

  useEffect(() => {
    API.get("/workers")
      .then((res) => setWorkers(res.data))
      .catch((err) => console.error(err));
  }, []);

  API.get("/workers")
  .then((res) => {
    console.log("Workers:", res.data);
    setWorkers(res.data);
  })
  .catch((err) => console.error(err));

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
              <td>{worker.status}</td>
              <td>{worker.partition}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default WorkerTable;