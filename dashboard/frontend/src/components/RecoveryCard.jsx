import { useEffect, useState } from "react";
import { API } from "../api";

export default function RecoveryCard() {
  const [recovery, setRecovery] = useState({
    status: "CHECKING",
    state_store: "RocksDB",
    records: 0,
  });

  useEffect(() => {
    const loadData = async () => {
      try {
        const res = await API.get("/state");

        setRecovery({
          status: res.data.error ? "ERROR" : "RECOVERED",
          state_store: "RocksDB",
          records: Number(res.data.total_readings || 0),
        });
      } catch (err) {
        console.error("Recovery:", err);

        setRecovery({
          status: "UNAVAILABLE",
          state_store: "RocksDB",
          records: 0,
        });
      }
    };

    loadData();

    const timer = setInterval(loadData, 5000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="metric-card">
      <h3>Recovery Status</h3>

      <p>
        <b>Status:</b> {recovery.status}
      </p>

      <p>
        <b>Store:</b> {recovery.state_store}
      </p>

      <p>
        <b>Records:</b>{" "}
        {recovery.records.toLocaleString()}
      </p>
    </div>
  );
}