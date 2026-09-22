import { useEffect, useState } from "react";
import { API } from "../api";

export default function RecoveryCard() {
  const [recovery, setRecovery] = useState({
    status: "CHECKING",
    state_store: "RocksDB",
    records: 0,
  });

  useEffect(() => {
    let mounted = true;

    const loadData = async () => {
      try {
        const res = await API.get("/recovery");

        const records = Number(
          res.data?.records || 0
        );

        if (mounted) {
          setRecovery({
            status: records > 0 ? "Recovered" : "UNAVAILABLE",
            state_store: "RocksDB",
            records,
          });
        }
      } catch (err) {
        console.error("Recovery:", err);

        if (mounted) {
          setRecovery({
            status: "UNAVAILABLE",
            state_store: "RocksDB",
            records: 0,
          });
        }
      }
    };

    loadData();

    const timer = setInterval(loadData, 5000);

    return () => {
      mounted = false;
      clearInterval(timer);
    };
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
