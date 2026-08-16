import { useEffect, useState } from "react";
import { API } from "../api";

export default function RecoveryCard() {
  const [recovery, setRecovery] = useState({});

  useEffect(() => {

  const loadData = async () => {
    try {
      const res = await API.get("/recovery");
      setRecovery(res.data);
    } catch (err) {
      console.error(err);
    }
  };

  loadData();

  const timer = setInterval(loadData, 5000);

  return () => clearInterval(timer);

}, []);

  return (
    <div className="metric-card">
      <h3>Recovery Status</h3>

      <p><b>Status:</b> {recovery.status}</p>

      <p><b>Store:</b> {recovery.state_store}</p>

      <p><b>Records:</b> {recovery.records}</p>
    </div>
  );
}