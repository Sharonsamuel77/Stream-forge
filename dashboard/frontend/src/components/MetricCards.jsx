import { useEffect, useState } from "react";
import { API } from "../api";

function MetricCards() {
  const [metrics, setMetrics] = useState({});

  useEffect(() => {
    API.get("/metrics")
      .then((res) => setMetrics(res.data))
      .catch(console.error);
  }, []);

  return (
    <div>
      <h2>Metrics</h2>

      <p>Throughput: {metrics.throughput}</p>
      <p>Workers: {metrics.active_workers}</p>
      <p>Failures: {metrics.failed_events}</p>
    </div>
  );
}

export default MetricCards;