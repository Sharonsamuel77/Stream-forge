import { useEffect, useState } from "react";
import { API } from "../api";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

function ThroughputChart() {
  const [data, setData] = useState([]);

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const res = await API.get("/metrics/history");
        setData(res.data);
      } catch (err) {
        console.error(err);
      }
    };

    fetchMetrics();

    const interval = setInterval(fetchMetrics, 3000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="chart-container">
      <h2>Kafka Throughput</h2>

      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" />
          <YAxis />
          <Tooltip />

          <Line
            type="monotone"
            dataKey="throughput"
            stroke="#00d4ff"
            strokeWidth={3}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default ThroughputChart;