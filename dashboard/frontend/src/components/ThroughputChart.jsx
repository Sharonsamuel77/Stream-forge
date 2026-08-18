import { useEffect, useState } from "react";
import { API } from "../api";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from "recharts";

export default function ThroughputChart() {

  const [data, setData] = useState([]);

  useEffect(() => {

    const loadMetrics = async () => {
      try {

        const res = await API.get("/metrics");

        const point = {
          time: new Date().toLocaleTimeString(),
          throughput: res.data.throughput
        };

        setData((prev) => {
          const updated = [...prev, point];

          // Keep only last 10 points
          return updated.slice(-10);
        });

      } catch (err) {
        console.error(err);
      }
    };

    loadMetrics();

    const timer = setInterval(loadMetrics, 3000);

    return () => clearInterval(timer);

  }, []);

  return (
    <div>
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
            dot={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}