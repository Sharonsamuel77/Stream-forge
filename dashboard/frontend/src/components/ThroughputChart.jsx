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

export default function ThroughputChart() {
  const [data, setData] = useState([]);

  useEffect(() => {
    const loadMetrics = async () => {
      try {
        const res = await API.get("/metrics");

        const point = {
          time: new Date().toLocaleTimeString(),
          throughput: Number(res.data.throughput || 0),
        };

        setData((prev) => [...prev, point].slice(-10));
      } catch (err) {
        console.error("Throughput metrics error:", err);
      }
    };

    loadMetrics();

    const timer = setInterval(loadMetrics, 3000);

    return () => clearInterval(timer);
  }, []);

  return (
    <div className="throughput-chart">
      <h2>Kafka Throughput</h2>

      <ResponsiveContainer width="100%" height={300}>
        <LineChart
          data={data}
          margin={{
            top: 10,
            right: 20,
            left: 10,
            bottom: 10,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />

          <XAxis dataKey="time" />

          <YAxis
            label={{
              value: "Events/sec",
              angle: -90,
              position: "insideLeft",
            }}
          />

          <Tooltip
            formatter={(value) => [
              `${value} events/sec`,
              "Throughput",
            ]}
          />

          <Line
            type="monotone"
            dataKey="throughput"
            stroke="#00d4ff"
            strokeWidth={3}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}