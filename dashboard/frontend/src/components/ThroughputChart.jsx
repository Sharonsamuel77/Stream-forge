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
    let mounted = true;

    const loadMetricsHistory = async () => {
      try {
        const res = await API.get("/metrics/history");

        if (!mounted) return;

        const history = Array.isArray(res.data)
          ? res.data
          : [];

        const formatted = history
          .slice(-20)
          .map((point) => ({
            time: point.time,
            throughput: Number(point.throughput ?? 0),
          }));

        setData(formatted);
      } catch (err) {
        console.error("Throughput history error:", err);
      }
    };

    loadMetricsHistory();

    const timer = setInterval(loadMetricsHistory, 2000);

    return () => {
      mounted = false;
      clearInterval(timer);
    };
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
              `${Number(value).toFixed(2)} events/sec`,
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
