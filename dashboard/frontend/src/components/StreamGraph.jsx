import React, { useEffect, useState } from "react";
import ReactFlow, {
  Controls,
  Background,
} from "reactflow";

import "reactflow/dist/style.css";

const API_URL = "http://127.0.0.1:8001";
const WS_URL = "ws://127.0.0.1:8001/ws/metrics";

function StreamGraph() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [totalThroughput, setTotalThroughput] = useState(0);
  const [connected, setConnected] = useState(false);

  // ==========================================================
  // Load topology
  // ==========================================================

  useEffect(() => {
    const loadTopology = async () => {
      try {
        const response = await fetch(
          `${API_URL}/api/topology`
        );

        if (!response.ok) {
          throw new Error(
            `Topology request failed: ${response.status}`
          );
        }

        const data = await response.json();

        setNodes(data.nodes || []);
        setEdges(data.edges || []);
      } catch (error) {
        console.error(
          "Failed to load topology:",
          error
        );
      }
    };

    loadTopology();

    const timer = setInterval(
      loadTopology,
      5000
    );

    return () => clearInterval(timer);
  }, []);

  // ==========================================================
  // WebSocket metrics
  // ==========================================================

  useEffect(() => {
    let websocket;
    let reconnectTimer;

    const connect = () => {
      console.log(
        "Connecting to StreamForge metrics..."
      );

      websocket = new WebSocket(WS_URL);

      websocket.onopen = () => {
        console.log(
          "Connected to StreamForge metrics"
        );

        setConnected(true);
      };

      websocket.onmessage = (event) => {
        try {
          const metrics = JSON.parse(
            event.data
          );

          setTotalThroughput(
            Number(
              metrics.total_events_sec || 0
            )
          );
        } catch (error) {
          console.error(
            "Invalid WebSocket data:",
            error
          );
        }
      };

      websocket.onerror = (error) => {
        console.error(
          "WebSocket error:",
          error
        );

        setConnected(false);
      };

      websocket.onclose = () => {
        console.log(
          "Disconnected from StreamForge metrics"
        );

        setConnected(false);

        reconnectTimer = setTimeout(
          connect,
          3000
        );
      };
    };

    connect();

    return () => {
      clearTimeout(reconnectTimer);

      if (websocket) {
        websocket.close();
      }
    };
  }, []);

  // ==========================================================
  // Render
  // ==========================================================

  return (
    <div className="stream-graph">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "10px",
        }}
      >
        <h2>Stream Topology</h2>

        <div>
          <span>
            {connected
              ? "🟢 Live"
              : "🔴 Disconnected"}
          </span>

          <span
            style={{
              marginLeft: "20px",
              fontWeight: "bold",
            }}
          >
            Throughput:{" "}
            {totalThroughput.toLocaleString()} msg/s
          </span>
        </div>
      </div>

      <div
        style={{
          width: "100%",
          height: "500px",
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </div>
  );
}

export default StreamGraph;