import React, { useEffect, useState } from "react";
import ReactFlow, {
  Controls,
  Background,
} from "reactflow";

import "reactflow/dist/style.css";

const API_URL = "http://127.0.0.1:8002";
const WS_URL = "ws://127.0.0.1:8002/ws/metrics";

// Keep these OUTSIDE the component.
// This prevents the React Flow nodeTypes/edgeTypes warning.
const nodeTypes = {};
const edgeTypes = {};

function StreamGraph() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [totalThroughput, setTotalThroughput] = useState(0);
  const [connected, setConnected] = useState(false);

  // ==========================================================
  // LOAD TOPOLOGY
  // ==========================================================

  useEffect(() => {
    let cancelled = false;

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

        if (!cancelled) {
          setNodes(
            Array.isArray(data.nodes)
              ? data.nodes
              : []
          );

          setEdges(
            Array.isArray(data.edges)
              ? data.edges
              : []
          );
        }
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

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  // ==========================================================
  // WEBSOCKET METRICS
  // ==========================================================

  useEffect(() => {
    let websocket = null;
    let reconnectTimer = null;
    let stopped = false;

    const connect = () => {
      if (stopped) {
        return;
      }

      console.log(
        "Connecting to StreamForge metrics..."
      );

      websocket = new WebSocket(WS_URL);

      websocket.onopen = () => {
        if (stopped) {
          websocket.close();
          return;
        }

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

          const throughput = Number(
            metrics.total_events_sec ??
              metrics.throughput ??
              metrics.total_throughput ??
              0
          );

          if (Number.isFinite(throughput)) {
            setTotalThroughput(throughput);
          } else {
            setTotalThroughput(0);
          }
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

        if (!stopped) {
          reconnectTimer = setTimeout(
            connect,
            3000
          );
        }
      };
    };

    connect();

    return () => {
      stopped = true;

      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }

      if (websocket) {
        websocket.onclose = null;
        websocket.onerror = null;

        if (
          websocket.readyState ===
            WebSocket.OPEN ||
          websocket.readyState ===
            WebSocket.CONNECTING
        ) {
          websocket.close();
        }
      }
    };
  }, []);

  // ==========================================================
  // RENDER
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
            {totalThroughput.toFixed(2)} msg/s
          </span>
        </div>
      </div>

      <div
        style={{
          width: "100%",
          height: "500px",
          borderRadius: "10px",
          overflow: "hidden",
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          fitViewOptions={{
            padding: 0.2,
          }}
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>

    </div>
  );
}

export default StreamGraph;