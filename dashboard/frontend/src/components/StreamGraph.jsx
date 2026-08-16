import ReactFlow, { Controls, Background } from "reactflow";
import "reactflow/dist/style.css";

const nodes = [
  {
    id: "kafka",
    position: { x: 250, y: 20 },
    data: { label: "Kafka" },
  },
  {
    id: "p0",
    position: { x: 50, y: 150 },
    data: { label: "Partition 0" },
  },
  {
    id: "p1",
    position: { x: 250, y: 150 },
    data: { label: "Partition 1" },
  },
  {
    id: "p2",
    position: { x: 450, y: 150 },
    data: { label: "Partition 2" },
  },
  {
    id: "w1",
    position: { x: 50, y: 300 },
    data: { label: "Worker 1" },
  },
  {
    id: "w2",
    position: { x: 250, y: 300 },
    data: { label: "Worker 2" },
  },
  {
    id: "w3",
    position: { x: 450, y: 300 },
    data: { label: "Worker 3" },
  },
];

const edges = [
  { id: "e1", source: "kafka", target: "p0" },
  { id: "e2", source: "kafka", target: "p1" },
  { id: "e3", source: "kafka", target: "p2" },

  { id: "e4", source: "p0", target: "w1" },
  { id: "e5", source: "p1", target: "w2" },
  { id: "e6", source: "p2", target: "w3" },
];

function StreamGraph() {
  return (
    <div className="stream-graph">
      <h2>Stream Topology</h2>

      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
      >
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  );
}

export default StreamGraph;