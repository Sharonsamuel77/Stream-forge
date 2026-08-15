import MetricCards from "./components/MetricCards";
import WorkerTable from "./components/WorkerTable";
import StreamGraph from "./components/StreamGraph";
import ThroughputChart from "./components/ThroughputChart";

function App() {
  return (
    <div className="dashboard">
      <h1>🚚 StreamForge Dashboard</h1>

      <MetricCards />

      <StreamGraph />

      <WorkerTable />

      <ThroughputChart />
    </div>
  );
}

export default App;