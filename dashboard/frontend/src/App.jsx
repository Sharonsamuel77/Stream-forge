import MetricCards from "./components/MetricCards";
import WorkerTable from "./components/WorkerTable";
import StreamGraph from "./components/StreamGraph";
import ThroughputChart from "./components/ThroughputChart";
import TruckTable from "./components/TruckTable";
import AlertsPanel from "./components/AlertsPanel";
import RecoveryCard from "./components/RecoveryCard";

function App() {
  return (
    <div className="dashboard">
      <h1>🚚 StreamForge Dashboard</h1>

      <MetricCards />

      <RecoveryCard/>

      <StreamGraph />

      <WorkerTable />

      <TruckTable />

      <AlertsPanel />

      <ThroughputChart />
    </div>
  );
}

export default App;