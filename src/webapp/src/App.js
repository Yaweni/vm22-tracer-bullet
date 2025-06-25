import React, { useState } from 'react';
import Header from './components/Header';
import DataIngestion from './components/DataIngestion';
import ScenarioRunner from './components/ScenarioRunner';
import JobsTable from './components/JobsTable';
// import ReportViewer from './components/ReportViewer'; // To be used with routing later

function App() {
  // State is "lifted up" to App so ScenarioRunner can notify JobsTable
  const [newJobId, setNewJobId] = useState(null);

  // This callback is passed to ScenarioRunner.
  // When a job is queued, ScenarioRunner calls this function.
  const handleJobQueued = (jobId) => {
    setNewJobId(jobId);
  };

  return (
    <>
      <Header />
      <div className="app-container">
        <DataIngestion />
        <ScenarioRunner onJobQueued={handleJobQueued} />
        <JobsTable newJobId={newJobId} />
        {/* The ReportViewer would likely be part of a routing setup, e.g., /reports/:jobId */}
        {/* <ReportViewer /> */}
      </div>
    </>
  );
}

export default App;