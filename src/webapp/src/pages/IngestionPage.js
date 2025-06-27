import React, { useState } from 'react';
import DataIngestion from '../components/DataIngestion';
import ScenarioIngestion from '../components/ScenarioIngestion';

const IngestionPage = () => {
  const [nonCompliantData, setNonCompliantData] = useState(null);

  // This function would be called by DataIngestion on a compliance failure
  const handleComplianceFailure = (errorData) => {
    // errorData could be an array of objects with {row, field, message}
    setNonCompliantData(errorData);
  };

  return (
    <div>
      <h1>Data Ingestion</h1>
      <p>Upload your policy data and economic scenario files here.</p>
      
      <div className="component-panel">
        <h2>Policy Data</h2>
        {/* Pass the callback to handle compliance failures */}
        <DataIngestion onComplianceFailure={handleComplianceFailure} />
        {nonCompliantData && (
          <div className="compliance-feedback">
            <h3>Compliance Issues Found</h3>
            <p>The following issues were found. Please correct them in a new CSV and upload the corrections file.</p>
            {/* Render the compliance errors here in a table */}
            <pre>{JSON.stringify(nonCompliantData, null, 2)}</pre>
            <h4>Upload Corrections</h4>
            {/* Add a file input here specifically for the correction file */}
          </div>
        )}
      </div>

      <div className="component-panel" style={{marginTop: '25px'}}>
        <h2>Economic Scenarios</h2>
        <ScenarioIngestion />
      </div>
    </div>
  );
};

export default IngestionPage;