import React from 'react';
import { useParams } from 'react-router-dom';
import ReportViewer from '../components/ReportViewer';

const ReportPage = () => {
  // This hook gets the 'jobId' from the URL '/reports/:jobId'
  const { jobId } = useParams();

  return (
    <div>
      <h1>Report for Job ID: {jobId}</h1>
      {/* ReportViewer will fetch data based on this jobId */}
      <ReportViewer reportId={jobId} />
      {/* This is where you would also fetch and display the LLM-generated text */}
      <div className="component-panel" style={{marginTop: '25px'}}>
        <h2>AI Generated Summary & Analysis</h2>
        <p>Loading analysis...</p>
      </div>
    </div>
  );
};

export default ReportPage;