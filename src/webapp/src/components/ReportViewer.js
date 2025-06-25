import React from 'react';

// This is a placeholder for the Power BI report viewer.
// A full implementation would use a library like 'powerbi-client-react'
// and fetch an embed token from the backend.

const ReportViewer = ({ reportId }) => {
  return (
    <div className="component-panel">
      <h2>Report Viewer</h2>
      <p>
        This area will display the Power BI report for a completed job.
      </p>
      <p>
        <strong>Report ID to load:</strong> {reportId || 'None selected'}
      </p>
      <div style={{ height: '500px', backgroundColor: '#e9ecef', border: '1px dashed #ccc', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <p style={{color: '#666'}}>Power BI Report Embed Area</p>
      </div>
    </div>
  );
};

export default ReportViewer;