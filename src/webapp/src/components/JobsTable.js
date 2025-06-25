import React, { useState, useEffect } from 'react';

const JobsTable = ({ newJobId }) => {
  const [jobs, setJobs] = useState([]);
  const [error, setError] = useState(null);

  const fetchJobs = async () => {
    try {
      const response = await fetch('https://func-vm22-tracer-engine.azurewebsites.net/api/jobs?');
      if (!response.ok) {
        throw new Error('Failed to fetch jobs from the server.');
      }
      const data = await response.json();
      setJobs(data);
      setError(null);
    } catch (err) {
      console.error('Error fetching jobs:', err);
      setError(err.message);
    }
  };

  // 1. Fetch jobs on initial component mount
  useEffect(() => {
    fetchJobs();
  }, []);

  // 2. Set up polling to refresh jobs every 10 seconds
  useEffect(() => {
    const intervalId = setInterval(fetchJobs, 10000);
    // Cleanup function to clear the interval when the component unmounts
    return () => clearInterval(intervalId);
  }, []);

  // 3. Immediately refetch when a new job is created (via prop change)
  useEffect(() => {
    if (newJobId) {
      fetchJobs();
    }
  }, [newJobId]);
  
  const getStatusClassName = (status) => {
      // FIX: Add a guard clause. If status is null, undefined, or an empty string,
      // return a default empty class name to prevent the crash.
      if (!status) {
        return ''; 
      }
      
      // If the guard clause passes, it's safe to call toLowerCase()
      switch (status.toLowerCase()) {
          case 'complete': return 'status-complete';
          case 'failed': return 'status-failed';
          case 'running': return 'status-running';
          case 'queued': return 'status-queued';
          default: return '';
      }
  }

  return (
    <div className="component-panel">
      <h2>Calculation History</h2>
      <button onClick={fetchJobs}>Refresh</button>
      {error && <div className="status-message status-error">{error}</div>}
      <table className="jobs-table">
        <thead>
          <tr>
            <th>Job ID</th>
            <th>Product(s)</th>
            <th>Status</th>
            <th>Requested Timestamp</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.length > 0 ? jobs.map(job => (
            <tr key={job.JobID}>
              <td>{job.JobID}</td>
              <td>{Array.isArray(job.ProductCode) ? job.ProductCode.join(', ') : job.ProductCode}</td>
              {/* The className function is now safe */}
              <td className={`status-cell ${getStatusClassName(job.Status)}`}>
                {/* IMPROVEMENT: Add a fallback for display purposes */}
                {job.Status || 'Pending...'}
              </td>
              <td>{new Date(job.RequestedTimestamp).toLocaleString()}</td>
              <td>
                {job.Status && job.Status.toLowerCase() === 'complete' ? (
                  <a href={`/reports/${job.JobID}`} className="view-results-link">View Results</a>
                ) : (
                  '--'
                )}
              </td>
            </tr>
          )) : (
            <tr>
              <td colSpan="5" style={{ textAlign: 'center' }}>No jobs found.</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default JobsTable;