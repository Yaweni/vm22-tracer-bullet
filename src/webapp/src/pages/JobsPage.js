import React from 'react';
import JobsTable from '../components/JobsTable';

const JobsPage = () => {
  return (
    <div>
      <h1>Calculation Jobs</h1>
      <p>View the status of current and past jobs. The list automatically refreshes.</p>
      {/* The JobsTable component is self-contained */}
      <JobsTable />
    </div>
  );
};

export default JobsPage;