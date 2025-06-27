import React, { useState, useEffect, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { useNavigate } from 'react-router-dom';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';

const JobsPage = () => {
    const [jobs, setJobs] = useState([]);
    const authFetch = useAuthenticatedFetch();
    const navigate = useNavigate();

    const fetchJobs = useCallback(async () => {
        try {
            const response = await authFetch('/api/jobs');
            if (!response.ok) throw new Error('Failed to fetch jobs.');
            const data = await response.json();
            setJobs(data);
        } catch (error) {
            console.error(error);
            // Optionally set an error state to display to the user
        }
    }, [authFetch]);

    // Fetch jobs on initial load and set up polling
    useEffect(() => {
        fetchJobs(); // Initial fetch
        const intervalId = setInterval(fetchJobs, 10000); // Poll every 10 seconds

        // Cleanup function to clear interval when the component unmounts
        return () => clearInterval(intervalId);
    }, [fetchJobs]);

    // Custom component for the "View Report" button
    const ViewReportButtonRenderer = (props) => {
        const isCompleted = props.data.status.toLowerCase() === 'complete';
        const handleClick = () => {
            navigate(`/reports/${props.data.jobId}`);
        };

        return (
            <button onClick={handleClick} disabled={!isCompleted}>
                View Report
            </button>
        );
    };
    
    // Custom cell renderer for status with styling
    const StatusCellRenderer = (props) => {
        const status = props.value ? props.value.toLowerCase() : 'unknown';
        return <span className={`status-cell status-${status}`}>{props.value}</span>
    };

    const columnDefs = [
        { field: 'jobId', headerName: 'Job ID', flex: 1 },
        { field: 'status', headerName: 'Status', flex: 1, cellRenderer: StatusCellRenderer },
        { field: 'requestedTimestamp', headerName: 'Requested Time', flex: 2,
          valueFormatter: params => new Date(params.value).toLocaleString()
        },
        { headerName: 'Actions', cellRenderer: ViewReportButtonRenderer, flex: 1, filter: false, sortable: false }
    ];

    return (
        <div>
            <h1>Job History</h1>
            <p>This page shows the status of your past and current jobs, refreshing automatically.</p>
            <div className="ag-theme-alpine" style={{ height: 600, width: '100%' }}>
                <AgGridReact
                    rowData={jobs}
                    columnDefs={columnDefs}
                    defaultColDef={{
                        sortable: true,
                        filter: true,
                        resizable: true,
                    }}
                    rowSelection="single"
                />
            </div>
        </div>
    );
};

export default JobsPage;

