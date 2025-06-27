import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';

// Placeholder for a real Power BI component
const PowerBiEmbed = ({ embedConfig }) => {
    if (!embedConfig) {
        return <div className="report-placeholder">Loading Power BI visual...</div>;
    }
    return (
        <div className="report-embed">
            <h4>Embedded Power BI Report</h4>
            <pre style={{backgroundColor: '#eee', padding: '10px', borderRadius: '5px'}}>
                {JSON.stringify(embedConfig, null, 2)}
            </pre>
            {/* In a real app, you would use a library like 'powerbi-client-react' here */}
            {/* <PowerBIEmbed embedConfig={embedConfig} /> */}
        </div>
    );
};


const ReportPage = () => {
    const { jobId } = useParams();
    const authFetch = useAuthenticatedFetch();
    const [reportData, setReportData] = useState(null);
    const [embedToken, setEmbedToken] = useState(null);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchReportData = async () => {
            setIsLoading(true);
            setError(null);
            try {
                // Fetch both results and embed token in parallel
                const [resultsResponse, tokenResponse] = await Promise.all([
                    authFetch(`/api/jobs/${jobId}/results`),
                    authFetch(`/api/jobs/${jobId}/embed-token`) // Updated to a more RESTful endpoint
                ]);

                if (!resultsResponse.ok || !tokenResponse.ok) {
                    throw new Error('Failed to load full report.');
                }

                const results = await resultsResponse.json();
                const token = await tokenResponse.json();
                
                setReportData(results);
                setEmbedToken(token);

            } catch (err) {
                setError(err.message);
            } finally {
                setIsLoading(false);
            }
        };

        fetchReportData();
    }, [jobId, authFetch]);

    if (isLoading) return <div>Loading report for Job ID: {jobId}...</div>;
    if (error) return <div className="status-message status-error">Error: {error}</div>;
    if (!reportData) return <div>No data found for this report.</div>;

    const handleGeneratePdf = () => {
        // This would trigger a client-side library (like jsPDF) or a backend service
        alert(`Initiating PDF generation for Job ${jobId}...`);
        window.print(); // Simple browser print as a placeholder
    };

    return (
        <div>
            <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                <h1>Report for Job: {jobId}</h1>
                <button onClick={handleGeneratePdf}>Generate PDF</button>
            </div>

            <div className="component-panel">
                <h2>AI-Generated Narrative Summary</h2>
                <p>{reportData.aiNarrative || 'No summary available.'}</p>
            </div>

            <div className="component-panel">
                <h2>Interactive Visuals</h2>
                <PowerBiEmbed embedConfig={embedToken} />
            </div>

            <div className="component-panel">
                <h2>Detailed Numerical Results</h2>
                <pre>{JSON.stringify(reportData.numericalResults, null, 2)}</pre>
            </div>
        </div>
    );
};

export default ReportPage;