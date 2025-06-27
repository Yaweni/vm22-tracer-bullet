import React, { useState } from 'react';

const DataIngestion = () => {
  const [ingestionMethod, setIngestionMethod] = useState('file'); // 'file' or 'api'
  const [selectedFile, setSelectedFile] = useState(null);
  const [apiUrl, setApiUrl] = useState('');
  const [status, setStatus] = useState({ message: '', type: '' }); // type: 'success', 'error', 'loading'
  const [isLoading, setIsLoading] = useState(false);

  const handleFileChange = (e) => {
    setSelectedFile(e.target.files[0]);
    setStatus({ message: '', type: '' });
  };
  
  const handleApiUrlChange = (e) => {
    setApiUrl(e.target.value);
  }

  const handleFileUpload = async () => {
    if (!selectedFile) return;

    setIsLoading(true);
    setStatus({ message: 'Uploading and processing file...', type: 'loading' });

    try {
      const response = await fetch('/ingest/policies/csv?', {
        method: 'POST',
        headers: {
          'Content-Type': 'text/csv',
        },
        body: selectedFile,
      });

      if (!response.ok) {
        // The backend provides a specific error message in the body for 400/500 errors
        const errorMessage = await response.text();
        throw new Error(errorMessage || 'File processing failed on the server.');
      }

      const resultText = await response.text();
      setStatus({ message: `Success: ${resultText}`, type: 'success' });
    } catch (error) {
      console.error('File upload error:', error);
      setStatus({ message: `Error: ${error.message}`, type: 'error' });
    } finally {
      setIsLoading(false);
    }
  };

  const handleApiConnect = async () => {
    if (!apiUrl) return;

    setIsLoading(true);
    setStatus({ message: 'Connecting and fetching data...', type: 'loading' });

    try {
      const response = await fetch('/ingest/policies/json?', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ source_url: apiUrl }),
      });

      if (!response.ok) {
        const errorMessage = await response.text();
        throw new Error(errorMessage || 'API data fetching failed.');
      }

      const resultText = await response.text();
      setStatus({ message: `Success: ${resultText}`, type: 'success' });
    } catch (error) {
      console.error('API connect error:', error);
      setStatus({ message: `Error: ${error.message}`, type: 'error' });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="component-panel">
      <h2>1. Ingest Policy Data</h2>
      
      <div className="ingestion-choice">
        <button 
          className={ingestionMethod === 'file' ? 'active' : ''}
          onClick={() => setIngestionMethod('file')}
        >
          Upload CSV/Excel File
        </button>
        <button 
          className={ingestionMethod === 'api' ? 'active' : ''}
          onClick={() => setIngestionMethod('api')}
        >
          Connect to JSON API Endpoint
        </button>
      </div>

      {ingestionMethod === 'file' && (
        <div>
          <label htmlFor="file-input">Select policy data file (.csv):</label>
          <input 
            id="file-input" 
            type="file" 
            accept=".csv" 
            onChange={handleFileChange} 
          />
          <button onClick={handleFileUpload} disabled={!selectedFile || isLoading}>
            {isLoading ? 'Processing...' : 'Upload & Process File'}
          </button>
        </div>
      )}

      {ingestionMethod === 'api' && (
        <div>
          <label htmlFor="api-url-input">API URL:</label>
          <input 
            id="api-url-input" 
            type="text" 
            value={apiUrl} 
            onChange={handleApiUrlChange}
            placeholder="https://your-data-source.com/api/policies" 
          />
          <button onClick={handleApiConnect} disabled={!apiUrl || isLoading}>
            {isLoading ? 'Connecting...' : 'Connect & Fetch Data'}
          </button>
        </div>
      )}
      
      {status.message && (
        <div className={`status-message status-${status.type}`}>
          {status.message}
        </div>
      )}
    </div>
  );
};

export default DataIngestion;