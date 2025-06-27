import React, { useState, useEffect } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { useNavigate } from 'react-router-dom';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';
import FileUpload from '../components/FileUpload'; // Import the new component

const DataManagementPage = () => {
    const [activeTab, setActiveTab] = useState('policies');
    const [policySets, setPolicySets] = useState([]);
    // const [scenarioSets, setScenarioSets] = useState([]);
    const navigate = useNavigate();
    const authFetch = useAuthenticatedFetch();

    useEffect(() => {
        if (activeTab === 'policies') {
            authFetch('/api/policy-sets')
                .then(res => res.json())
                .then(data => setPolicySets(data))
                .catch(console.error);
        } else {
            // Fetch scenario sets when this tab is active
            // authFetch('/api/scenario-sets').then...
        }
    }, [activeTab, authFetch]);

    const ViewEditButtonRenderer = (props) => {
        const handleClick = () => {
            navigate(`/data/policies/${props.data.id}`);
        };
        return <button onClick={handleClick}>View/Edit Data</button>;
    };

    const policyCols = [
        { field: 'name', headerName: 'Policy Set Name', flex: 2 },
        { field: 'recordCount', headerName: 'Records', flex: 1 },
        { field: 'createdAt', headerName: 'Upload Date', flex: 1, valueFormatter: p => new Date(p.value).toLocaleDateString() },
        { headerName: 'Actions', cellRenderer: ViewEditButtonRenderer, flex: 1, sortable: false, filter: false }
    ];

    return (
        <div>
            <h1>Data Management</h1>
            <div className="tab-navigation">
                <button onClick={() => setActiveTab('policies')} className={activeTab === 'policies' ? 'active' : ''}>Policy Sets</button>
                <button onClick={() => setActiveTab('scenarios')} className={activeTab === 'scenarios' ? 'active' : ''}>Economic Scenario Sets</button>
            </div>

            <div className="component-panel">
                {activeTab === 'policies' && (
                    <>
                        <h2>Upload New Policy Set</h2>
                        <FileUpload 
                            label="Select policy data file (.csv):" 
                            getUploadUrlEndpoint="/api/policy-sets/get-upload-url"
                        />
                        <h2 style={{marginTop: '30px'}}>Existing Policy Sets</h2>
                        <div className="ag-theme-alpine" style={{ height: 400, width: '100%' }}>
                            <AgGridReact rowData={policySets} columnDefs={policyCols} />
                        </div>
                    </>
                )}
                {activeTab === 'scenarios' && (
                     <>
                        <h2>Upload New Scenario Set</h2>
                        <FileUpload 
                            label="Select economic scenario file (.csv):" 
                            getUploadUrlEndpoint="/api/scenario-sets/get-upload-url"
                        />
                        <h2 style={{marginTop: '30px'}}>Existing Scenario Sets</h2>
                        {/* AG-Grid for scenario sets would go here */}
                        <p>Scenario sets grid will be displayed here.</p>
                    </>
                )}
            </div>
        </div>
    );
};

export default DataManagementPage;