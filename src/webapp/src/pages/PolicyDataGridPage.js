import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { AgGridReact } from 'ag-grid-react';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';

const PolicyDataGridPage = () => {
    const { setId } = useParams();
    const [rowData, setRowData] = useState([]);
    const [columnDefs, setColumnDefs] = useState([]);
    const authFetch = useAuthenticatedFetch();

    useEffect(() => {
        authFetch(`/api/policies?setId=${setId}`)
            .then(res => res.json())
            .then(data => {
                if (data.length > 0) {
                    // Dynamically create columns from the first data object's keys
                    const cols = Object.keys(data[0]).map(key => ({
                        field: key,
                        headerName: key,
                        editable: key !== 'id', // Make all fields except ID editable
                    }));
                    setColumnDefs([...cols, { headerName: "Delete", cellRenderer: DeleteButtonRenderer }]);
                    setRowData(data);
                }
            });
    }, );

    const handleCellValueChanged = (event) => {
        console.log("Saving changes for row:", event.data);
        authFetch('/api/policies/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(event.data)
        }).catch(console.error);
    };
    
    const DeleteButtonRenderer = (props) => {
      const handleDelete = () => {
        if(window.confirm(`Delete policy ${props.data.id}?`)) {
          authFetch(`/api/policies/${props.data.id}`, { method: 'DELETE' })
            .then(res => {
              if (res.ok) {
                // Refresh data or remove row from grid state
                setRowData(prev => prev.filter(row => row.id !== props.data.id));
              }
            });
        }
      };
      return <button onClick={handleDelete}>Delete</button>
    }

    return (
        <div>
            <h1>Viewing Policy Set: {setId}</h1>
            <div className="ag-theme-alpine" style={{ height: 700, width: '100%' }}>
                <AgGridReact
                    rowData={rowData}
                    columnDefs={columnDefs}
                    onCellValueChanged={handleCellValueChanged}
                    defaultColDef={{ flex: 1, filter: true, sortable: true }}
                />
            </div>
        </div>
    );
};

export default PolicyDataGridPage;