import React, { useState, useEffect } from 'react';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';

const CalculationLabPage = () => {
    const authFetch = useAuthenticatedFetch();
    
    // State for data fetched from API
    const [policySets, setPolicySets] = useState([]);
    const [scenarios, setScenarios] = useState([]);
    const [productCodes, setProductCodes] = useState([]);

    // State for user selections
    const [selectedSetIds, setSelectedSetIds] = useState([]);
    const [selectedProductCodes, setSelectedProductCodes] = useState([]);
    const [selectedScenarioId, setSelectedScenarioId] = useState('');
    const [assumptions, setAssumptions] = useState('');
    const [runStochastic, setRunStochastic] = useState(false);
    const [runAttribution, setRunAttribution] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    
    // Fetch initial data (policy sets, scenarios)
    useEffect(() => {
        authFetch('/api/my-policy-sets').then(res => res.json()).then(setPolicySets);
        authFetch('/api/my-scenarios').then(res => res.json()).then(setScenarios);
    }, [authFetch]);

    // ** DYNAMIC LOGIC **
    // Fetch product codes whenever selected policy sets change
    useEffect(() => {
        if (selectedSetIds.length === 0) {
            setProductCodes([]);
            setSelectedProductCodes([]);
            return;
        }
        const query = new URLSearchParams({ setIds: selectedSetIds.join(',') }).toString();
        authFetch(`/api/product-codes?${query}`)
            .then(res => res.json())
            .then(setProductCodes);
    }, [selectedSetIds, authFetch]);

    const handlePolicySetChange = (e) => {
        const { value, checked } = e.target;
        setSelectedSetIds(prev => checked ? [...prev, value] : prev.filter(id => id !== value));
    };
    
    const handleProductCodeChange = (e) => {
        const { value, checked } = e.target;
        setSelectedProductCodes(prev => checked ? [...prev, value] : prev.filter(code => code !== value));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setIsLoading(true);
        const jobConfig = {
            policySetIds: selectedSetIds,
            productCodes: selectedProductCodes,
            scenarioId: selectedScenarioId,
            assumptionsText: assumptions,
            runStochastic: runStochastic,
            includeAttribution: runAttribution,
        };
        
        try {
          const response = await authFetch('/api/calculate', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(jobConfig)
          });
          // Handle response...
          alert('Job queued successfully!', response.status);
        } catch(error) {
          console.error('Failed to queue job', error);
          alert('Error: Could not queue job.');
        } finally {
          setIsLoading(false);
        }
    };

    return (
        <div>
            <h1>Calculation Lab</h1>
            <form onSubmit={handleSubmit} className="component-panel">
                <h2>1. Select Data</h2>
                <div style={{display: 'flex', gap: '50px'}}>
                    <div>
                        <label className="label-text">Policy Sets:</label>
                        {policySets.map(set => (
                            <label key={set.id}><input type="checkbox" value={set.id} onChange={handlePolicySetChange}/> {set.name}</label>
                        ))}
                    </div>
                    <div>
                        <label className="label-text">Product Codes (from selected sets):</label>
                        {productCodes.map(code => (
                            <label key={code}><input type="checkbox" value={code} onChange={handleProductCodeChange}/> {code}</label>
                        ))}
                    </div>
                </div>

                <h2>2. Select Scenarios & Assumptions</h2>
                <label htmlFor="scenario-select" className="label-text">Economic Scenarios:</label>
                <select id="scenario-select" value={selectedScenarioId} onChange={e => setSelectedScenarioId(e.target.value)} required>
                    <option value="" disabled>-- Select a scenario --</option>
                    <option value="default">Default Scenarios</option>
                    {scenarios.map(scen => <option key={scen.id} value={scen.id}>{scen.name}</option>)}
                </select>
                <label htmlFor="assumptions" className="label-text">Assumptions:</label>
                <textarea id="assumptions" value={assumptions} onChange={e => setAssumptions(e.target.value)} />

                <h2>3. Configure Output</h2>
                <label><input type="checkbox" checked={runStochastic} onChange={e => setRunStochastic(e.target.checked)} /> Run Stochastic Scenarios?</label>
                <label><input type="checkbox" checked={runAttribution} onChange={e => setRunAttribution(e.target.checked)} /> Include Attribution Analysis?</label>
                
                <button type="submit" disabled={isLoading} style={{marginTop: '20px'}}>Queue Calculation Job</button>
            </form>
        </div>
    );
};

export default CalculationLabPage;