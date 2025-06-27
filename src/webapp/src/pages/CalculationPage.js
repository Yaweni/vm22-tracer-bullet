import React, { useState, useEffect } from 'react';

// The original ScenarioRunner logic is now inside this page component
const CalculationPage = () => {
  // Mock data for now. This would be fetched from your API.
  const [availableProducts, setAvailableProducts] = useState(['SPDA_G3', 'FIA_S&P_C8', 'VA_GLWB5']);
  const [availableScenarios, setAvailableScenarios] = useState([
    { id: 'scen_01', name: 'Baseline 2024 Interest Rates' },
    { id: 'scen_02', name: 'High Inflation Stress Test' }
  ]);
  
  const [selectedProducts, setSelectedProducts] = useState([]);
  const [selectedScenario, setSelectedScenario] = useState('');
  
  const [calcStochastic, setCalcStochastic] = useState(false);
  const [includeAttribution, setIncludeAttribution] = useState(false);
  const [assumptionsText, setAssumptionsText] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [status, setStatus] = useState({ message: '', type: '' });



  const handleProductChange = (e) => {
    const { value, checked } = e.target;
    setSelectedProducts(prev => checked ? [...prev, value] : prev.filter(p => p !== value));
  };
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    setStatus({ message: 'Submitting job configuration...', type: 'loading' });

    const jobConfig = {
      product_codes: selectedProducts,
      scenario_id: selectedScenario,
      calculate_stochastic: calcStochastic,
      perform_attribution: includeAttribution,
      assumptions_text: assumptionsText,
    };

    try {
      const response = await fetch('/calculate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(jobConfig),
      });

      if (response.status !== 202) {
        const errorText = await response.text();
        throw new Error(errorText || `Server responded with status ${response.status}`);
      }
      
      const result = await response.json();
      setStatus({ message: `Job ${result.job_id} successfully queued.`, type: 'success' });

    } catch (error) {
      setStatus({ message: `Error: ${error.message}`, type: 'error' });
    } finally {
      setIsLoading(false);
    }
  };
  
  const isButtonDisabled = isLoading || !selectedScenario || selectedProducts.length === 0;

  return (
    <div className="component-panel">
      <h2>2. Configure & Run Calculation</h2>
      <form onSubmit={handleSubmit}>
        <div className="checkbox-group">
          <span className="label-text">Select Ingested Product Blocks:</span>
          {availableProducts.map(product => (
            <label key={product} className="checkbox-item">
              <input type="checkbox" value={product} onChange={handleProductChange} />
              {product}
            </label>
          ))}
        </div>

        <div>
          <label htmlFor="scenario-select" className="label-text">Select Economic Scenario File:</label>
          <select 
            id="scenario-select" 
            value={selectedScenario} 
            onChange={e => setSelectedScenario(e.target.value)}
            required
          >
            <option value="" disabled>-- Choose a scenario --</option>
            {availableScenarios.map(scen => <option key={scen.id} value={scen.id}>{scen.name}</option>)}
          </select>
        </div>

        <div className="checkbox-group" style={{marginTop: '20px'}}>
            <span className="label-text">Calculation Options:</span>
            <label className="checkbox-item">
              <input type="checkbox" checked={calcStochastic} onChange={(e) => setCalcStochastic(e.target.checked)} />
              Calculate Stochastic Reserves?
            </label>
            <label className="checkbox-item">
              <input type="checkbox" checked={includeAttribution} onChange={(e) => setIncludeAttribution(e.target.checked)} />
              Include Attribution Analysis?
            </label>
        </div>
        
        <div>
          <label htmlFor="assumptions-text" className="label-text">Describe dynamic assumption logic:</label>
          <textarea id="assumptions-text" value={assumptionsText} onChange={e => setAssumptionsText(e.target.value)} />
        </div>

        <button type="submit" disabled={isButtonDisabled}>
          {isLoading ? 'Queueing...' : 'Queue Calculation Job'}
        </button>
      </form>
      {status.message && (
        <div className={`status-message status-${status.type}`}>
          {status.message}
        </div>
      )}
    </div>
  );
};

export default CalculationPage;