import React, { useState } from 'react';

const AVAILABLE_PRODUCTS = ['SPDA_G3', 'FIA_S&P_C8', 'VA_GLWB5'];

const ScenarioRunner = ({ onJobQueued }) => {
  const [selectedProducts, setSelectedProducts] = useState([]);
  const [calcStochastic, setCalcStochastic] = useState(false);
  const [includeAttribution, setIncludeAttribution] = useState(false);
  const [assumptionsText, setAssumptionsText] = useState('');
  const [assumptionsFile, setAssumptionsFile] = useState(null); // State for optional file upload
  const [isLoading, setIsLoading] = useState(false);
  const [status, setStatus] = useState({ message: '', type: '' });


  const handleProductChange = (e) => {
    const { value, checked } = e.target;
    if (checked) {
      setSelectedProducts(prev => [...prev, value]);
    } else {
      setSelectedProducts(prev => prev.filter(product => product !== value));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    setStatus({ message: 'Submitting job configuration...', type: 'loading' });

    const jobConfig = {
      product_codes: selectedProducts,
      calculate_stochastic: calcStochastic,
      perform_attribution: includeAttribution,
      assumptions_text: assumptionsText,
      assumptionsFile: assumptionsFile ? assumptionsFile.name : "", // Only send file name for now
    };
    // Note: A real implementation would use FormData to send both JSON and a file.
    // For this MVP, we are only sending the JSON configuration as specified.

    try {
      const response = await fetch('/calculate?', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(jobConfig),
      });

      if (response.status !== 202) {
        const errorText = await response.text();
        throw new Error(errorText || `Server responded with status ${response.status}`);
      }

      const result = await response.json();
      setStatus({ message: `Job ${result.job_id} successfully queued.`, type: 'success' });
      
      // Notify parent component (App.js) that a new job was created
      if (onJobQueued) {
        onJobQueued(result.job_id);
      }

    } catch (error) {
      console.error('Job submission error:', error);
      setStatus({ message: `Error: ${error.message}`, type: 'error' });
    } finally {
      setIsLoading(false);
    }
  };

  const isButtonDisabled = selectedProducts.length === 0 || isLoading;

  return (
    <div className="component-panel">
      <h2>2. Configure & Run Calculation Scenario</h2>
      <form onSubmit={handleSubmit}>
        <div className="checkbox-group">
          <span className="label-text">Select Product Blocks to Include:</span>
          {AVAILABLE_PRODUCTS.map(product => (
            <label key={product} className="checkbox-item">
              <input
                type="checkbox"
                value={product}
                checked={selectedProducts.includes(product)}
                onChange={handleProductChange}
              />
              {product}
            </label>
          ))}
        </div>

        <div className="checkbox-group">
          <span className="label-text">Calculation Options:</span>
           <label className="checkbox-item">
            <input
              type="checkbox"
              checked={calcStochastic}
              onChange={(e) => setCalcStochastic(e.target.checked)}
            />
            Calculate Stochastic Reserves for products that fail SERT? (Otherwise, just flag them)
          </label>
           <label className="checkbox-item">
            <input
              type="checkbox"
              checked={includeAttribution}
              onChange={(e) => setIncludeAttribution(e.target.checked)}
            />
            Include Attribution Analysis (Waterfall Chart) in report?
          </label>
        </div>
        
        <div>
          <label htmlFor="assumptions-text">Describe your dynamic assumption logic (e.g., for lapse rates):</label>
          <textarea
            id="assumptions-text"
            value={assumptionsText}
            onChange={(e) => setAssumptionsText(e.target.value)}
            placeholder="Double the base lapse rate if treasury > 5%..."
          />
        </div>

        <div>
           <label htmlFor="assumptions-file">Upload static assumption set (e.g., mortality table CSV):</label>
           <input
             id="assumptions-file"
             type="file"
             onChange={(e) => setAssumptionsFile(e.target.files[0])}
           />
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

export default ScenarioRunner;