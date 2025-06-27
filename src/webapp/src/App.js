import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Header from './components/Header';
import IngestionPage from './pages/IngestionPage';
import CalculationPage from './pages/CalculationPage';
import JobsPage from './pages/JobsPage';
import ReportPage from './pages/ReportPage';
import './App.css'; // Make sure this CSS file is present and correct

function App() {
  return (
    <Router>
      <div className="app-layout">
        <Sidebar />
        <main className="main-content">
          <Header />
          <div className="page-container">
            <Routes>
              {/* This is the default route. It redirects from "/" to "/ingestion" */}
              <Route path="/" element={<Navigate to="/ingestion" replace />} />

              {/* Define the routes for each page */}
              <Route path="/ingestion" element={<IngestionPage />} />
              <Route path="/calculation" element={<CalculationPage />} />
              <Route path="/jobs" element={<JobsPage />} />
              <Route path="/reports/:jobId" element={<ReportPage />} />

              {/* A catch-all route that redirects any unknown URL to the main page */}
              <Route path="*" element={<Navigate to="/ingestion" replace />} />
            </Routes>
          </div>
        </main>
      </div>
    </Router>
  );
}

export default App;