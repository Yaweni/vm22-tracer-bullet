import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthenticatedTemplate, UnauthenticatedTemplate } from "@azure/msal-react";

import Sidebar from './components/Sidebar';
import DataManagementPage from './pages/DataManagementPage';
import PolicyDataGridPage from './pages/PolicyDataGridPage';
import CalculationLabPage from './pages/CalculationLabPage';
import JobsPage from './pages/JobsPage';
import ReportPage from './pages/ReportPage';
import Login from './pages/Login';
import './App.css'; 

function App() {
  return (
    <Router>
      <UnauthenticatedTemplate>
        <Login />
      </UnauthenticatedTemplate>

      <AuthenticatedTemplate>
        <div className="app-layout">
          <Sidebar />
          <main className="main-content">
            <div className="page-container">
              <Routes>
                <Route path="/" element={<Navigate to="/data" replace />} />
                <Route path="/data" element={<DataManagementPage />} />
                <Route path="/data/policies/:setId" element={<PolicyDataGridPage />} />
                <Route path="/calculation" element={<CalculationLabPage />} />
                <Route path="/jobs" element={<JobsPage />} />
                <Route path="/reports/:jobId" element={<ReportPage />} />
                <Route path="*" element={<Navigate to="/data" replace />} />
              </Routes>
            </div>
          </main>
        </div>
      </AuthenticatedTemplate>
    </Router>
  );
}

export default App;