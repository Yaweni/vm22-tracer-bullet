import React from 'react';
import ReactDOM from 'react-dom/client';
import { PublicClientApplication } from "@azure/msal-browser";
import { MsalProvider } from "@azure/msal-react";
// CHANGE: Import the single msalConfig object
import { msalConfig } from "./authConfig";
import App from './App';
import './index.css';
import 'ag-grid-community/styles/ag-grid.css'; // Core grid CSS
import 'ag-grid-community/styles/ag-theme-alpine.css'; // Theme

// CHANGE: Initialize MSAL instance with the single, unified config
const msalInstance = new PublicClientApplication(msalConfig);

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <MsalProvider instance={msalInstance}>
      <App />
    </MsalProvider>
  </React.StrictMode>
);