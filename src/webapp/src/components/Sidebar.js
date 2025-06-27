import React from 'react';
import { NavLink } from 'react-router-dom';
import './Sidebar.css'; // We'll create this for styling

const Sidebar = () => {
  return (
    <nav className="sidebar">
      <ul>
        <li>
          <NavLink to="/ingestion">1. Data Ingestion</NavLink>
        </li>
        <li>
          <NavLink to="/calculation">2. Run Calculation</NavLink>
        </li>
        <li>
          <NavLink to="/jobs">3. View Jobs</NavLink>
        </li>
        {/* Add more links here later, e.g., for Assumptions */}
      </ul>
    </nav>
  );
};

export default Sidebar;