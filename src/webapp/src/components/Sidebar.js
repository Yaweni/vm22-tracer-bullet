import React from 'react';
import { NavLink } from 'react-router-dom';
import { useMsal } from "@azure/msal-react";
import './Sidebar.css'; // Assuming you have this CSS file

const Sidebar = () => {
    const { instance } = useMsal();

    const handleLogout = () => {
        instance.logoutPopup({
            postLogoutRedirectUri: "/",
            mainWindowRedirectUri: "/"
        });
    };

    return (
        <nav className="sidebar">
            <div className="sidebar-header">
                <h3>Actuarial Hub</h3>
            </div>
            <ul>
                <li>
                    <NavLink to="/data">Data Management</NavLink>
                </li>
                <li>
                    <NavLink to="/calculation">Calculation Lab</NavLink>
                </li>
                <li>
                    <NavLink to="/jobs">Job History</NavLink>
                </li>
            </ul>
            <div className="sidebar-footer">
                <button onClick={handleLogout} className="logout-button">Sign Out</button>
            </div>
        </nav>
    );
};

export default Sidebar;