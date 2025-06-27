import React from 'react';
import { useMsal } from "@azure/msal-react";
// CHANGE: Only import loginRequest, as we no longer switch configs
import { loginRequest } from "../authConfig";

const Login = () => {
    const { instance } = useMsal();

    // CHANGE: The handler is now extremely simple.
    const handleLogin = () => {
        instance.loginPopup(loginRequest).catch(e => {
            console.error("Login failed:", e);
        });
    };

    return (
        <div className="login-container">
            <div className="login-box">
                <h1>Integrated Actuarial Hub</h1>
                <p>Sign in to your account to continue.</p>
                {/* CHANGE: Replaced two buttons with one */}
                <button onClick={handleLogin} className="login-button">
                    Sign In / Sign Up
                </button>
            </div>
        </div>
    );
};

export default Login;
