import { LogLevel } from "@azure/msal-browser";

// Single, unified MSAL configuration for your Customer Application
export const msalConfig = {
    auth: {
        // This is the Application (client) ID from your app registration
        // in your "vm22actuarialhub2" customer tenant.
        clientId: "6e7d3f60-b9ab-4654-9fb4-c54769bf1527",

        // This is the authority URL for Entra ID for customers (CIAM).
        // It's your customer tenant's subdomain with "ciamlogin.com".
        // Notice it does NOT contain the policy name.
        authority: "https://vm22actuarialhub2.ciamlogin.com/",

        // The Redirect URI must exactly match what you entered in the
        // app registration in the Azure portal.
        redirectUri: "http://localhost:3000",

        // This is recommended for CIAM scenarios to ensure MSAL recognizes the authority.
        knownAuthorities: ["vm22actuarialhub2.ciamlogin.com"],

        // Optional: If you have a separate logout page. If not, can be same as redirectUri.
        postLogoutRedirectUri: "http://localhost:3000/logout",
    },
    cache: {
        cacheLocation: "sessionStorage", // "sessionStorage" is good for security, "localStorage" for persistence.
        storeAuthStateInCookie: false,
    },
    system: {
        loggerOptions: {
            // Optional: for debugging during development
            loggerCallback: (level, message, containsPii) => {
                if (containsPii) { return; }
                switch (level) {
                    case LogLevel.Error: console.error(message); return;
                    case LogLevel.Info: console.info(message); return;
                    case LogLevel.Verbose: console.log(message); return;
                    case LogLevel.Warning: console.warn(message); return;
                    default: return;
                }
            }
        }
    }
};

// Define the scopes for the ID token. These are standard.
export const loginRequest = {
    scopes: ["openid", "profile", "offline_access"]
};

// Define the scopes needed to call your Azure Function backend.
// You need to configure this in Azure first.
export const protectedApiRequest = {
    scopes: ["api://6e7d3f60-b9ab-4654-9fb4-c54769bf1527/user_impersonation"]
};