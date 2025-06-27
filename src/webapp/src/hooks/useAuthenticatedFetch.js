import { useMsal } from "@azure/msal-react";
import { protectedApiRequest } from "../authConfig";

export const useAuthenticatedFetch = () => {
    const { instance, accounts } = useMsal();

    const authenticatedFetch = async (relativeUrl, options = {}) => {
        const account = accounts[0];
        if (!account) {
            throw new Error("No active account! Please log in.");
        }

        const response = await instance.acquireTokenSilent({
            ...protectedApiRequest,
            account: account
        });
        
        // ** CHANGE: Construct the full URL using the environment variable **
        const fullUrl = `${process.env.REACT_APP_API_BASE_URL}${relativeUrl}`;

        const headers = new Headers(options.headers || {});
        headers.append('Authorization', `Bearer ${response.accessToken}`);

        const newOptions = { ...options, headers };

        return fetch(fullUrl, newOptions);
    };

    return authenticatedFetch;
};