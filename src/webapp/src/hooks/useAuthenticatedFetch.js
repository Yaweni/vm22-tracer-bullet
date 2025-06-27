import { useMsal } from "@azure/msal-react";
import { protectedApiRequest } from "../authConfig";

export const useAuthenticatedFetch = () => {
    const { instance, accounts } = useMsal();

    const authenticatedFetch = async (url, options = {}) => {
        const account = accounts[0];
        if (!account) {
            throw new Error("No active account! Please log in.");
        }

        const response = await instance.acquireTokenSilent({
            ...protectedApiRequest,
            account: account
        });

        const headers = new Headers(options.headers || {});
        headers.append('Authorization', `Bearer ${response.accessToken}`);

        const newOptions = { ...options, headers };

        return fetch(url, newOptions);
    };

    return authenticatedFetch;
};