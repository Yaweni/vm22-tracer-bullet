import React, { useState } from 'react';
import { useAuthenticatedFetch } from '../hooks/useAuthenticatedFetch';

const FileUpload = ({ label, getUploadUrlEndpoint }) => {
    const [selectedFile, setSelectedFile] = useState(null);
    const [status, setStatus] = useState({ message: '', type: '' });
    const [isLoading, setIsLoading] = useState(false);
    const authFetch = useAuthenticatedFetch();

    const handleFileChange = (e) => {
        setSelectedFile(e.target.files[0]);
        setStatus({ message: '', type: '' });
    };

    const handleUpload = async () => {
        if (!selectedFile) return;

        setIsLoading(true);
        setStatus({ message: 'Preparing secure upload...', type: 'loading' });

        try {
            // Step 1: Get the secure, one-time upload URL from our backend
            const response = await authFetch(`${getUploadUrlEndpoint}?fileName=${selectedFile.name}`);
            if (!response.ok) throw new Error('Could not get a secure upload URL.');
            
            const { uploadUrl } = await response.json();
            setStatus({ message: 'Uploading file...', type: 'loading' });

            // Step 2: Use the returned URL to upload the file directly to cloud storage
            // Note: We use the native fetch() here, not our authenticated hook,
            // because the URL is pre-signed and contains all necessary auth.
            // The method is often PUT for services like S3 or Azure Blob Storage.
            const uploadResponse = await fetch(uploadUrl, {
                method: 'PUT',
                headers: { 'Content-Type': selectedFile.type },
                body: selectedFile,
            });

            if (!uploadResponse.ok) throw new Error('File upload failed.');
            
            setStatus({ message: 'Upload successful! File is now being processed.', type: 'success' });
            setSelectedFile(null); // Clear file input
        } catch (error) {
            console.error('Upload process failed:', error);
            setStatus({ message: `Error: ${error.message}`, type: 'error' });
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div>
            <label htmlFor="file-input">{label}</label>
            <input
                id="file-input"
                type="file"
                onChange={handleFileChange}
                key={selectedFile ? 'file-selected' : 'no-file'} // Resets the input field
            />
            <button onClick={handleUpload} disabled={!selectedFile || isLoading}>
                {isLoading ? 'Uploading...' : 'Upload & Process File'}
            </button>
            {status.message && (
                <div className={`status-message status-${status.type}`} style={{marginTop: '10px'}}>
                    {status.message}
                </div>
            )}
        </div>
    );
};

export default FileUpload;