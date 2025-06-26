const express = require('express');
const path = require('path');
const app = express();

// Define the port the app will run on.
// Use the environment variable PORT if it's set (e.g., by a hosting service),
// or default to 3001 for local development.
const PORT = process.env.PORT || 3001;

// --- MIDDLEWARE --- //

// 1. Serve the static files from the React app's "build" directory
app.use(express.static(path.join(__dirname, 'build')));

// --- ROUTES --- //


// This is your future API route.
// Example: app.get('/api/users', (req, res) => { ... });


// 2. The "catchall" handler: for any request that doesn't match one above,
// send back React's index.html file.
// This is the key for Single-Page Applications (SPAs) with client-side routing.
app.get('/{*splat}', (req, res) => {res.sendFile( __dirname + "/build/" + "index.html" )});

// --- START THE SERVER --- //
app.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});