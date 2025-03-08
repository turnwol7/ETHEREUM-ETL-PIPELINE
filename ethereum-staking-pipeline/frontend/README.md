# Frontend

This directory contains a Next.js frontend for visualizing Ethereum staking data.

## Features

- Recent transactions table
- Hourly staking activity chart
- Pipeline status display

## Setup

1. Install dependencies:
   ```
   npm install
   ```

2. Create a `.env.local` file with your API URL:
   ```
   NEXT_PUBLIC_API_URL=http://localhost:8000
   ```

3. Run the development server:
   ```
   npm run dev
   ```

4. Open http://localhost:3000 to view the dashboard

## Deployment

1. Push to GitHub
2. Connect to Vercel
3. Set the `NEXT_PUBLIC_API_URL` environment variable to your deployed API URL
