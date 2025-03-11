'use client'

import { useState, useEffect } from 'react';
import axios from 'axios';
// import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

// Define TypeScript interfaces for your data
interface Transaction {
  TRANSACTION_HASH: string;
  SENDER_ADDRESS: string;
  AMOUNT_ETH: string;
  TIMESTAMP: string;
  GAS_COST_ETH: string;
}

// New interface for staking metrics
interface StakingMetrics {
  TOTAL_ETH_LAST_24H: string;
  TOTAL_TXS_LAST_24H: string;
  AVG_ETH_LAST_24H: string;
  TOTAL_ETH_LAST_7D: string;
  TOTAL_TXS_LAST_7D: string;
  AVG_ETH_LAST_7D: string;
  TOTAL_ETH_ALL_TIME: string;
  TOTAL_TXS_ALL_TIME: string;
  AVG_ETH_ALL_TIME: string;
  TOTAL_ETH_LAST_HOUR: string;
  TOTAL_TXS_LAST_HOUR: string;
  CALCULATED_AT: string;
}

export default function Home() {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [stakingMetrics, setStakingMetrics] = useState<StakingMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // Fetch recent transactions
        const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
        const transactionsResponse = await axios.get(`${apiUrl}/transactions/recent`);
        setTransactions(transactionsResponse.data.transactions || []);
        
        // Fetch staking metrics
        try {
          const metricsResponse = await axios.get('http://localhost:8000/metrics/staking');
          
          
          // Ensure we have valid metrics data
          if (metricsResponse.data && metricsResponse.data.metrics) {
            // Set default values for any missing metrics
            const metrics = {
              TOTAL_ETH_LAST_24H: '0',
              TOTAL_TXS_LAST_24H: '0',
              AVG_ETH_LAST_24H: '0',
              TOTAL_ETH_LAST_7D: '0',
              TOTAL_TXS_LAST_7D: '0',
              AVG_ETH_LAST_7D: '0',
              TOTAL_ETH_ALL_TIME: '0',
              TOTAL_TXS_ALL_TIME: '0',
              AVG_ETH_ALL_TIME: '0',
              TOTAL_ETH_LAST_HOUR: '0',
              TOTAL_TXS_LAST_HOUR: '0',
              CALCULATED_AT: new Date().toISOString(),
              ...metricsResponse.data.metrics
            };
            setStakingMetrics(metrics);
          } else {
            // Set default metrics if the response is empty
            setStakingMetrics({
              TOTAL_ETH_LAST_24H: '0',
              TOTAL_TXS_LAST_24H: '0',
              AVG_ETH_LAST_24H: '0',
              TOTAL_ETH_LAST_7D: '0',
              TOTAL_TXS_LAST_7D: '0',
              AVG_ETH_LAST_7D: '0',
              TOTAL_ETH_ALL_TIME: '0',
              TOTAL_TXS_ALL_TIME: '0',
              AVG_ETH_ALL_TIME: '0',
              TOTAL_ETH_LAST_HOUR: '0',
              TOTAL_TXS_LAST_HOUR: '0',
              CALCULATED_AT: new Date().toISOString()
            });
          }
        } catch {
          // Set default metrics if there's an error
          setStakingMetrics({
            TOTAL_ETH_LAST_24H: '0',
            TOTAL_TXS_LAST_24H: '0',
            AVG_ETH_LAST_24H: '0',
            TOTAL_ETH_LAST_7D: '0',
            TOTAL_TXS_LAST_7D: '0',
            AVG_ETH_LAST_7D: '0',
            TOTAL_ETH_ALL_TIME: '0',
            TOTAL_TXS_ALL_TIME: '0',
            AVG_ETH_ALL_TIME: '0',
            TOTAL_ETH_LAST_HOUR: '0',
            TOTAL_TXS_LAST_HOUR: '0',
            CALCULATED_AT: new Date().toISOString()
          });
        }
        
        setLoading(false);
      } catch {
        setError('Failed to fetch data. Please try again later.');
        setLoading(false);
      }
    };

    // Initial data fetch
    fetchData();
    
    
    // const intervalId = setInterval(() => {
    //   fetchData();
    // }, 1200000);
    
    // return () => {
    //   clearInterval(intervalId);
    // };
    
    // No cleanup needed since we're not setting up an interval
    return () => {};
  }, []);

  

  // Add WebSocket support for real-time updates
  // useEffect(() => {
  //   const ws = new WebSocket('ws://your-api-endpoint/ws');
    
  //   ws.onmessage = (event) => {
  //     const newTransaction = JSON.parse(event.data);
  //     setTransactions(prev => [newTransaction, ...prev.slice(0, 9)]);
  //   };
    
  //   return () => ws.close();
  // }, []);

  // Helper function to format timestamp with proper typing
  const formatTimestamp = (timestamp: string): string => {
    if (!timestamp || timestamp === 'N/A') return 'N/A';
    
    // Check for negative years or other invalid date formats
    if (timestamp.startsWith('-') || isNaN(Date.parse(timestamp))) {
      // Extract just the time part if possible
      const timeParts = timestamp.split(' ');
      if (timeParts.length > 1) {
        // Try to create a more complete timestamp
        const today = new Date();
        return `${today.toLocaleDateString()} ${timeParts[1]}`;
      }
      return 'Recent';
    }
    
    try {
      return new Date(timestamp).toLocaleString();
    } catch {
      return 'Recent';
    }
  };

  // Helper function to format number with proper typing
  const formatNumber = (value: string | undefined, decimals = 4): string => {
    if (!value || value === 'N/A' || value === 'null' || value === 'undefined') return '0.0000';
    const num = parseFloat(value);
    return isNaN(num) ? '0.0000' : num.toFixed(decimals);
  };

  // // Format hourly stats for chart - handle invalid dates and parsing errors
  // const chartData = hourlyStats.map((stat, index) => {
  //   return {
  //     batch: `Batch ${index + 1}`,
  //     validators: parseFloat(stat.NUM_TRANSACTIONS) || 0,
  //     totalStaked: parseFloat(stat.TOTAL_ETH) || 0,
  //     estimatedRewards: (parseFloat(stat.TOTAL_ETH) || 0) * 0.0015, // ~5.5% APR / 365
  //     avgGasCost: parseFloat(stat.TOTAL_GAS_COST) / parseFloat(stat.NUM_TRANSACTIONS) || 0
  //   };
  // }).slice(0, 10); // Limit to 10 data points to avoid overcrowding

  if (loading) return <div className="container mx-auto p-4"><p>Loading...</p></div>;
  if (error) return <div className="container mx-auto p-4"><p className="text-red-500">{error}</p></div>;

  return (
    <main className="min-h-screen p-8 bg-gray-50">
      <h1 className="text-3xl font-bold mb-6 text-blue-800">Ethereum Staking Dashboard</h1>
      
      {loading ? (
        <div className="flex justify-center items-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
        </div>
      ) : error ? (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
        </div>
      ) : (
        <>
          {/* Staking Metrics Section - Top metrics side by side */}
          <div className="mb-8">
            <h2 className="text-2xl font-semibold mb-4 text-gray-800">Staking Metrics</h2>
            
            {/* Grid for side-by-side metrics */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* ETH Staked in Last Hour */}
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-medium text-gray-600 mb-2">ETH Staked in Last Hour</h3>
                <div className="flex items-center">
                  <span className="text-4xl font-bold text-blue-700">
                    {stakingMetrics ? stakingMetrics.TOTAL_ETH_LAST_HOUR || '0' : '0'}
                  </span>
                  <span className="text-xl ml-2 text-gray-600">ETH</span>
                </div>
              </div>

              {/* Transactions in Last Hour */}
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-medium text-gray-600 mb-2">Transactions in Last Hour</h3>
                <div className="flex items-center">
                  <span className="text-4xl font-bold text-blue-700">
                    {stakingMetrics ? stakingMetrics.TOTAL_TXS_LAST_HOUR || '0' : '0'}
                  </span>
                  <span className="text-xl ml-2 text-gray-600">Transactions</span>
                </div>
              </div>
            </div>
            
            {/* Last updated timestamp */}
            <div className="text-xs text-gray-500 mt-2 text-right">
              Last updated: {stakingMetrics ? formatTimestamp(stakingMetrics.CALCULATED_AT) : new Date().toLocaleString()}
            </div>
          </div>
          
          {/* Recent Transactions */}
          <div>
            <h2 className="text-xl font-semibold mb-4 text-black">Recent Transactions</h2>
            {loading ? (
              <p>Loading transactions...</p>
            ) : transactions.length > 0 ? (
              <div className="overflow-x-auto text-black">
                <table className="min-w-full bg-white">
                  <thead>
                    <tr>
                      <th className="py-2 px-4 border-b">Transaction Hash</th>
                      <th className="py-2 px-4 border-b">Sender</th>
                      <th className="py-2 px-4 border-b">Amount (ETH)</th>
                      <th className="py-2 px-4 border-b">Timestamp</th>
                      <th className="py-2 px-4 border-b">Gas Cost (ETH)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {transactions.map((tx, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                        <td className="py-2 px-4 border-b">{tx.TRANSACTION_HASH?.substring(0, 10)}...</td>
                        <td className="py-2 px-4 border-b">{tx.SENDER_ADDRESS?.substring(0, 10)}...</td>
                        <td className="py-2 px-4 border-b">{formatNumber(tx.AMOUNT_ETH)}</td>
                        <td className="py-2 px-4 border-b">{formatTimestamp(tx.TIMESTAMP)}</td>
                        <td className="py-2 px-4 border-b">{formatNumber(tx.GAS_COST_ETH, 6)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p>No transactions available</p>
            )}
          </div>
        </>
      )}
    </main>
  );
}
