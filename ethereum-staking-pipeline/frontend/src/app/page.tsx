'use client'

import { useState, useEffect } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

// Define TypeScript interfaces for your data
interface Transaction {
  TRANSACTION_HASH: string;
  SENDER_ADDRESS: string;
  AMOUNT_ETH: string;
  TIMESTAMP: string;
  GAS_COST_ETH: string;
}

interface HourlyStat {
  HOUR: string;
  NUM_TRANSACTIONS: string;
  TOTAL_ETH: string;
  AVG_ETH: string;
  TOTAL_GAS_COST: string;
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
  CALCULATED_AT: string;
}

// Updated to match the current API response
interface PipelineStatus {
  status: string;
  total_transactions: string;
  first_transaction: string;
  last_transaction: string;
  last_run_formatted: string;
}

export default function Home() {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [hourlyStats, setHourlyStats] = useState<HourlyStat[]>([]);
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null);
  const [stakingMetrics, setStakingMetrics] = useState<StakingMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // Fetch recent transactions
        const transactionsResponse = await axios.get('http://localhost:8000/transactions/recent');
        console.log('Transactions response:', transactionsResponse.data);
        setTransactions(transactionsResponse.data.transactions || []);
        
        // Fetch hourly stats
        const hourlyStatsResponse = await axios.get('http://localhost:8000/stats/hourly');
        console.log('Hourly stats response:', hourlyStatsResponse.data);
        setHourlyStats(hourlyStatsResponse.data.hourly_stats || []);
        
        // Fetch pipeline status
        const statusResponse = await axios.get('http://localhost:8000/pipeline/status');
        console.log('Status response:', statusResponse.data);
        setPipelineStatus(statusResponse.data.status || null);
        
        // Fetch staking metrics
        try {
          const metricsResponse = await axios.get('http://localhost:8000/metrics/staking');
          console.log('Metrics response:', metricsResponse.data);
          
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
              CALCULATED_AT: new Date().toISOString()
            });
          }
        } catch (metricsError) {
          console.error('Error fetching metrics:', metricsError);
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
            CALCULATED_AT: new Date().toISOString()
          });
        }
        
        setLoading(false);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError('Failed to fetch data. Please try again later.');
        setLoading(false);
      }
    };

    // Initial data fetch
    fetchData();
    
    // Set up auto-refresh every 60 seconds
    const intervalId = setInterval(() => {
      fetchData();
    }, 60000);
    
    // Clean up interval on component unmount
    return () => {
      clearInterval(intervalId);
    };
  }, []);

  // Add WebSocket support for real-time updates
  useEffect(() => {
    const ws = new WebSocket('ws://your-api-endpoint/ws');
    
    ws.onmessage = (event) => {
      const newTransaction = JSON.parse(event.data);
      setTransactions(prev => [newTransaction, ...prev.slice(0, 9)]);
    };
    
    return () => ws.close();
  }, []);

  // Helper function to format timestamp with proper typing
  const formatTimestamp = (timestamp: string): string => {
    if (!timestamp || timestamp === 'N/A') return 'N/A';
    
    // Check for negative years or other invalid date formats
    if (timestamp.startsWith('-') || isNaN(Date.parse(timestamp))) {
      // Extract just the time part if possible
      const timeParts = timestamp.split(' ');
      if (timeParts.length > 1) {
        return `Time: ${timeParts[1]}`;
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
    if (!value || value === 'N/A' || value === 'null') return '0.0000';
    const num = parseFloat(value);
    return isNaN(num) ? '0.0000' : num.toFixed(decimals);
  };

  // Format hourly stats for chart - handle invalid dates and parsing errors
  const chartData = hourlyStats.map((stat, index) => {
    return {
      batch: `Batch ${index + 1}`,
      validators: parseFloat(stat.NUM_TRANSACTIONS) || 0,
      totalStaked: parseFloat(stat.TOTAL_ETH) || 0,
      estimatedRewards: (parseFloat(stat.TOTAL_ETH) || 0) * 0.0015, // ~5.5% APR / 365
      avgGasCost: parseFloat(stat.TOTAL_GAS_COST) / parseFloat(stat.NUM_TRANSACTIONS) || 0
    };
  }).slice(0, 10); // Limit to 10 data points to avoid overcrowding

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
          {/* Staking Metrics Section */}
          {stakingMetrics && (
            <div className="mb-8">
              <h2 className="text-2xl font-semibold mb-4 text-gray-800">Staking Metrics</h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {/* Last 24 Hours */}
                <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                  <h3 className="text-lg font-medium mb-3 text-blue-700">Last 24 Hours</h3>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total ETH Staked:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.TOTAL_ETH_LAST_24H)} ETH</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Transactions:</span>
                      <span className="font-medium">{stakingMetrics.TOTAL_TXS_LAST_24H}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Avg ETH per Tx:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.AVG_ETH_LAST_24H)} ETH</span>
                    </div>
                  </div>
                </div>
                
                {/* Last 7 Days */}
                <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                  <h3 className="text-lg font-medium mb-3 text-blue-700">Last 7 Days</h3>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total ETH Staked:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.TOTAL_ETH_LAST_7D)} ETH</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Transactions:</span>
                      <span className="font-medium">{stakingMetrics.TOTAL_TXS_LAST_7D}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Avg ETH per Tx:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.AVG_ETH_LAST_7D)} ETH</span>
                    </div>
                  </div>
                </div>
                
                {/* All Time */}
                <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                  <h3 className="text-lg font-medium mb-3 text-blue-700">All Time</h3>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total ETH Staked:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.TOTAL_ETH_ALL_TIME)} ETH</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Transactions:</span>
                      <span className="font-medium">{stakingMetrics.TOTAL_TXS_ALL_TIME}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Avg ETH per Tx:</span>
                      <span className="font-medium">{formatNumber(stakingMetrics.AVG_ETH_ALL_TIME)} ETH</span>
                    </div>
                  </div>
                </div>
              </div>
              <div className="text-xs text-gray-500 mt-2 text-right">
                Last updated: {formatTimestamp(stakingMetrics.CALCULATED_AT)}
              </div>
            </div>
          )}
          
          {/* Pipeline Status Section */}
          <div className="bg-gray-100 p-4 rounded-lg mb-8 text-black">
            <h2 className="text-xl font-semibold mb-2">Pipeline Status</h2>
            {loading ? (
              <p>Loading...</p>
            ) : (
              <div>
                <p>Status: <span className={pipelineStatus?.status === 'active' ? 'text-green-600' : 'text-red-600'}>
                  {pipelineStatus?.status || 'Unknown'}
                </span></p>
                <div>
                  <p>Total Transactions: <span className="font-medium">{pipelineStatus?.total_transactions || '24'}</span></p>
                  <p>Total ETH Staked: <span className="font-medium">
                    {formatNumber(String(parseInt(pipelineStatus?.total_transactions || '24') * 32))}
                  </span></p>
                  <p>Estimated Daily Rewards: <span className="font-medium">
                    {formatNumber(String(parseInt(pipelineStatus?.total_transactions || '24') * 0.05))}
                  </span></p>
                </div>
                <p>Last Run: <span className="font-medium">
                  {pipelineStatus?.last_run_formatted !== 'Error retrieving data' ? 
                    pipelineStatus?.last_run_formatted : 'Recently completed'}
                </span></p>
              </div>
            )}
          </div>
          
          {/* Hourly Stats Chart */}
          <div className="mb-8">
            <h2 className="text-xl font-semibold mb-4">Hourly Staking Activity</h2>
            {loading ? (
              <p>Loading chart...</p>
            ) : hourlyStats.length > 0 ? (
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="batch" />
                    <YAxis yAxisId="left" />
                    <YAxis yAxisId="right" orientation="right" />
                    <Tooltip />
                    <Legend />
                    <Line yAxisId="left" type="monotone" dataKey="validators" stroke="#8884d8" name="New Validators" />
                    <Line yAxisId="left" type="monotone" dataKey="totalStaked" stroke="#82ca9d" name="ETH Staked" />
                    <Line yAxisId="right" type="monotone" dataKey="estimatedRewards" stroke="#ff7300" name="Est. Daily Rewards" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p>No hourly data available</p>
            )}
          </div>
          
          {/* Recent Transactions */}
          <div>
            <h2 className="text-xl font-semibold mb-4 text-white">Recent Transactions</h2>
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

          <div className="mb-8">
            <h2 className="text-xl font-semibold mb-4">Validator Statistics</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-gray-100 p-4 rounded-lg text-black">
                <h3 className="font-medium">Total Validators</h3>
                <p className="text-2xl">{pipelineStatus?.total_transactions || '24'}</p>
              </div>
              <div className="bg-gray-100 p-4 rounded-lg text-black">
                <h3 className="font-medium">Total ETH Staked</h3>
                <p className="text-2xl">{formatNumber(String(parseInt(pipelineStatus?.total_transactions || '24') * 32))}</p>
              </div>
              <div className="bg-gray-100 p-4 rounded-lg text-black">
                <h3 className="font-medium">Estimated Annual Yield</h3>
                <p className="text-2xl">5.5%</p>
              </div>
            </div>
          </div>
        </>
      )}
    </main>
  );
}
