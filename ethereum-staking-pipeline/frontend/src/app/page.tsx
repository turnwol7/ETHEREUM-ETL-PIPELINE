'use client'

import { useState, useEffect } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

// Define TypeScript interfaces for your data
interface Transaction {
  TRANSACTION_HASH: string;
  SENDER_ADDRESS: string;
  AMOUNT_ETH: string | number;
  TIMESTAMP: string;
  GAS_COST_ETH: string | number;
}

interface HourlyStat {
  HOUR: string;
  NUM_TRANSACTIONS: number;
  TOTAL_ETH: string | number;
  AVG_ETH: string | number;
  TOTAL_GAS_COST: string | number;
}

interface PipelineStatus {
  last_run?: string;
  status?: string;
  minutes_since_last_run?: number;
}

export default function Home() {
  // Use the defined interfaces for your state
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [hourlyStats, setHourlyStats] = useState<HourlyStat[]>([]);
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        // Replace with your API URL
        const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
        
        const [txResponse, statsResponse, statusResponse] = await Promise.all([
          axios.get(`${apiUrl}/recent-transactions`),
          axios.get(`${apiUrl}/hourly-stats`),
          axios.get(`${apiUrl}/pipeline-status`)
        ]);
        
        setTransactions(txResponse.data.transactions || []);
        setHourlyStats(statsResponse.data.hourly_stats || []);
        setPipelineStatus(statusResponse.data || {});
      } catch (error) {
        console.error('Error fetching data:', error);
      }
      setLoading(false);
    };

    fetchData();
    
    // Refresh data every 5 minutes
    const interval = setInterval(fetchData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  // Format hourly stats for chart
  const chartData = hourlyStats.map(stat => ({
    hour: new Date(stat.HOUR).toLocaleTimeString(),
    avgEth: parseFloat(stat.AVG_ETH as string),
    totalEth: parseFloat(stat.TOTAL_ETH as string)
  })).reverse();

  return (
    <div className="container mx-auto px-4 py-8">
      {/* Next.js App Router doesn't use Head component directly */}
      {/* Instead, use metadata export or a layout file */}
      
      <h1 className="text-3xl font-bold mb-8">Ethereum Staking Dashboard</h1>
      
      {/* Pipeline Status */}
      <div className="bg-gray-100 p-4 rounded-lg mb-8">
        <h2 className="text-xl font-semibold mb-2">Pipeline Status</h2>
        {loading ? (
          <p>Loading...</p>
        ) : (
          <div>
            <p>Last Run: {pipelineStatus.last_run ? new Date(pipelineStatus.last_run).toLocaleString() : 'Unknown'}</p>
            <p>Status: <span className={pipelineStatus.status === 'success' ? 'text-green-600' : 'text-red-600'}>
              {pipelineStatus.status || 'Unknown'}
            </span></p>
            <p>Minutes Since Last Run: {pipelineStatus.minutes_since_last_run?.toFixed(2) || 'Unknown'}</p>
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
                <XAxis dataKey="hour" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="avgEth" stroke="#8884d8" name="Avg ETH Staked" />
                <Line type="monotone" dataKey="totalEth" stroke="#82ca9d" name="Total ETH Staked" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p>No hourly data available</p>
        )}
      </div>
      
      {/* Recent Transactions */}
      <div>
        <h2 className="text-xl font-semibold mb-4">Recent Transactions</h2>
        {loading ? (
          <p>Loading transactions...</p>
        ) : transactions.length > 0 ? (
          <div className="overflow-x-auto">
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
                    <td className="py-2 px-4 border-b">{parseFloat(tx.AMOUNT_ETH as string).toFixed(4)}</td>
                    <td className="py-2 px-4 border-b">{new Date(tx.TIMESTAMP).toLocaleString()}</td>
                    <td className="py-2 px-4 border-b">{parseFloat(tx.GAS_COST_ETH as string).toFixed(6)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p>No transactions available</p>
        )}
      </div>
    </div>
  );
}
