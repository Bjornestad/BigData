import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine
} from 'recharts';
import './App.css';

function App() {
  const [data, setData] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState('connecting');
  const [stats, setStats] = useState({
    currentActual: null,
    currentPredicted: null,
    avgActual: null,
    avgPredicted: null,
    variance: null
  });
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);

  const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'ws://localhost:8080';

  const connectWebSocket = useCallback(() => {
    try {
      const ws = new WebSocket(BACKEND_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log('WebSocket connected');
        setConnectionStatus('connected');
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
        }
      };

      ws.onmessage = (event) => {
        const message = JSON.parse(event.data);

        if (message.type === 'initial') {
          // Process initial data
          const combined = {};
          
          message.actual.forEach(point => {
            const timestamp = new Date(point.timestamp).getTime();
            if (!combined[timestamp]) {
              combined[timestamp] = { timestamp };
            }
            combined[timestamp].actual = point.value;
          });

          message.predicted.forEach(point => {
            const timestamp = new Date(point.timestamp).getTime();
            if (!combined[timestamp]) {
              combined[timestamp] = { timestamp };
            }
            combined[timestamp].predicted = point.value;
          });

          const sortedData = Object.values(combined).sort((a, b) => a.timestamp - b.timestamp);
          setData(sortedData);
        } else if (message.type === 'actual' || message.type === 'predicted') {
          // Add new data point
          const point = message.data;
          const timestamp = new Date(point.timestamp).getTime();

          setData(prevData => {
            const newData = [...prevData];
            const existingIndex = newData.findIndex(d => d.timestamp === timestamp);

            if (existingIndex >= 0) {
              newData[existingIndex] = {
                ...newData[existingIndex],
                [message.type === 'actual' ? 'actual' : 'predicted']: point.value
              };
            } else {
              newData.push({
                timestamp,
                [message.type === 'actual' ? 'actual' : 'predicted']: point.value
              });
            }

            // Keep only last 100 points
            return newData.sort((a, b) => a.timestamp - b.timestamp).slice(-100);
          });
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        setConnectionStatus('error');
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected');
        setConnectionStatus('disconnected');
        wsRef.current = null;
        
        // Attempt to reconnect after 3 seconds
        reconnectTimeoutRef.current = setTimeout(() => {
          console.log('Attempting to reconnect...');
          setConnectionStatus('reconnecting');
          connectWebSocket();
        }, 3000);
      };
    } catch (error) {
      console.error('Error connecting to WebSocket:', error);
      setConnectionStatus('error');
    }
  }, [BACKEND_URL]);

  useEffect(() => {
    connectWebSocket();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [connectWebSocket]);

  // Calculate statistics
  useEffect(() => {
    if (data.length > 0) {
      const actualValues = data.filter(d => d.actual !== undefined).map(d => d.actual);
      const predictedValues = data.filter(d => d.predicted !== undefined).map(d => d.predicted);
      
      const currentActual = actualValues.length > 0 ? actualValues[actualValues.length - 1] : null;
      const currentPredicted = predictedValues.length > 0 ? predictedValues[predictedValues.length - 1] : null;
      
      const avgActual = actualValues.length > 0 
        ? actualValues.reduce((a, b) => a + b, 0) / actualValues.length 
        : null;
      
      const avgPredicted = predictedValues.length > 0
        ? predictedValues.reduce((a, b) => a + b, 0) / predictedValues.length
        : null;
      
      const variance = currentActual && currentPredicted
        ? ((currentPredicted - currentActual) / currentActual * 100).toFixed(1)
        : null;

      setStats({
        currentActual,
        currentPredicted,
        avgActual,
        avgPredicted,
        variance
      });
    }
  }, [data]);

  const formatXAxis = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit',
      hour12: false 
    });
  };

  const formatTooltip = (value, name) => {
    const label = name === 'actual' ? 'Actual' : 'Predicted';
    return [`${value.toFixed(2)} MW`, label];
  };

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const timestamp = payload[0].payload.timestamp;
      const date = new Date(timestamp);
      
      return (
        <div className="custom-tooltip">
          <p className="tooltip-time">{date.toLocaleString()}</p>
          {payload.map((entry, index) => (
            <p key={index} className="tooltip-value" style={{ color: entry.color }}>
              {entry.name === 'actual' ? 'Actual' : 'Predicted'}: {entry.value.toFixed(2)} MW
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const now = Date.now();
  const hasData = data.length > 0;

  return (
    <div className="App">
      <div className="grid-lines"></div>
      <div className="noise-overlay"></div>
      
      <header className="app-header">
        <div className="header-content">
          <div className="logo-section">
            <div className="logo-icon">⚡</div>
            <div>
              <h1>POWER GRID MONITOR</h1>
              <p className="subtitle">Real-Time Energy Consumption Analysis</p>
            </div>
          </div>
          
          <div className="connection-status">
            <div className={`status-indicator ${connectionStatus}`}></div>
            <span className="status-text">
              {connectionStatus === 'connected' ? 'LIVE' :
               connectionStatus === 'connecting' ? 'CONNECTING' :
               connectionStatus === 'reconnecting' ? 'RECONNECTING' :
               connectionStatus === 'error' ? 'ERROR' : 'OFFLINE'}
            </span>
          </div>
        </div>
      </header>

      <div className="stats-panel">
        <div className="stat-card actual">
          <div className="stat-label">CURRENT LOAD</div>
          <div className="stat-value">
            {stats.currentActual !== null ? `${stats.currentActual.toFixed(2)} MW` : '---'}
          </div>
          <div className="stat-sublabel">Actual Consumption</div>
        </div>

        <div className="stat-card predicted">
          <div className="stat-label">FORECAST</div>
          <div className="stat-value">
            {stats.currentPredicted !== null ? `${stats.currentPredicted.toFixed(2)} MW` : '---'}
          </div>
          <div className="stat-sublabel">Predicted Consumption</div>
        </div>

        <div className="stat-card variance">
          <div className="stat-label">VARIANCE</div>
          <div className="stat-value">
            {stats.variance !== null ? `${stats.variance > 0 ? '+' : ''}${stats.variance}%` : '---'}
          </div>
          <div className="stat-sublabel">Prediction Accuracy</div>
        </div>

        <div className="stat-card average">
          <div className="stat-label">AVERAGE</div>
          <div className="stat-value">
            {stats.avgActual !== null ? `${stats.avgActual.toFixed(2)} MW` : '---'}
          </div>
          <div className="stat-sublabel">Mean Actual Load</div>
        </div>
      </div>

      <div className="chart-container">
        <div className="chart-header">
          <h2>Consumption Timeline</h2>
          <div className="legend-custom">
            <div className="legend-item">
              <div className="legend-line actual"></div>
              <span>Actual</span>
            </div>
            <div className="legend-item">
              <div className="legend-line predicted"></div>
              <span>Predicted</span>
            </div>
          </div>
        </div>
        
        {hasData ? (
          <ResponsiveContainer width="100%" height={500}>
            <LineChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
              <defs>
                <linearGradient id="actualGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#00ff88" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#00ff88" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="predictedGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#b066ff" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#b066ff" stopOpacity={0} />
                </linearGradient>
                <filter id="glow">
                  <feGaussianBlur stdDeviation="2" result="coloredBlur"/>
                  <feMerge>
                    <feMergeNode in="coloredBlur"/>
                    <feMergeNode in="SourceGraphic"/>
                  </feMerge>
                </filter>
              </defs>
              
              <CartesianGrid strokeDasharray="3 3" stroke="#2a3f5f" opacity={0.3} />
              
              <XAxis 
                dataKey="timestamp" 
                tickFormatter={formatXAxis}
                stroke="#7a8ea8"
                style={{ fontSize: '12px', fontFamily: 'Rajdhani, sans-serif' }}
              />
              
              <YAxis 
                stroke="#7a8ea8"
                style={{ fontSize: '12px', fontFamily: 'Rajdhani, sans-serif' }}
                label={{ value: 'Power (MW)', angle: -90, position: 'insideLeft', style: { fill: '#7a8ea8' } }}
              />
              
              <Tooltip content={<CustomTooltip />} />
              
              <ReferenceLine 
                x={now} 
                stroke="#ff6b35" 
                strokeDasharray="5 5" 
                strokeWidth={2}
                label={{ value: 'NOW', position: 'top', fill: '#ff6b35', fontSize: 12 }}
              />
              
              <Line 
                type="monotone" 
                dataKey="actual" 
                stroke="#00ff88" 
                strokeWidth={3}
                dot={false}
                filter="url(#glow)"
                isAnimationActive={false}
              />
              
              <Line 
                type="monotone" 
                dataKey="predicted" 
                stroke="#b066ff" 
                strokeWidth={3}
                strokeDasharray="5 5"
                dot={false}
                filter="url(#glow)"
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="no-data">
            <div className="loading-spinner"></div>
            <p>Waiting for data stream...</p>
          </div>
        )}
      </div>

      <footer className="app-footer">
        <div className="footer-info">
          <span>Data Points: {data.length}</span>
          <span>•</span>
          <span>Update Rate: Real-time</span>
          <span>•</span>
          <span>System Status: {connectionStatus.toUpperCase()}</span>
        </div>
      </footer>
    </div>
  );
}

export default App;
