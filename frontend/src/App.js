import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine
} from 'recharts';
import './App.css';

function App() {
  const [data, setData] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState('connecting');
  const [dataSource, setDataSource] = useState('realtime');
  const [timeRange, setTimeRange] = useState('realtime');
  const [loading, setLoading] = useState(false);
  const [showCustomRangePicker, setShowCustomRangePicker] = useState(false);
  const [customStartDate, setCustomStartDate] = useState('');
  const [customEndDate, setCustomEndDate] = useState('');
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
  const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';

  // Fetch historical data from database
  const fetchHistoricalData = useCallback(async (range, customStart = null, customEnd = null) => {
    setLoading(true);
    setDataSource('historical');
    
    try {
      let startTime, endTime;
      
      if (range === 'custom' && customStart && customEnd) {
        startTime = new Date(customStart);
        endTime = new Date(customEnd);
      } else {
        endTime = new Date();
        
        switch (range) {
          case '1h':
            startTime = new Date(endTime - 60 * 60 * 1000);
            break;
          case '1d':
            startTime = new Date(endTime - 24 * 60 * 60 * 1000);
            break;
          case '1w':
            startTime = new Date(endTime - 7 * 24 * 60 * 60 * 1000);
            break;
          case '1m':
            startTime = new Date(endTime - 30 * 24 * 60 * 60 * 1000);
            break;
          default:
            startTime = new Date(endTime - 60 * 60 * 1000);
        }
      }
      
      const response = await fetch(`${API_URL}/api/historical`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          start: startTime.toISOString(),
          end: endTime.toISOString()
        })
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      const result = await response.json();
      
      // Transform database format to chart format
      const combined = {};
      
      result.data.forEach(point => {
        const timestamp = new Date(point.timestamp).getTime();
        if (!combined[timestamp]) {
          combined[timestamp] = { timestamp };
        }
        combined[timestamp][point.type] = point.value;
      });
      
      const sortedData = Object.values(combined).sort((a, b) => a.timestamp - b.timestamp);
      setData(sortedData);
      
      console.log(`Loaded ${sortedData.length} points from database (${result.metadata.interval} interval)`);
    } catch (error) {
      console.error('Error fetching historical data:', error);
      alert('Failed to load historical data. Is the database configured?');
    } finally {
      setLoading(false);
    }
  }, [API_URL]);

  // Connect to WebSocket for real-time data
  const connectWebSocket = useCallback(() => {
    try {
      const ws = new WebSocket(BACKEND_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log('WebSocket connected');
        setConnectionStatus('connected');
        setDataSource('realtime');
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
        }
      };

      ws.onmessage = (event) => {
        const message = JSON.parse(event.data);

        if (message.type === 'initial') {
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

  // Handle time range change
  const handleTimeRangeChange = (newRange) => {
    setTimeRange(newRange);
    
    if (newRange === 'realtime') {
      if (wsRef.current && wsRef.current.readyState !== WebSocket.OPEN) {
        connectWebSocket();
      }
      setDataSource('realtime');
    } else if (newRange === 'custom') {
      setShowCustomRangePicker(true);
    } else {
      fetchHistoricalData(newRange);
    }
  };

  // Handle custom range submission
  const handleCustomRangeSubmit = () => {
    if (!customStartDate || !customEndDate) {
      alert('Please select both start and end dates');
      return;
    }
    
    const start = new Date(customStartDate);
    const end = new Date(customEndDate);
    
    if (start >= end) {
      alert('Start date must be before end date');
      return;
    }
    
    setShowCustomRangePicker(false);
    fetchHistoricalData('custom', customStartDate, customEndDate);
  };

  useEffect(() => {
    if (timeRange === 'realtime') {
      connectWebSocket();
    }

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [connectWebSocket, timeRange]);

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
    
    if (timeRange === 'realtime' || timeRange === '1h') {
      return date.toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit',
        second: '2-digit',
        hour12: false 
      });
    } else if (timeRange === '1d') {
      return date.toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit',
        hour12: false 
      });
    } else {
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        hour: '2-digit'
      });
    }
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

  const hasData = data.length > 0;
  
  const getTimeRangeLabel = () => {
    if (timeRange === 'custom' && customStartDate && customEndDate) {
      return `${new Date(customStartDate).toLocaleDateString()} - ${new Date(customEndDate).toLocaleDateString()}`;
    }
    return timeRange.toUpperCase();
  };

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
              {dataSource === 'realtime' ? (
                connectionStatus === 'connected' ? 'LIVE' :
                connectionStatus === 'connecting' ? 'CONNECTING' :
                connectionStatus === 'reconnecting' ? 'RECONNECTING' :
                connectionStatus === 'error' ? 'ERROR' : 'OFFLINE'
              ) : (
                loading ? 'LOADING' : 'HISTORICAL'
              )}
            </span>
          </div>
        </div>
      </header>

      {/* Time Range Selector */}
      <div className="time-range-selector">
        <button 
          className={timeRange === 'realtime' ? 'active' : ''} 
          onClick={() => handleTimeRangeChange('realtime')}
        >
          LIVE
        </button>
        <button 
          className={timeRange === '1h' ? 'active' : ''} 
          onClick={() => handleTimeRangeChange('1h')}
        >
          1 HOUR
        </button>
        <button 
          className={timeRange === '1d' ? 'active' : ''} 
          onClick={() => handleTimeRangeChange('1d')}
        >
          1 DAY
        </button>
        <button 
          className={timeRange === '1w' ? 'active' : ''} 
          onClick={() => handleTimeRangeChange('1w')}
        >
          1 WEEK
        </button>
        <button 
          className={timeRange === '1m' ? 'active' : ''} 
          onClick={() => handleTimeRangeChange('1m')}
        >
          1 MONTH
        </button>
        <button 
          className={timeRange === 'custom' ? 'active custom-range-btn' : 'custom-range-btn'} 
          onClick={() => handleTimeRangeChange('custom')}
        >
          📅 CUSTOM RANGE
        </button>
      </div>

      {/* Custom Range Picker Modal */}
      {showCustomRangePicker && (
        <div className="modal-overlay" onClick={() => setShowCustomRangePicker(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Select Custom Date Range</h2>
              <button 
                className="modal-close" 
                onClick={() => setShowCustomRangePicker(false)}
              >
                ✕
              </button>
            </div>
            <div className="modal-body">
              <div className="date-picker-group">
                <label>
                  <span className="date-label">Start Date & Time</span>
                  <input 
                    type="datetime-local" 
                    value={customStartDate}
                    onChange={(e) => setCustomStartDate(e.target.value)}
                    max={customEndDate || undefined}
                  />
                </label>
                <label>
                  <span className="date-label">End Date & Time</span>
                  <input 
                    type="datetime-local" 
                    value={customEndDate}
                    onChange={(e) => setCustomEndDate(e.target.value)}
                    min={customStartDate || undefined}
                    max={new Date().toISOString().slice(0, 16)}
                  />
                </label>
              </div>
              <div className="quick-presets">
                <p className="presets-label">Quick Presets:</p>
                <button onClick={() => {
                  const end = new Date();
                  const start = new Date(end - 6 * 60 * 60 * 1000);
                  setCustomStartDate(start.toISOString().slice(0, 16));
                  setCustomEndDate(end.toISOString().slice(0, 16));
                }}>
                  Last 6 Hours
                </button>
                <button onClick={() => {
                  const end = new Date();
                  const start = new Date(end - 12 * 60 * 60 * 1000);
                  setCustomStartDate(start.toISOString().slice(0, 16));
                  setCustomEndDate(end.toISOString().slice(0, 16));
                }}>
                  Last 12 Hours
                </button>
                <button onClick={() => {
                  const end = new Date();
                  const start = new Date(end - 3 * 24 * 60 * 60 * 1000);
                  setCustomStartDate(start.toISOString().slice(0, 16));
                  setCustomEndDate(end.toISOString().slice(0, 16));
                }}>
                  Last 3 Days
                </button>
              </div>
            </div>
            <div className="modal-footer">
              <button 
                className="btn-secondary" 
                onClick={() => setShowCustomRangePicker(false)}
              >
                Cancel
              </button>
              <button 
                className="btn-primary" 
                onClick={handleCustomRangeSubmit}
              >
                Apply Range
              </button>
            </div>
          </div>
        </div>
      )}

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
          <h2>Consumption Timeline - {getTimeRangeLabel()}</h2>
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
        
        {loading ? (
          <div className="no-data">
            <div className="loading-spinner"></div>
            <p>Loading historical data...</p>
          </div>
        ) : hasData ? (
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
              
              {dataSource === 'realtime' && (
                <ReferenceLine 
                  x={Date.now()} 
                  stroke="#ff6b35" 
                  strokeDasharray="5 5" 
                  strokeWidth={2}
                  label={{ value: 'NOW', position: 'top', fill: '#ff6b35', fontSize: 12 }}
                />
              )}
              
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
          <span>Source: {dataSource === 'realtime' ? 'WebSocket (Backend Buffer)' : 'Database (TimescaleDB)'}</span>
          <span>•</span>
          <span>Status: {connectionStatus.toUpperCase()}</span>
        </div>
      </footer>

      {/* Debug Panel */}
      <div className="debug-panel">
        <h3>🔍 Debug Information</h3>
        <div className="debug-grid">
          <div className="debug-section">
            <h4>Data Overview</h4>
            <table>
              <tbody>
                <tr><td><strong>Total Points:</strong></td><td>{data.length}</td></tr>
                <tr><td><strong>Points with Actual:</strong></td><td>{data.filter(d => d.actual !== undefined).length}</td></tr>
                <tr><td><strong>Points with Predicted:</strong></td><td>{data.filter(d => d.predicted !== undefined).length}</td></tr>
                <tr><td><strong>Time Range:</strong></td><td>{timeRange}</td></tr>
                <tr><td><strong>Data Source:</strong></td><td>{dataSource}</td></tr>
                <tr><td><strong>Loading:</strong></td><td>{loading ? 'Yes' : 'No'}</td></tr>
              </tbody>
            </table>
          </div>

          <div className="debug-section">
            <h4>Time Range</h4>
            <table>
              <tbody>
                <tr>
                  <td><strong>First Point:</strong></td>
                  <td>{data.length > 0 ? new Date(data[0].timestamp).toLocaleString() : 'N/A'}</td>
                </tr>
                <tr>
                  <td><strong>Last Point:</strong></td>
                  <td>{data.length > 0 ? new Date(data[data.length - 1].timestamp).toLocaleString() : 'N/A'}</td>
                </tr>
                <tr>
                  <td><strong>Duration:</strong></td>
                  <td>
                    {data.length >= 2 
                      ? `${Math.round((data[data.length - 1].timestamp - data[0].timestamp) / (1000 * 60))} minutes`
                      : 'N/A'
                    }
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="debug-section">
            <h4>WebSocket Status</h4>
            <table>
              <tbody>
                <tr><td><strong>Connection:</strong></td><td>{connectionStatus}</td></tr>
                <tr><td><strong>Backend URL:</strong></td><td>{BACKEND_URL}</td></tr>
                <tr><td><strong>API URL:</strong></td><td>{API_URL}</td></tr>
                <tr>
                  <td><strong>Ready State:</strong></td>
                  <td>
                    {wsRef.current ? 
                      ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][wsRef.current.readyState] 
                      : 'Not initialized'
                    }
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="debug-section full-width">
            <h4>Sample Data (Last 5 Points)</h4>
            <div className="debug-code">
              <pre>{JSON.stringify(data.slice(-5), null, 2)}</pre>
            </div>
          </div>

          {dataSource === 'historical' && (
            <div className="debug-section full-width">
              <h4>Last API Request</h4>
              <div className="debug-code">
                <pre>{JSON.stringify({
                  timeRange,
                  customStartDate,
                  customEndDate,
                  endpoint: `${API_URL}/api/historical`
                }, null, 2)}</pre>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
