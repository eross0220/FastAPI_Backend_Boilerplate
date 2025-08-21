'use client';

import React, { useState, useRef, useEffect } from 'react';
import { Activity, Filter, Trash2, Download, Info, CheckCircle, AlertTriangle, XCircle } from 'lucide-react';
import { Button } from './ui/Button';
import { Card, CardHeader, CardContent } from './ui/Card';

export interface LogEntry {
  id: string;
  timestamp: Date;
  message: string;
  type: 'info' | 'success' | 'warning' | 'error';
}

interface StreamLogsProps {
  logs: LogEntry[];
  onClearLogs: () => void;
}

export function StreamLogs({ logs, onClearLogs }: StreamLogsProps) {
  const [filter, setFilter] = useState<'all' | 'info' | 'success' | 'warning' | 'error'>('all');
  const [autoScroll, setAutoScroll] = useState(true);
  const logsEndRef = useRef<HTMLDivElement>(null);

  const filteredLogs = logs.filter(log => filter === 'all' || log.type === filter);

  useEffect(() => {
    if (autoScroll && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, autoScroll]);

  const getLogIcon = (type: LogEntry['type']) => {
    switch (type) {
      case 'info':
        return <Info className="w-4 h-4 text-blue-500" />;
      case 'success':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'warning':
        return <AlertTriangle className="w-4 h-4 text-yellow-500" />;
      case 'error':
        return <XCircle className="w-4 h-4 text-red-500" />;
    }
  };

  const formatTimestamp = (date: Date) => {
    return date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  const exportLogs = () => {
    const logText = filteredLogs
      .map(log => `[${formatTimestamp(log.timestamp)}] ${log.type.toUpperCase()}: ${log.message}`)
      .join('\n');
    
    const blob = new Blob([logText], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `logs-${new Date().toISOString().split('T')[0]}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getLogCount = (type: LogEntry['type']) => {
    return logs.filter(log => log.type === type).length;
  };

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Activity className="w-5 h-5 text-primary-600" />
            <h2 className="text-lg font-semibold text-gray-900">Stream Logs</h2>
            <span className="text-sm text-gray-500">({logs.length})</span>
          </div>
          <div className="flex items-center space-x-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={exportLogs}
              disabled={filteredLogs.length === 0}
            >
              <Download className="w-4 h-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClearLogs}
              disabled={logs.length === 0}
            >
              <Trash2 className="w-4 h-4" />
            </Button>
          </div>
        </div>
      </CardHeader>
      
      <CardContent className="flex-1 flex flex-col">
        {/* Filter Controls */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-2">
            <Filter className="w-4 h-4 text-gray-500" />
            <span className="text-sm font-medium text-gray-700">Filter:</span>
            <div className="flex space-x-1">
              {(['all', 'info', 'success', 'warning', 'error'] as const).map((type) => (
                <button
                  key={type}
                  onClick={() => setFilter(type)}
                  className={`px-3 py-1 text-xs rounded-full transition-colors ${
                    filter === type
                      ? 'bg-primary-100 text-primary-700 font-medium'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {type === 'all' ? 'All' : type.charAt(0).toUpperCase() + type.slice(1)}
                  {type !== 'all' && (
                    <span className="ml-1 text-xs">
                      ({getLogCount(type)})
                    </span>
                  )}
                </button>
              ))}
            </div>
          </div>
          
          <label className="flex items-center space-x-2 text-sm text-gray-600">
            <input
              type="checkbox"
              checked={autoScroll}
              onChange={(e) => setAutoScroll(e.target.checked)}
              className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
            />
            <span>Auto-scroll</span>
          </label>
        </div>

        {/* Logs Container */}
        <div className="flex-1 bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
          <div className="h-full overflow-y-auto p-4 space-y-2">
            {filteredLogs.length === 0 ? (
              <div className="flex items-center justify-center h-full text-gray-500">
                <div className="text-center">
                  <Activity className="w-8 h-8 mx-auto mb-2 text-gray-300" />
                  <p className="text-sm">No logs to display</p>
                  {filter !== 'all' && (
                    <p className="text-xs text-gray-400 mt-1">
                      Try changing the filter or upload a CSV file
                    </p>
                  )}
                </div>
              </div>
            ) : (
              filteredLogs.map((log) => (
                <div
                  key={log.id}
                  className={`log-entry ${log.type} rounded-lg animate-fade-in`}
                >
                  <div className="flex items-start space-x-3">
                    <div className="flex-shrink-0 mt-0.5">
                      {getLogIcon(log.type)}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <p className="text-sm text-gray-900">{log.message}</p>
                        <span className="text-xs text-gray-500 flex-shrink-0 ml-2">
                          {formatTimestamp(log.timestamp)}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            )}
            <div ref={logsEndRef} />
          </div>
        </div>

        {/* Status Bar */}
        <div className="mt-4 flex items-center justify-between text-xs text-gray-500">
          <div className="flex items-center space-x-4">
            <span>Showing {filteredLogs.length} of {logs.length} logs</span>
            {filter !== 'all' && (
              <span className="text-primary-600">
                Filtered by: {filter}
              </span>
            )}
          </div>
          <div className="flex items-center space-x-4">
            <span className="flex items-center space-x-1">
              <Info className="w-3 h-3 text-blue-500" />
              <span>{getLogCount('info')}</span>
            </span>
            <span className="flex items-center space-x-1">
              <CheckCircle className="w-3 h-3 text-green-500" />
              <span>{getLogCount('success')}</span>
            </span>
            <span className="flex items-center space-x-1">
              <AlertTriangle className="w-3 h-3 text-yellow-500" />
              <span>{getLogCount('warning')}</span>
            </span>
            <span className="flex items-center space-x-1">
              <XCircle className="w-3 h-3 text-red-500" />
              <span>{getLogCount('error')}</span>
            </span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
