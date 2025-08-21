"use client";

import React, { useEffect, useState, useRef } from "react";
import {
  RefreshCw,
  Trash2,
  Play,
  Square,
  AlertCircle,
  CheckCircle,
  Info,
  XCircle,
} from "lucide-react";

interface KafkaLogEntry {
  timestamp: string;
  event_type: string;
  topic: string;
  pipeline_run_id?: number;
  pipeline_id?: number;
  block_run_id?: number;
  success?: boolean;
  message?: string;
  level?: "INFO" | "WARN" | "ERROR" | "DEBUG";
  raw_data?: any;
  [key: string]: any;
}

interface KafkaLogsProps {
  onPipelineCompleted?: (pipelineRunId: number) => void;
  onPipelineFailed?: (pipelineRunId: number) => void;
}

const WEBSOCKET_URL = "ws://localhost:8333/ws/logs";

const KafkaLogs: React.FC<KafkaLogsProps> = ({
  onPipelineCompleted,
  onPipelineFailed,
}) => {
  const [logs, setLogs] = useState<KafkaLogEntry[]>([]);
  const [processedEvents, setProcessedEvents] = useState<Set<string>>(
    new Set()
  );
  const [isConnected, setIsConnected] = useState(false);
  const [connectionError, setConnectionError] = useState<string>("");
  const [isStreaming, setIsStreaming] = useState(true);
  const wsRef = useRef<WebSocket | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const scrollToBottom = () => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [logs]);

  const addLog = (logData: KafkaLogEntry) => {
    // Create a unique key for this event
    const eventKey = `${logData.event_type}_${logData.timestamp}_${logData.pipeline_run_id}_${logData.block_run_id}`;

    // Check if we've already processed this event
    if (processedEvents.has(eventKey)) {
      console.log("Duplicate event detected, skipping:", eventKey);
      return;
    }

    // Add to processed events set
    setProcessedEvents((prev) => {
      const newSet = new Set(prev);
      newSet.add(eventKey);
      return newSet;
    });

    // Add to logs
    setLogs((prev) => [...prev, logData].slice(-500));
  };

  const connectWebSocket = () => {
    // Prevent multiple connections
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      console.log("WebSocket already connected, skipping...");
      return;
    }

    try {
      console.log("Connecting to Kafka log stream");
      const ws = new WebSocket(WEBSOCKET_URL);

      ws.onopen = () => {
        console.log("Connected to Kafka log stream");
        setIsConnected(true);
        setConnectionError("");
        setIsStreaming(true);

        // Clear any pending reconnect timeout
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
          reconnectTimeoutRef.current = null;
        }
      };

      ws.onmessage = (event) => {
        try {
          const logData: KafkaLogEntry = JSON.parse(event.data);

          // Check for duplicate logs by comparing with the last log
          setLogs((prev) => {
            if (prev.length > 0) {
              const lastLog = prev[prev.length - 1];
              // Check if this is a duplicate based on key fields
              if (
                lastLog.event_type === logData.event_type &&
                lastLog.timestamp === logData.timestamp &&
                lastLog.pipeline_run_id === logData.pipeline_run_id &&
                lastLog.block_run_id === logData.block_run_id
              ) {
                console.log("Duplicate log detected, skipping:", logData);
                return prev;
              }
            }
            return [...prev, logData].slice(-500);
          });

          // Call parent callbacks for relevant events
          if (logData.pipeline_run_id) {
            if (
              logData.event_type === "pipeline_completed" &&
              logData.success
            ) {
              console.log(
                "Pipeline completed, notifying parent:",
                logData.pipeline_run_id
              );
              onPipelineCompleted?.(logData.pipeline_run_id);
            } else if (logData.event_type === "pipeline_failed") {
              console.log(
                "Pipeline failed, notifying parent:",
                logData.pipeline_run_id
              );
              onPipelineFailed?.(logData.pipeline_run_id);
            }
          }
        } catch (error) {
          console.error("Error parsing log data:", error);
        }
      };

      ws.onclose = () => {
        console.log("Disconnected from Kafka log stream");
        setIsConnected(false);
        setIsStreaming(false);
        setConnectionError("Connection lost. Reconnecting...");

        // Only reconnect if streaming is still enabled and no timeout already set
        if (isStreaming && !reconnectTimeoutRef.current) {
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectTimeoutRef.current = null;
            if (isStreaming) {
              connectWebSocket();
            }
          }, 5000);
        }
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        setConnectionError("Connection error");
        setIsStreaming(false);
      };

      wsRef.current = ws;
    } catch (error) {
      console.error("Failed to connect:", error);
      setConnectionError("Failed to connect");
      setIsStreaming(false);
    }
  };

  const disconnectWebSocket = () => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    // Clear any pending reconnect timeout
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    setIsConnected(false);
    setIsStreaming(false);
  };

  useEffect(() => {
    connectWebSocket();
    return () => {
      disconnectWebSocket();
    };
  }, []);

  const getLogLevel = (log: KafkaLogEntry): string => {
    if (log.level) return log.level;
    if (log.success === false) return "ERROR";
    if (log.event_type?.includes("failed")) return "ERROR";
    if (log.event_type?.includes("completed")) return "INFO";
    if (log.event_type?.includes("started")) return "INFO";
    return "INFO";
  };

  const getLogIcon = (level: string) => {
    switch (level) {
      case "ERROR":
        return <XCircle className="w-4 h-4 text-red-500" />;
      case "WARN":
        return <AlertCircle className="w-4 h-4 text-yellow-500" />;
      case "INFO":
        return <Info className="w-4 h-4 text-blue-500" />;
      case "DEBUG":
        return <div className="w-4 h-4 bg-gray-300 rounded-full" />;
      default:
        return <Info className="w-4 h-4 text-blue-500" />;
    }
  };

  const getLogColor = (level: string) => {
    switch (level) {
      case "ERROR":
        return "border-l-red-500 bg-red-50";
      case "WARN":
        return "border-l-yellow-500 bg-yellow-50";
      case "INFO":
        return "border-l-blue-500 bg-blue-50";
      case "DEBUG":
        return "border-l-gray-500 bg-gray-50";
      default:
        return "border-l-blue-500 bg-blue-50";
    }
  };

  const formatTimestamp = (timestamp: string): string => {
    try {
      if (!timestamp) return "N/A";
      const dt = new Date(timestamp);
      return dt.toLocaleTimeString("en-US", {
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    } catch {
      return timestamp;
    }
  };

  const clearLogs = () => {
    setLogs([]);
  };

  const getEventTypeDisplay = (eventType: string | undefined): string => {
    if (!eventType) return "Unknown Event";
    return eventType
      .replace(/_/g, " ")
      .toLowerCase()
      .replace(/\b\w/g, (l) => l.toUpperCase());
  };

  const renderBeautifulLog = (log: KafkaLogEntry) => {
    const eventType = log.event_type;
    const timestamp = log.timestamp;
    const topic = log.topic;
    const pipelineRunId = log.pipeline_run_id;
    const pipelineId = log.pipeline_id;
    const blockRunId = log.block_run_id;
    const success = log.success;
    const level = getLogLevel(log);

    const timeStr = formatTimestamp(timestamp);

    if (eventType === "pipeline_started") {
      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-blue-600">
            🚀 [{timeStr}] Pipeline #{pipelineRunId} (ID: {pipelineId}) STARTED
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Pipeline: {pipelineRunId}
          </div>
        </div>
      );
    } else if (eventType === "pipeline_completed") {
      const status = success ? "✅ COMPLETED" : "❌ FAILED";
      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-green-600">
            🏁 [{timeStr}] Pipeline #{pipelineRunId} {status}
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Pipeline: {pipelineRunId}
          </div>
        </div>
      );
    } else if (eventType === "block_started") {
      const blockType = log.raw_data?.block_type || "unknown";
      const blockName = log.raw_data?.block_name || "unknown";
      const blockPurpose = log.raw_data?.block_purpose || "";

      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-purple-600">
            ⚡ [{timeStr}] Block #{blockRunId} ({blockType}) STARTED
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Block: {blockRunId} | Name: {blockName}
            {blockPurpose && ` | Purpose: ${blockPurpose}`}
          </div>
        </div>
      );
    } else if (eventType === "block_completed") {
      const blockType = log.raw_data?.block_type || "unknown";
      const blockName = log.raw_data?.block_name || "unknown";
      const blockPurpose = log.raw_data?.block_purpose || "";
      const status = success ? "✅ COMPLETED" : "❌ FAILED";

      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-green-600">
            🎯 [{timeStr}] Block #{blockRunId} ({blockType}) {status}
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Block: {blockRunId} | Name: {blockName}
            {blockPurpose && ` | Purpose: ${blockPurpose}`}
          </div>
        </div>
      );
    } else if (eventType === "block_failed") {
      const blockType = log.raw_data?.block_type || "unknown";
      const blockName = log.raw_data?.block_name || "unknown";
      const blockPurpose = log.raw_data?.block_purpose || "";

      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-red-600">
            💥 [{timeStr}] Block #{blockRunId} ({blockType}) FAILED
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Block: {blockRunId} | Name: {blockName}
            {blockPurpose && ` | Purpose: ${blockPurpose}`}
          </div>
        </div>
      );
    } else {
      return (
        <div className="space-y-1">
          <div className="text-lg font-semibold text-gray-600">
            [{timeStr}] {topic}: {eventType}
          </div>
          <div className="text-sm text-gray-600 ml-4">
            📍 Topic: {topic} | Event: {eventType}
          </div>
        </div>
      );
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-lg font-semibold text-gray-900">
          Real-Time Kafka Logs
        </h2>
        <div className="flex items-center space-x-3">
          <div
            className={`flex items-center space-x-2 px-3 py-1 rounded-full text-sm ${
              isConnected
                ? "bg-green-100 text-green-800"
                : "bg-red-100 text-red-800"
            }`}
          >
            <div
              className={`w-2 h-2 rounded-full ${
                isConnected ? "bg-green-500" : "bg-red-500"
              }`}
            />
            {isConnected ? "Connected" : "Disconnected"}
          </div>
          <button
            onClick={clearLogs}
            className="inline-flex items-center px-3 py-2 border border-gray-300 text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500"
          >
            <Trash2 className="w-4 h-4 mr-2" />
            Clear
          </button>
        </div>
      </div>

      {connectionError && (
        <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-md">
          <p className="text-sm text-red-600">{connectionError}</p>
        </div>
      )}

      <div className="bg-gray-900 rounded-lg p-4 h-[600px] overflow-y-auto">
        {logs.length === 0 ? (
          <div className="text-center text-gray-400 py-20">
            <Info className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>Waiting for Kafka logs...</p>
            <p className="text-sm">Start a pipeline to see real-time events</p>
          </div>
        ) : (
          <div className="space-y-4 font-mono text-sm">
            {logs.map((log, index) => {
              const level = getLogLevel(log);
              const colorClass = getLogColor(level);

              return (
                <div
                  key={index}
                  className={`p-4 rounded-lg border-l-4 ${colorClass} bg-white shadow-sm`}
                >
                  {renderBeautifulLog(log)}

                  {log.message && (
                    <div className="mt-2 text-sm text-gray-700 ml-4">
                      💬 {log.message}
                    </div>
                  )}

                  {log.raw_data && (
                    <details className="mt-2 ml-4">
                      <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-700">
                        🔍 Show raw data
                      </summary>
                      <pre className="mt-2 text-xs text-gray-600 bg-gray-50 p-2 rounded overflow-x-auto">
                        {JSON.stringify(log.raw_data, null, 2)}
                      </pre>
                    </details>
                  )}
                </div>
              );
            })}
            <div ref={logsEndRef} />
          </div>
        )}
      </div>

      <div className="mt-4 text-xs text-gray-500 text-center">
        {logs.length > 0 && (
          <span>Showing {logs.length} log entries • Auto-scroll enabled</span>
        )}
      </div>
    </div>
  );
};

export default KafkaLogs;
