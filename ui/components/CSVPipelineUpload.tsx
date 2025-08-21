"use client";

import React, { useState, useEffect } from "react";
import {
  Upload,
  Play,
  FileText,
  CheckCircle,
  AlertCircle,
  Plus,
  Download,
  RefreshCw,
} from "lucide-react";

interface CSVPipelineUploadProps {
  onPipelineCreated?: (pipelineId: number) => void;
  completedPipelineRuns: Set<number>; // Add this prop
}

interface PipelineFile {
  block_name: string;
  filename: string;
  file_size: string;
  records_written: number;
  download_url: string;
}

interface PipelineRun {
  id: number;
  pipeline_id: number;
  status: "queued" | "running" | "completed" | "failed";
  started_at?: string;
  completed_at?: string;
}

const CSVPipelineUpload: React.FC<CSVPipelineUploadProps> = ({
  onPipelineCreated,
  completedPipelineRuns,
}) => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState<{
    type: "success" | "error" | null;
    message: string;
  }>({ type: null, message: "" });
  const [pipelineId, setPipelineId] = useState<number | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [hasUploaded, setHasUploaded] = useState(false);
  const [hasExecuted, setHasExecuted] = useState(false);

  // New state for pipeline files
  const [pipelineRunId, setPipelineRunId] = useState<number | null>(null);
  const [pipelineFiles, setPipelineFiles] = useState<PipelineFile[]>([]);
  const [isFetchingFiles, setIsFetchingFiles] = useState(false);
  const [filesError, setFilesError] = useState<string | null>(null);

  const API_BASE_URL = "http://localhost:8333/api/v1";

  // Fetch pipeline files when pipeline is completed
  const fetchPipelineFiles = async (runId: number) => {
    setIsFetchingFiles(true);
    setFilesError(null);

    try {
      const response = await fetch(
        `${API_BASE_URL}/downloads/pipeline/${runId}/files`
      );
      if (response.ok) {
        const data = await response.json();
        setPipelineFiles(data.files || []);
      } else {
        throw new Error("Failed to fetch pipeline files");
      }
    } catch (err) {
      setFilesError("Failed to fetch pipeline files");
      console.error("Error fetching pipeline files:", err);
    } finally {
      setIsFetchingFiles(false);
    }
  };

  // Download CSV file
  const downloadFile = async (filename: string) => {
    if (!pipelineRunId) return;

    try {
      const response = await fetch(
        `${API_BASE_URL}/downloads/pipeline/${pipelineRunId}/file/${filename}`,
        {
          method: "GET",
        }
      );

      if (response.ok) {
        // Create blob and download
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      } else {
        setUploadStatus({
          type: "error",
          message: "Failed to download file",
        });
      }
    } catch (err) {
      setUploadStatus({
        type: "error",
        message: "Failed to download file",
      });
      console.error("Error downloading file:", err);
    }
  };

  // Check pipeline run status and fetch files
  const checkPipelineStatus = async (runId: number) => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/pipelines/pipelines/${pipelineId}/runs`
      );
      if (response.ok) {
        const runs: PipelineRun[] = await response.json();
        const currentRun = runs.find((run) => run.id === runId);

        if (currentRun && currentRun.status === "completed") {
          // Pipeline completed, fetch files
          await fetchPipelineFiles(runId);
        } else if (currentRun && currentRun.status === "running") {
          // Still running, check again in 5 seconds
          setTimeout(() => checkPipelineStatus(runId), 5000);
        }
      }
    } catch (err) {
      console.error("Error checking pipeline status:", err);
    }
  };

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file && file.type === "text/csv") {
      setSelectedFile(file);
      setUploadStatus({ type: null, message: "" });
    } else {
      setUploadStatus({
        type: "error",
        message: "Please select a valid CSV file",
      });
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    setUploadStatus({ type: null, message: "" });

    try {
      const formData = new FormData();
      formData.append("file", selectedFile);

      const response = await fetch(`${API_BASE_URL}/pipelines/upload-csv`, {
        method: "POST",
        body: formData,
      });

      if (response.ok) {
        const result = await response.json();
        setPipelineId(result.pipeline_id);
        setHasUploaded(true);
        setUploadStatus({
          type: "success",
          message: `Pipeline created successfully! ID: ${result.pipeline_id}`,
        });

        // Notify parent component
        if (onPipelineCreated) {
          onPipelineCreated(result.pipeline_id);
        }
      } else {
        const error = await response.json();
        setUploadStatus({
          type: "error",
          message: `Upload failed: ${error.detail}`,
        });
      }
    } catch (error) {
      setUploadStatus({
        type: "error",
        message: `Upload failed: ${error}`,
      });
    } finally {
      setIsUploading(false);
    }
  };

  // Watch for completed pipeline runs and fetch files
  useEffect(() => {
    if (pipelineRunId && completedPipelineRuns.has(pipelineRunId)) {
      console.log(
        "✅ Pipeline completed detected! Fetching files for run:",
        pipelineRunId
      );

      // Fetch files when pipeline completes
      fetchPipelineFiles(pipelineRunId);

      // Update status message
      setUploadStatus({
        type: "success",
        message: `Pipeline completed successfully! Generated files are ready for download.`,
      });
    }
  }, [completedPipelineRuns, pipelineRunId]);

  // Remove the manual polling since we're now using the completed state
  const handleExecutePipeline = async () => {
    if (!pipelineId) return;

    setIsExecuting(true);
    try {
      const response = await fetch(
        `${API_BASE_URL}/pipelines/pipelines/${pipelineId}/execute`,
        {
          method: "POST",
        }
      );

      if (response.ok) {
        const result = await response.json();
        setPipelineRunId(result.pipeline_run_id);
        setHasExecuted(true);
        setUploadStatus({
          type: "success",
          message: `Pipeline execution started! Run ID: ${result.pipeline_run_id}. Waiting for completion...`,
        });

        // No need to start manual polling - parent state will handle it
        console.log(
          "✅ Pipeline execution started, waiting for completion event..."
        );
      } else {
        const error = await response.json();
        setUploadStatus({
          type: "error",
          message: `Execution failed: ${error.detail}`,
        });
      }
    } catch (error) {
      setUploadStatus({
        type: "error",
        message: `Execution failed: ${error}`,
      });
    } finally {
      setIsExecuting(false);
    }
  };

  const handleCreateNewPipeline = () => {
    // Reset all states to create a new pipeline
    setSelectedFile(null);
    setPipelineId(null);
    setPipelineRunId(null);
    setHasUploaded(false);
    setHasExecuted(false);
    setPipelineFiles([]);
    setUploadStatus({ type: null, message: "" });
    setFilesError(null);

    // Reset file input
    const fileInput = document.querySelector(
      'input[type="file"]'
    ) as HTMLInputElement;
    if (fileInput) {
      fileInput.value = "";
    }
  };

  const handleRefreshFiles = async () => {
    if (pipelineRunId) {
      await fetchPipelineFiles(pipelineRunId);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900">
          CSV Pipeline Creator
        </h2>
        {hasUploaded && (
          <button
            onClick={handleCreateNewPipeline}
            className="inline-flex items-center px-3 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
          >
            <Plus className="w-4 h-4 mr-2" />
            New Pipeline
          </button>
        )}
      </div>

      <div className="space-y-4">
        {/* File Selection */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Select CSV File
          </label>
          <div className="flex items-center space-x-4">
            <input
              type="file"
              accept=".csv"
              onChange={handleFileSelect}
              disabled={hasUploaded}
              className={`block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold ${
                hasUploaded
                  ? "file:bg-gray-100 file:text-gray-400 cursor-not-allowed opacity-50"
                  : "file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
              }`}
            />
            {selectedFile && (
              <div className="flex items-center text-sm text-gray-600">
                <FileText className="w-4 h-4 mr-2" />
                {selectedFile.name}
              </div>
            )}
          </div>
        </div>

        <div className="flex items-center space-x-4">
          {/* Upload Button */}
          <button
            onClick={handleUpload}
            disabled={!selectedFile || isUploading || hasUploaded}
            className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white ${
              !selectedFile || isUploading || hasUploaded
                ? "bg-gray-400 cursor-not-allowed"
                : "bg-blue-600 hover:bg-blue-700"
            }`}
          >
            <Upload className="w-4 h-4 mr-2" />
            {isUploading ? "Creating Pipeline..." : "Upload & Create Pipeline"}
          </button>

          {/* Execute Button */}
          {pipelineId && (
            <button
              onClick={handleExecutePipeline}
              disabled={isExecuting || hasExecuted}
              className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white ${
                isExecuting || hasExecuted
                  ? "bg-gray-400 cursor-not-allowed"
                  : "bg-green-600 hover:bg-green-700"
              }`}
            >
              <Play className="w-4 h-4 mr-2" />
              {isExecuting
                ? "Executing..."
                : hasExecuted
                ? "Pipeline Executed"
                : "Run Pipeline"}
            </button>
          )}
        </div>

        {/* Status Messages */}
        {uploadStatus.type && (
          <div
            className={`flex items-center p-3 rounded-md ${
              uploadStatus.type === "success"
                ? "bg-green-50 text-green-800 border border-green-200"
                : "bg-red-50 text-red-800 border border-red-200"
            }`}
          >
            {uploadStatus.type === "success" ? (
              <CheckCircle className="w-5 h-5 mr-2" />
            ) : (
              <AlertCircle className="w-5 h-5 mr-2" />
            )}
            {uploadStatus.message}
          </div>
        )}

        {/* Pipeline Info */}
        {pipelineId && (
          <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
            <h3 className="text-sm font-medium text-blue-800 mb-2">
              Pipeline Created Successfully
            </h3>
            <p className="text-sm text-blue-700">Pipeline ID: {pipelineId}</p>
            <p className="text-sm text-blue-700">
              Blocks: CSV Reader → Sentiment Analysis → Toxicity Detection →
              File Writers
            </p>
            {hasExecuted && (
              <p className="text-sm text-green-700 mt-2 font-medium">
                ✅ Pipeline execution completed
              </p>
            )}
          </div>
        )}

        {/* Pipeline Files Section */}
        {hasExecuted && pipelineRunId && (
          <div className="bg-green-50 border border-green-200 rounded-md p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-green-800">
                Generated Files
              </h3>
              <button
                onClick={handleRefreshFiles}
                disabled={isFetchingFiles}
                className="inline-flex items-center px-2 py-1 border border-green-300 text-xs font-medium rounded text-green-700 bg-white hover:bg-green-50 disabled:opacity-50"
              >
                <RefreshCw
                  className={`w-3 h-3 mr-1 ${
                    isFetchingFiles ? "animate-spin" : ""
                  }`}
                />
                Refresh
              </button>
            </div>

            {isFetchingFiles ? (
              <div className="flex items-center space-x-2 text-green-600">
                <RefreshCw className="w-4 h-4 animate-spin" />
                <span className="text-sm">Fetching files...</span>
              </div>
            ) : filesError ? (
              <div className="text-red-600 text-sm">{filesError}</div>
            ) : pipelineFiles.length > 0 ? (
              <div className="space-y-2">
                {pipelineFiles.map((file, index) => (
                  <div
                    key={index}
                    className="flex items-center space-x-3 p-3 bg-white rounded border border-green-200"
                  >
                    <FileText className="w-5 h-5 text-green-600" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-gray-900 truncate">
                        {file.filename}
                      </p>
                      <p className="text-xs text-gray-600">
                        {file.block_name} • {file.records_written} records •{" "}
                        {file.file_size}
                      </p>
                    </div>
                    <button
                      onClick={() => downloadFile(file.filename)}
                      className="inline-flex items-center px-3 py-2 border border-green-300 text-sm font-medium rounded-md text-green-700 bg-white hover:bg-green-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 transition-colors"
                      title="Download CSV"
                    >
                      <Download className="w-4 h-4 mr-2" />
                      Download
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-green-600 text-sm">
                No files available yet. Files will appear here once the pipeline
                completes processing.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default CSVPipelineUpload;
