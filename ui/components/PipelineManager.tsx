"use client";

import React, { useState, useEffect } from "react";
import {
  Play,
  Plus,
  RefreshCw,
  CheckCircle,
  XCircle,
  ChevronLeft,
  ChevronRight,
  Download,
  FileText,
} from "lucide-react";

interface Pipeline {
  id: number;
  name: string;
  description: string;
  created_at: string;
}

interface PipelineRun {
  id: number;
  pipeline_id: number;
  status: "queued" | "running" | "completed" | "failed";
  started_at?: string;
  completed_at?: string;
}

interface PipelineFile {
  block_name: string;
  filename: string;
  file_size: string;
  records_written: number;
  download_url: string;
}

const PipelineManager: React.FC = () => {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [pipelineRuns, setPipelineRuns] = useState<PipelineRun[]>([]);
  const [pipelineFiles, setPipelineFiles] = useState<{
    [key: number]: PipelineFile[];
  }>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Add state for selected pipeline
  const [selectedPipelineId, setSelectedPipelineId] = useState<number | null>(
    null
  );

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [runsCurrentPage, setRunsCurrentPage] = useState(1);
  const [itemsPerPage] = useState(5);

  const API_BASE_URL = "http://localhost:8333/api/v1";

  // Fetch pipelines
  const fetchPipelines = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/pipelines/pipelines`);
      if (response.ok) {
        const data = await response.json();
        console.log("Fetched pipelines:", data);
        setPipelines(data);
      }
    } catch (err) {
      setError("Failed to fetch pipelines");
      console.error("Error fetching pipelines:", err);
    }
  };

  // Execute pipeline
  const executePipeline = async (pipelineId: number) => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(
        `${API_BASE_URL}/pipelines/pipelines/${pipelineId}/execute`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
        }
      );

      if (response.ok) {
        const pipelineRun = await response.json();
        setPipelineRuns((prev) => [...prev, pipelineRun]);
        setError(null);
        return pipelineRun;
      } else {
        throw new Error("Failed to execute pipeline");
      }
    } catch (err) {
      setError("Failed to execute pipeline");
      console.error("Error executing pipeline:", err);
      return null;
    } finally {
      setLoading(false);
    }
  };

  // Fetch pipeline runs for a specific pipeline
  const fetchPipelineRuns = async (pipelineId: number) => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/pipelines/pipelines/${pipelineId}/runs`
      );
      if (response.ok) {
        const data = await response.json();
        setPipelineRuns(data);
        setSelectedPipelineId(pipelineId);

        // Fetch files for completed pipeline runs
        await fetchPipelineFiles(pipelineId, data);
      }
    } catch (err) {
      setError("Failed to fetch pipeline runs");
      console.error("Error fetching pipeline runs:", err);
    }
  };

  // Fetch pipeline files for completed runs
  const fetchPipelineFiles = async (
    pipelineId: number,
    runs: PipelineRun[]
  ) => {
    const completedRuns = runs.filter((run) => run.status === "completed");
    const filesMap: { [key: number]: PipelineFile[] } = {};

    for (const run of completedRuns) {
      try {
        const response = await fetch(
          `${API_BASE_URL}/downloads/pipeline/${run.id}/files`
        );
        if (response.ok) {
          const data = await response.json();
          filesMap[run.id] = data.files || [];
        }
      } catch (err) {
        console.error(`Error fetching files for run ${run.id}:`, err);
        filesMap[run.id] = [];
      }
    }

    setPipelineFiles(filesMap);
  };

  // Download CSV file
  const downloadFile = async (pipelineRunId: number, filename: string) => {
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
        setError("Failed to download file");
      }
    } catch (err) {
      setError("Failed to download file");
      console.error("Error downloading file:", err);
    }
  };

  // Clear pipeline runs
  const clearPipelineRuns = () => {
    setSelectedPipelineId(null);
    setPipelineRuns([]);
    setPipelineFiles({});
  };

  useEffect(() => {
    fetchPipelines();
  }, []);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "completed":
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case "failed":
        return <XCircle className="w-4 h-4 text-red-500" />;
      case "running":
        return <RefreshCw className="w-4 h-4 text-blue-500 animate-spin" />;
      default:
        return <div className="w-4 h-4 bg-gray-300 rounded-full" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "completed":
        return "bg-green-100 text-green-800";
      case "failed":
        return "bg-red-100 text-red-800";
      case "running":
        return "bg-blue-100 text-blue-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  // Pagination calculations
  const totalPages = Math.ceil(pipelines.length / itemsPerPage);
  const totalRunsPages = Math.ceil(pipelineRuns.length / itemsPerPage);

  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const currentPipelines = pipelines.slice(startIndex, endIndex);

  // Filter runs for selected pipeline only
  const selectedPipelineRuns = selectedPipelineId
    ? pipelineRuns.filter((run) => run.pipeline_id === selectedPipelineId)
    : [];

  // Update pagination calculations for selected pipeline runs
  const totalRunsPagesForSelected = Math.ceil(
    selectedPipelineRuns.length / itemsPerPage
  );
  const runsStartIndex = (runsCurrentPage - 1) * itemsPerPage;
  const runsEndIndex = runsStartIndex + itemsPerPage;
  const currentRuns = selectedPipelineRuns.slice(runsStartIndex, runsEndIndex);

  // Pagination component
  const Pagination = ({
    currentPage,
    totalPages,
    onPageChange,
    label,
  }: {
    currentPage: number;
    totalPages: number;
    onPageChange: (page: number) => void;
    label: string;
  }) => (
    <div className="flex items-center justify-between">
      <div className="text-sm text-gray-700">
        Showing page {currentPage} of {totalPages} for {label}
      </div>
      <div className="flex items-center space-x-2">
        <button
          onClick={() => onPageChange(currentPage - 1)}
          disabled={currentPage === 1}
          className="inline-flex items-center px-2 py-1 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <ChevronLeft className="w-4 h-4" />
        </button>
        <span className="px-3 py-1 text-sm text-gray-700">
          {currentPage} / {totalPages}
        </span>
        <button
          onClick={() => onPageChange(currentPage + 1)}
          disabled={currentPage === totalPages}
          className="inline-flex items-center px-2 py-1 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <ChevronRight className="w-4 h-4" />
        </button>
      </div>
    </div>
  );

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-lg font-semibold text-gray-900">
          Pipeline History
        </h2>
        <div className="flex space-x-3">
          <button
            onClick={fetchPipelines}
            disabled={loading}
            className="inline-flex items-center px-3 py-2 border border-gray-300 text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
          >
            <RefreshCw
              className={`w-4 h-4 mr-2 ${loading ? "animate-spin" : ""}`}
            />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-md">
          <p className="text-sm text-red-600">{error}</p>
        </div>
      )}

      {/* Pipelines Table */}
      <div className="space-y-4">
        <h3 className="text-md font-medium text-gray-900">Pipelines</h3>
        {pipelines.length === 0 ? (
          <p className="text-gray-500 text-sm">
            No pipelines created yet. Create a sample pipeline to get started.
          </p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Description
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Created
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {currentPipelines.map((pipeline) => (
                    <tr key={pipeline.id} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        #{pipeline.id}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {pipeline.name}
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-500 max-w-xs truncate">
                        {pipeline.description}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {new Date(pipeline.created_at).toLocaleDateString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                        <div className="flex space-x-2">
                          <button
                            onClick={() => fetchPipelineRuns(pipeline.id)}
                            className="inline-flex items-center px-2 py-1 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50"
                          >
                            View Runs
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pipelines Pagination */}
            {totalPages > 1 && (
              <div className="mt-4">
                <Pagination
                  currentPage={currentPage}
                  totalPages={totalPages}
                  onPageChange={setCurrentPage}
                  label="pipelines"
                />
              </div>
            )}
          </>
        )}
      </div>

      {/* Pipeline Runs Table - Only show if a pipeline is selected */}
      {selectedPipelineId && selectedPipelineRuns.length > 0 && (
        <div className="mt-8 space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-md font-medium text-gray-900">
              Pipeline Runs for Pipeline #{selectedPipelineId}
            </h3>
            <button
              onClick={clearPipelineRuns}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              Close
            </button>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Run ID
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Pipeline ID
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Completed
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Files
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {currentRuns.map((run) => (
                  <tr key={run.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      #{run.id}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      #{run.pipeline_id}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center space-x-2">
                        {getStatusIcon(run.status)}
                        <span
                          className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(
                            run.status
                          )}`}
                        >
                          {run.status}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {run.completed_at
                        ? new Date(run.completed_at).toLocaleString()
                        : "-"}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {run.status === "completed" &&
                      pipelineFiles[run.id] &&
                      pipelineFiles[run.id].length > 0 ? (
                        <div className="space-y-2">
                          {pipelineFiles[run.id].map((file, index) => (
                            <div
                              key={index}
                              className="flex items-center space-x-2 p-2 bg-gray-50 rounded border"
                            >
                              <FileText className="w-4 h-4 text-gray-500" />
                              <div className="flex-1 min-w-0">
                                <p className="text-xs text-gray-700 truncate">
                                  {file.filename}
                                </p>
                                <p className="text-xs text-gray-500">
                                  {file.records_written} records •{" "}
                                  {file.file_size}
                                </p>
                              </div>
                              <button
                                onClick={() =>
                                  downloadFile(run.id, file.filename)
                                }
                                className="inline-flex items-center px-2 py-1 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500"
                                title="Download CSV"
                              >
                                <Download className="w-3 h-3" />
                              </button>
                            </div>
                          ))}
                        </div>
                      ) : run.status === "completed" ? (
                        <span className="text-gray-400 text-xs">
                          No files available
                        </span>
                      ) : (
                        <span className="text-gray-400 text-xs">-</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Runs Pagination */}
          {totalRunsPagesForSelected > 1 && (
            <div className="mt-4">
              <Pagination
                currentPage={runsCurrentPage}
                totalPages={totalRunsPagesForSelected}
                onPageChange={setRunsCurrentPage}
                label="pipeline runs"
              />
            </div>
          )}
        </div>
      )}

      {/* Show message when no runs exist for selected pipeline */}
      {selectedPipelineId && selectedPipelineRuns.length === 0 && (
        <div className="mt-8 p-4 bg-gray-50 rounded-md">
          <p className="text-gray-500 text-sm">
            No pipeline runs found for Pipeline #{selectedPipelineId}
          </p>
          <button
            onClick={clearPipelineRuns}
            className="mt-2 text-sm text-gray-600 hover:text-gray-800 underline"
          >
            Close
          </button>
        </div>
      )}
    </div>
  );
};

export default PipelineManager;
