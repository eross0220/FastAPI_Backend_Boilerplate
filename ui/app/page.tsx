"use client";

import React, { useState } from "react";
import CSVPipelineUpload from "../components/CSVPipelineUpload";
import PipelineManager from "../components/PipelineManager";
import KafkaLogs from "../components/KafkaLogs";

export default function Home() {
  // State to track completed pipeline runs
  const [completedPipelineRuns, setCompletedPipelineRuns] = useState<
    Set<number>
  >(new Set());

  // Callback function to receive completed pipeline runs from KafkaLogs
  const handlePipelineCompleted = (pipelineRunId: number) => {
    console.log(" Pipeline completed in KafkaLogs:", pipelineRunId);
    setCompletedPipelineRuns((prev) => new Set(prev).add(pipelineRunId));
  };

  // Callback function to receive completed pipeline runs from KafkaLogs
  const handlePipelineFailed = (pipelineRunId: number) => {
    console.log("❌ Pipeline failed in KafkaLogs:", pipelineRunId);
    // You can handle failed pipelines here if needed
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-3">
              <div className="w-8 h-8 bg-white border-2 border-gray-900 rounded-full flex items-center justify-center">
                <img
                  src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAxUlEQVR4AWLwySwEtF/GNhCDQBCswUW4C7qgDndCK18CRVDIpaT8BvvSBs5sluBBmnjmjOQTS/m/gB2Ac4ICAoRbnkAFHQzQ3ZN/fnISLvkBLhHbAxJoKwN0eiWcd+8P4N0XENMCOGG6Iav89QBOlyipoCkqnhWQKRoPiCefvYJhD5CfSvcHyPQrA7LIvQGySseqgAvE9ACcQzhBoryBMSVAZBlcpBDd428SKueEFHmI24XhDtC7ddMB5YsDwg3lbb+MdsAXgpwpRTasO3oAAAAASUVORK5CYII="
                  alt="Logo"
                  className="w-5 h-5"
                />
              </div>
              <h1 className="text-xl font-bold text-gray-900">
                Data Pipeline Builder
              </h1>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Left Column */}
          <div className="space-y-8">
            {/* CSV Pipeline Upload */}
            <CSVPipelineUpload
              onPipelineCreated={() => {
                console.log("Pipeline created");
              }}
              completedPipelineRuns={completedPipelineRuns}
            />
            {/* Pipeline Manager */}
            <PipelineManager />
          </div>

          {/* Right Column */}
          <div className="space-y-8">
            {/* Kafka Logs */}
            <KafkaLogs
              onPipelineCompleted={handlePipelineCompleted}
              onPipelineFailed={handlePipelineFailed}
            />
          </div>
        </div>
      </main>
    </div>
  );
}
