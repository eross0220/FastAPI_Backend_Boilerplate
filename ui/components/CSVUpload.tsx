"use client";

import React, { useState, useCallback } from "react";
import Papa from "papaparse";
import { Upload, FileText, CheckCircle, AlertCircle, X } from "lucide-react";
import { Button } from "./ui/Button";
import { Card, CardHeader, CardContent } from "./ui/Card";

interface CSVData {
  [key: string]: string;
}

interface CSVUploadProps {
  onDataUploaded: (data: CSVData[]) => void;
  onLog: (
    message: string,
    type: "info" | "success" | "warning" | "error"
  ) => void;
}

export function CSVUpload({ onDataUploaded, onLog }: CSVUploadProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);

    const files = Array.from(e.dataTransfer.files);
    const csvFile = files.find(
      (file) => file.type === "text/csv" || file.name.endsWith(".csv")
    );

    if (csvFile) {
      handleFileUpload(csvFile);
    } else {
      onLog("Please upload a valid CSV file", "error");
    }
  }, []);

  const handleFileSelect = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) {
        handleFileUpload(file);
      }
    },
    []
  );

  const handleFileUpload = useCallback(
    (file: File) => {
      setIsProcessing(true);
      setUploadedFile(file);

      onLog(`Processing file: ${file.name}`, "info");

      Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: (results) => {
          setIsProcessing(false);

          if (results.errors.length > 0) {
            onLog(`Found ${results.errors.length} parsing errors`, "warning");
          }

          if (results.data.length > 0) {
            onLog(`Successfully parsed ${results.data.length} rows`, "success");
            onDataUploaded(results.data as CSVData[]);
          } else {
            onLog("No data found in CSV file", "error");
          }
        },
        error: (error) => {
          setIsProcessing(false);
          onLog(`Error parsing CSV: ${error.message}`, "error");
        },
      });
    },
    [onDataUploaded, onLog]
  );

  const clearFile = useCallback(() => {
    setUploadedFile(null);
    onLog("File cleared", "info");
  }, [onLog]);

  return (
    <Card className="h-full">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <FileText className="w-5 h-5 text-primary-600" />
            <h2 className="text-lg font-semibold text-gray-900">CSV Upload</h2>
          </div>
          {uploadedFile && (
            <Button
              variant="ghost"
              size="sm"
              onClick={clearFile}
              className="text-gray-400 hover:text-gray-600"
            >
              <X className="w-4 h-4" />
            </Button>
          )}
        </div>
      </CardHeader>

      <CardContent>
        <div
          className={`upload-area rounded-lg p-8 text-center transition-all duration-200 ${
            isDragOver ? "dragover" : ""
          } ${uploadedFile ? "bg-green-50 border-green-300" : ""}`}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
        >
          {uploadedFile ? (
            <div className="space-y-4">
              <CheckCircle className="w-12 h-12 text-green-500 mx-auto" />
              <div>
                <p className="text-sm font-medium text-gray-900">
                  {uploadedFile.name}
                </p>
                <p className="text-xs text-gray-500">
                  {(uploadedFile.size / 1024).toFixed(1)} KB
                </p>
              </div>
              {isProcessing && (
                <div className="flex items-center justify-center space-x-2">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-primary-600"></div>
                  <span className="text-sm text-gray-600">Processing...</span>
                </div>
              )}
            </div>
          ) : (
            <div className="space-y-4">
              <div className="mx-auto w-16 h-16 bg-primary-100 rounded-full flex items-center justify-center">
                <Upload className="w-8 h-8 text-primary-600" />
              </div>
              <div>
                <p className="text-lg font-medium text-gray-900">
                  Drop your CSV file here
                </p>
                <p className="text-sm text-gray-500 mt-1">
                  or click to browse files
                </p>
              </div>
              <Button
                variant="outline"
                onClick={() => document.getElementById("file-input")?.click()}
                className="mt-4"
              >
                Choose File
              </Button>
            </div>
          )}
        </div>

        <input
          id="file-input"
          type="file"
          accept=".csv"
          onChange={handleFileSelect}
          className="hidden"
        />

        <div className="mt-6 space-y-3">
          <h3 className="text-sm font-medium text-gray-900">
            Supported formats:
          </h3>
          <ul className="text-sm text-gray-600 space-y-1">
            <li>• CSV files with headers</li>
            <li>• UTF-8 encoding</li>
            <li>• Maximum file size: 10MB</li>
          </ul>
        </div>
      </CardContent>
    </Card>
  );
}
