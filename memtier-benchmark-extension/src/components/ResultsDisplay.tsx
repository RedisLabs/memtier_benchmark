import React, { useState } from 'react';
import { BenchmarkResult, BenchmarkStats } from '../shared/types';
import { formatDuration, formatNumber, copyToClipboard } from '../shared/utils';
import { 
  Clock, 
  Activity, 
  Zap, 
  AlertCircle, 
  CheckCircle, 
  Copy, 
  Download,
  Eye,
  EyeOff,
  BarChart3
} from 'lucide-react';

interface ResultsDisplayProps {
  results: BenchmarkResult[];
  onResultSelect?: (result: BenchmarkResult) => void;
  compact?: boolean;
}

export const ResultsDisplay: React.FC<ResultsDisplayProps> = ({
  results,
  onResultSelect,
  compact = false
}) => {
  const [selectedResult, setSelectedResult] = useState<BenchmarkResult | null>(null);
  const [showOutput, setShowOutput] = useState<{ [key: string]: boolean }>({});

  const handleResultClick = (result: BenchmarkResult) => {
    setSelectedResult(result);
    onResultSelect?.(result);
  };

  const toggleOutput = (resultId: string) => {
    setShowOutput(prev => ({
      ...prev,
      [resultId]: !prev[resultId]
    }));
  };

  const handleCopyOutput = async (output: string) => {
    const success = await copyToClipboard(output);
    // You could show a toast notification here
  };

  const handleDownloadOutput = (result: BenchmarkResult) => {
    const blob = new Blob([result.output], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `benchmark-${result.id}-${new Date(result.timestamp).toISOString().slice(0, 19)}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getStatusIcon = (status: BenchmarkResult['status']) => {
    switch (status) {
      case 'running':
        return <Activity className="animate-spin text-blue-500" size={16} />;
      case 'completed':
        return <CheckCircle className="text-green-500" size={16} />;
      case 'failed':
        return <AlertCircle className="text-red-500" size={16} />;
      default:
        return <Clock className="text-gray-500" size={16} />;
    }
  };

  const getStatusColor = (status: BenchmarkResult['status']) => {
    switch (status) {
      case 'running':
        return 'bg-blue-50 border-blue-200';
      case 'completed':
        return 'bg-green-50 border-green-200';
      case 'failed':
        return 'bg-red-50 border-red-200';
      default:
        return 'bg-gray-50 border-gray-200';
    }
  };

  const renderStats = (stats: BenchmarkStats) => (
    <div className="grid grid-cols-2 gap-4 mt-3">
      <div className="space-y-2">
        <div className="flex items-center space-x-2">
          <Zap size={14} className="text-blue-500" />
          <span className="text-sm text-gray-600">RPS:</span>
          <span className="text-sm font-medium">{formatNumber(stats.requestsPerSecond)}</span>
        </div>
        <div className="flex items-center space-x-2">
          <Clock size={14} className="text-green-500" />
          <span className="text-sm text-gray-600">Avg Latency:</span>
          <span className="text-sm font-medium">{stats.avgLatency.toFixed(2)}ms</span>
        </div>
        <div className="flex items-center space-x-2">
          <BarChart3 size={14} className="text-purple-500" />
          <span className="text-sm text-gray-600">Total Requests:</span>
          <span className="text-sm font-medium">{formatNumber(stats.totalRequests)}</span>
        </div>
      </div>
      <div className="space-y-2">
        <div className="text-xs text-gray-500">Percentiles:</div>
        <div className="text-xs space-y-1">
          <div>P50: {stats.p50Latency.toFixed(2)}ms</div>
          <div>P90: {stats.p90Latency.toFixed(2)}ms</div>
          <div>P99: {stats.p99Latency.toFixed(2)}ms</div>
        </div>
      </div>
    </div>
  );

  const renderResultCard = (result: BenchmarkResult) => (
    <div
      key={result.id}
      className={`border rounded-lg p-4 cursor-pointer transition-all hover:shadow-md ${getStatusColor(result.status)} ${
        selectedResult?.id === result.id ? 'ring-2 ring-blue-500' : ''
      }`}
      onClick={() => handleResultClick(result)}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center space-x-2">
          {getStatusIcon(result.status)}
          <span className="text-sm font-medium text-gray-900">
            {new Date(result.timestamp).toLocaleString()}
          </span>
        </div>
        <div className="flex items-center space-x-1">
          <button
            onClick={(e) => {
              e.stopPropagation();
              toggleOutput(result.id);
            }}
            className="p-1 hover:bg-gray-200 rounded"
            title="Toggle output"
          >
            {showOutput[result.id] ? <EyeOff size={14} /> : <Eye size={14} />}
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              handleCopyOutput(result.output);
            }}
            className="p-1 hover:bg-gray-200 rounded"
            title="Copy output"
          >
            <Copy size={14} />
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              handleDownloadOutput(result);
            }}
            className="p-1 hover:bg-gray-200 rounded"
            title="Download output"
          >
            <Download size={14} />
          </button>
        </div>
      </div>

      {/* Configuration Summary */}
      <div className="text-xs text-gray-600 mb-2">
        {result.config.server}:{result.config.port} • 
        {result.config.clients} clients • 
        {result.config.threads} threads • 
        {result.config.protocol}
      </div>

      {/* Duration */}
      {result.duration && (
        <div className="text-xs text-gray-500 mb-2">
          Duration: {formatDuration(result.duration)}
        </div>
      )}

      {/* Stats */}
      {result.stats && renderStats(result.stats)}

      {/* Output */}
      {showOutput[result.id] && (
        <div className="mt-3 p-3 bg-gray-900 rounded text-green-400 text-xs font-mono max-h-40 overflow-y-auto">
          <pre className="whitespace-pre-wrap">{result.output}</pre>
        </div>
      )}

      {/* Error Display */}
      {result.status === 'failed' && (
        <div className="mt-2 p-2 bg-red-100 border border-red-200 rounded text-xs text-red-700">
          Benchmark failed. Check output for details.
        </div>
      )}
    </div>
  );

  if (results.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <Activity size={48} className="mx-auto mb-4 opacity-50" />
        <p>No benchmark results yet</p>
        <p className="text-sm">Run your first benchmark to see results here</p>
      </div>
    );
  }

  return (
    <div className={`space-y-4 ${compact ? 'max-h-96 overflow-y-auto' : ''}`}>
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-medium text-gray-900">
          Benchmark Results ({results.length})
        </h3>
        {results.length > 0 && (
          <div className="text-sm text-gray-500">
            Latest: {new Date(results[0].timestamp).toLocaleString()}
          </div>
        )}
      </div>

      <div className="space-y-3">
        {results.map(renderResultCard)}
      </div>

      {/* Selected Result Details */}
      {selectedResult && !compact && (
        <div className="mt-6 p-4 bg-white border border-gray-200 rounded-lg">
          <h4 className="text-md font-medium text-gray-900 mb-3">
            Detailed Results
          </h4>
          
          {/* Configuration Details */}
          <div className="mb-4">
            <h5 className="text-sm font-medium text-gray-700 mb-2">Configuration:</h5>
            <div className="grid grid-cols-2 gap-4 text-xs">
              <div>
                <div><strong>Server:</strong> {selectedResult.config.server}:{selectedResult.config.port}</div>
                <div><strong>Protocol:</strong> {selectedResult.config.protocol}</div>
                <div><strong>Clients:</strong> {selectedResult.config.clients}</div>
                <div><strong>Threads:</strong> {selectedResult.config.threads}</div>
              </div>
              <div>
                <div><strong>Requests:</strong> {selectedResult.config.requests}</div>
                <div><strong>Ratio:</strong> {selectedResult.config.ratio}</div>
                <div><strong>Pipeline:</strong> {selectedResult.config.pipeline}</div>
                <div><strong>Data Size:</strong> {selectedResult.config.dataSize} bytes</div>
              </div>
            </div>
          </div>

          {/* Full Output */}
          <div>
            <h5 className="text-sm font-medium text-gray-700 mb-2">Full Output:</h5>
            <div className="p-3 bg-gray-900 rounded text-green-400 text-xs font-mono max-h-60 overflow-y-auto">
              <pre className="whitespace-pre-wrap">{selectedResult.output}</pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
