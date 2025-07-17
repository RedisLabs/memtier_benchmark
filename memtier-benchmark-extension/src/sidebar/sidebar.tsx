import React, { useState, useEffect, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import { MemtierConfig, BenchmarkResult, Message } from '../shared/types';
import { StorageManager } from '../shared/storage';
import { BenchmarkConfigManager } from '../shared/benchmark-config';
import { ConfigForm } from '../components/ConfigForm';
import { ResultsDisplay } from '../components/ResultsDisplay';
import { 
  X, 
  Settings, 
  History, 
  Play, 
  Square,
  Maximize2,
  Minimize2
} from 'lucide-react';

type SidebarTab = 'config' | 'results' | 'console';

const SidebarApp: React.FC = () => {
  const [activeTab, setActiveTab] = useState<SidebarTab>('config');
  const [config, setConfig] = useState<MemtierConfig>(BenchmarkConfigManager.mergeWithDefaults({}));
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [currentBenchmark, setCurrentBenchmark] = useState<BenchmarkResult | null>(null);
  const [consoleOutput, setConsoleOutput] = useState<string>('');
  const [isRunning, setIsRunning] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [loading, setLoading] = useState(true);
  
  const consoleRef = useRef<HTMLDivElement>(null);
  const resizeHandleRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    loadInitialData();
    setupMessageListener();
    setupResizeHandler();
  }, []);

  useEffect(() => {
    // Auto-scroll console to bottom when new output is added
    if (consoleRef.current) {
      consoleRef.current.scrollTop = consoleRef.current.scrollHeight;
    }
  }, [consoleOutput]);

  const loadInitialData = async () => {
    try {
      const [loadedConfig, loadedResults] = await Promise.all([
        StorageManager.loadCurrentConfig(),
        StorageManager.loadBenchmarkResults()
      ]);

      setConfig(loadedConfig);
      setResults(loadedResults);
    } catch (error) {
      console.error('Error loading initial data:', error);
    } finally {
      setLoading(false);
    }
  };

  const setupMessageListener = () => {
    // Listen for messages from content script
    window.addEventListener('message', (event) => {
      if (event.data?.source === 'memtier-content') {
        handleContentMessage(event.data);
      }
    });

    // Listen for messages from background script
    chrome.runtime.onMessage.addListener((message: Message) => {
      if (message.type === 'BENCHMARK_STATUS') {
        handleBenchmarkStatus(message.payload);
      }
    });
  };

  const setupResizeHandler = () => {
    const resizeHandle = resizeHandleRef.current;
    if (!resizeHandle) return;

    let isResizing = false;
    let startX = 0;
    let startWidth = 0;

    const handleMouseDown = (e: MouseEvent) => {
      isResizing = true;
      startX = e.clientX;
      startWidth = document.body.offsetWidth;
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    };

    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      
      const deltaX = startX - e.clientX;
      const newWidth = Math.max(300, Math.min(800, startWidth + deltaX));
      
      document.body.style.width = `${newWidth}px`;
      
      // Notify parent about resize
      window.parent.postMessage({
        source: 'memtier-sidebar',
        type: 'RESIZE_SIDEBAR',
        width: newWidth
      }, '*');
    };

    const handleMouseUp = () => {
      isResizing = false;
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    resizeHandle.addEventListener('mousedown', handleMouseDown);
  };

  const handleContentMessage = (data: any) => {
    switch (data.type) {
      case 'BENCHMARK_STATUS':
        handleBenchmarkStatus(data.payload);
        break;
      case 'BENCHMARK_OUTPUT':
        handleBenchmarkOutput(data.payload);
        break;
      case 'BENCHMARK_COMPLETE':
        handleBenchmarkComplete(data.payload);
        break;
    }
  };

  const handleBenchmarkStatus = (payload: { benchmarkId: string; result: BenchmarkResult }) => {
    const { result } = payload;
    
    setCurrentBenchmark(result);
    setConsoleOutput(result.output);
    
    if (result.status === 'running') {
      setIsRunning(true);
      setActiveTab('console');
    } else {
      setIsRunning(false);
    }
    
    // Update results list
    setResults(prev => {
      const updated = prev.filter(r => r.id !== result.id);
      return [result, ...updated];
    });
  };

  const handleBenchmarkOutput = (payload: { benchmarkId: string; output: string }) => {
    setConsoleOutput(payload.output);
  };

  const handleBenchmarkComplete = (payload: { benchmarkId: string; result: BenchmarkResult }) => {
    setCurrentBenchmark(payload.result);
    setConsoleOutput(payload.result.output);
    setIsRunning(false);
  };

  const handleConfigChange = (newConfig: MemtierConfig) => {
    setConfig(newConfig);
    StorageManager.saveCurrentConfig(newConfig);
  };

  const handleRunBenchmark = (benchmarkConfig: MemtierConfig) => {
    setConsoleOutput('Starting benchmark...\n');
    setActiveTab('console');
    
    // Send message to content script to forward to background
    window.parent.postMessage({
      source: 'memtier-sidebar',
      type: 'RUN_BENCHMARK',
      config: benchmarkConfig
    }, '*');
  };

  const handleStopBenchmark = () => {
    // TODO: Implement benchmark stopping
    setIsRunning(false);
  };

  const handleCloseSidebar = () => {
    window.parent.postMessage({
      source: 'memtier-sidebar',
      type: 'CLOSE_SIDEBAR'
    }, '*');
  };

  const toggleExpanded = () => {
    const newWidth = isExpanded ? 400 : 600;
    setIsExpanded(!isExpanded);
    document.body.style.width = `${newWidth}px`;
    
    window.parent.postMessage({
      source: 'memtier-sidebar',
      type: 'RESIZE_SIDEBAR',
      width: newWidth
    }, '*');
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'config':
        return (
          <ConfigForm
            config={config}
            onChange={handleConfigChange}
            onRun={handleRunBenchmark}
            isRunning={isRunning}
            compact={true}
          />
        );

      case 'results':
        return (
          <ResultsDisplay 
            results={results} 
            compact={true}
          />
        );

      case 'console':
        return (
          <div>
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium">Console Output</h3>
              <div className="flex items-center space-x-2">
                {currentBenchmark && (
                  <div className={`status-indicator ${
                    currentBenchmark.status === 'running' ? 'status-running' :
                    currentBenchmark.status === 'completed' ? 'status-completed' :
                    'status-failed'
                  }`}>
                    {currentBenchmark.status === 'running' && <Play size={12} className="animate-pulse" />}
                    {currentBenchmark.status}
                  </div>
                )}
                {isRunning && (
                  <button
                    onClick={handleStopBenchmark}
                    className="sidebar-btn sidebar-btn-secondary sidebar-btn-small"
                  >
                    <Square size={12} />
                    Stop
                  </button>
                )}
              </div>
            </div>
            
            <div 
              ref={consoleRef}
              className="console-output"
            >
              {consoleOutput || 'No output yet. Run a benchmark to see results here.'}
            </div>
            
            {currentBenchmark?.stats && (
              <div className="mt-4 p-3 bg-gray-50 rounded-lg">
                <h4 className="text-sm font-medium mb-2">Quick Stats</h4>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>RPS: {currentBenchmark.stats.requestsPerSecond.toFixed(0)}</div>
                  <div>Avg Latency: {currentBenchmark.stats.avgLatency.toFixed(2)}ms</div>
                  <div>P99: {currentBenchmark.stats.p99Latency.toFixed(2)}ms</div>
                  <div>Errors: {currentBenchmark.stats.errors}</div>
                </div>
              </div>
            )}
          </div>
        );

      default:
        return null;
    }
  };

  if (loading) {
    return (
      <div className="sidebar-container">
        <div className="loading"></div>
      </div>
    );
  }

  return (
    <div className="sidebar-container">
      <div ref={resizeHandleRef} className="resize-handle"></div>
      
      {/* Header */}
      <div className="sidebar-header">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold text-gray-900">Memtier Benchmark</h1>
          <div className="flex items-center space-x-1">
            <button
              onClick={toggleExpanded}
              className="close-button"
              title={isExpanded ? 'Collapse' : 'Expand'}
            >
              {isExpanded ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
            </button>
            <button
              onClick={handleCloseSidebar}
              className="close-button"
              title="Close sidebar"
            >
              <X size={16} />
            </button>
          </div>
        </div>
        
        {/* Tabs */}
        <div className="sidebar-tabs">
          <button
            className={`sidebar-tab ${activeTab === 'config' ? 'active' : ''}`}
            onClick={() => setActiveTab('config')}
          >
            <Settings size={14} />
            Config
          </button>
          <button
            className={`sidebar-tab ${activeTab === 'results' ? 'active' : ''}`}
            onClick={() => setActiveTab('results')}
          >
            <History size={14} />
            Results
          </button>
          <button
            className={`sidebar-tab ${activeTab === 'console' ? 'active' : ''}`}
            onClick={() => setActiveTab('console')}
          >
            <Play size={14} />
            Console
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="sidebar-content">
        {renderTabContent()}
      </div>

      {/* Footer */}
      <div className="sidebar-footer">
        <div className="text-xs text-gray-500 text-center">
          Press Ctrl+Shift+M to toggle sidebar
        </div>
      </div>
    </div>
  );
};

// Initialize the sidebar
const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(<SidebarApp />);
}
