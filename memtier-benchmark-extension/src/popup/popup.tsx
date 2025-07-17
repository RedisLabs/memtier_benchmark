import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import { MemtierConfig, BenchmarkResult, ConfigPreset, Message } from '../shared/types';
import { StorageManager } from '../shared/storage';
import { BenchmarkConfigManager } from '../shared/benchmark-config';
import { ConfigForm } from '../components/ConfigForm';
import { ResultsDisplay } from '../components/ResultsDisplay';
import { 
  Settings, 
  History, 
  Play, 
  Sidebar,
  BookOpen,
  Download,
  Upload,
  Trash2
} from 'lucide-react';

type TabType = 'config' | 'results' | 'presets' | 'settings';

const PopupApp: React.FC = () => {
  const [activeTab, setActiveTab] = useState<TabType>('config');
  const [config, setConfig] = useState<MemtierConfig>(BenchmarkConfigManager.mergeWithDefaults({}));
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [presets, setPresets] = useState<ConfigPreset[]>([]);
  const [isRunning, setIsRunning] = useState(false);
  const [currentBenchmarkId, setCurrentBenchmarkId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadInitialData();
    setupMessageListener();
  }, []);

  const loadInitialData = async () => {
    try {
      const [loadedConfig, loadedResults, loadedPresets] = await Promise.all([
        StorageManager.loadCurrentConfig(),
        StorageManager.loadBenchmarkResults(),
        StorageManager.loadConfigPresets()
      ]);

      setConfig(loadedConfig);
      setResults(loadedResults);
      setPresets(loadedPresets);
    } catch (error) {
      console.error('Error loading initial data:', error);
    } finally {
      setLoading(false);
    }
  };

  const setupMessageListener = () => {
    chrome.runtime.onMessage.addListener((message: Message) => {
      if (message.type === 'BENCHMARK_STATUS') {
        const { benchmarkId, result } = message.payload;
        
        if (benchmarkId === currentBenchmarkId) {
          if (result.status === 'completed' || result.status === 'failed') {
            setIsRunning(false);
            setCurrentBenchmarkId(null);
          }
        }
        
        // Update results
        setResults(prev => {
          const updated = prev.filter(r => r.id !== benchmarkId);
          return [result, ...updated];
        });
      }
    });
  };

  const handleConfigChange = (newConfig: MemtierConfig) => {
    setConfig(newConfig);
    StorageManager.saveCurrentConfig(newConfig);
  };

  const handleRunBenchmark = async (benchmarkConfig: MemtierConfig) => {
    setIsRunning(true);
    
    try {
      const response = await chrome.runtime.sendMessage({
        type: 'RUN_BENCHMARK',
        payload: benchmarkConfig
      });

      if (response.success) {
        setCurrentBenchmarkId(response.benchmarkId);
      } else {
        setIsRunning(false);
        console.error('Failed to start benchmark:', response.error);
      }
    } catch (error) {
      setIsRunning(false);
      console.error('Error starting benchmark:', error);
    }
  };

  const handleSavePreset = async (config: MemtierConfig) => {
    const name = prompt('Enter preset name:');
    if (!name) return;

    const preset: ConfigPreset = {
      id: Date.now().toString(),
      name,
      description: `${config.server}:${config.port} - ${config.clients} clients`,
      config,
      createdAt: Date.now(),
      updatedAt: Date.now()
    };

    await StorageManager.saveConfigPreset(preset);
    setPresets(prev => [preset, ...prev]);
  };

  const handleLoadPreset = async (preset: ConfigPreset) => {
    setConfig(preset.config);
    await StorageManager.saveCurrentConfig(preset.config);
    setActiveTab('config');
  };

  const handleDeletePreset = async (presetId: string) => {
    if (confirm('Are you sure you want to delete this preset?')) {
      await StorageManager.deleteConfigPreset(presetId);
      setPresets(prev => prev.filter(p => p.id !== presetId));
    }
  };

  const handleToggleSidebar = () => {
    chrome.runtime.sendMessage({ type: 'TOGGLE_SIDEBAR' });
  };

  const handleExportData = async () => {
    const data = await StorageManager.exportData();
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `memtier-benchmark-data-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleImportData = () => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.json';
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;

      try {
        const text = await file.text();
        const data = JSON.parse(text);
        await StorageManager.importData(data);
        await loadInitialData();
        alert('Data imported successfully!');
      } catch (error) {
        alert('Error importing data: ' + error.message);
      }
    };
    input.click();
  };

  const handleClearResults = async () => {
    if (confirm('Are you sure you want to clear all benchmark results?')) {
      await StorageManager.clearBenchmarkResults();
      setResults([]);
    }
  };

  if (loading) {
    return (
      <div className="popup-container">
        <div className="loading"></div>
      </div>
    );
  }

  const renderTabContent = () => {
    switch (activeTab) {
      case 'config':
        return (
          <ConfigForm
            config={config}
            onChange={handleConfigChange}
            onRun={handleRunBenchmark}
            onSave={handleSavePreset}
            isRunning={isRunning}
            compact={true}
          />
        );

      case 'results':
        return (
          <div>
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium">Results</h3>
              {results.length > 0 && (
                <button
                  onClick={handleClearResults}
                  className="btn btn-secondary text-xs"
                >
                  <Trash2 size={12} />
                  Clear All
                </button>
              )}
            </div>
            <ResultsDisplay results={results} compact={true} />
          </div>
        );

      case 'presets':
        return (
          <div>
            <h3 className="text-lg font-medium mb-4">Configuration Presets</h3>
            {presets.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                <BookOpen size={48} className="mx-auto mb-4 opacity-50" />
                <p>No presets saved yet</p>
                <p className="text-sm">Save configurations for quick access</p>
              </div>
            ) : (
              <div className="space-y-2">
                {presets.map(preset => (
                  <div key={preset.id} className="border rounded-lg p-3">
                    <div className="flex justify-between items-start">
                      <div className="flex-1">
                        <h4 className="font-medium text-sm">{preset.name}</h4>
                        <p className="text-xs text-gray-600 mt-1">{preset.description}</p>
                        <p className="text-xs text-gray-500 mt-1">
                          Created: {new Date(preset.createdAt).toLocaleDateString()}
                        </p>
                      </div>
                      <div className="flex space-x-1">
                        <button
                          onClick={() => handleLoadPreset(preset)}
                          className="btn btn-primary text-xs"
                        >
                          Load
                        </button>
                        <button
                          onClick={() => handleDeletePreset(preset.id)}
                          className="btn btn-secondary text-xs"
                        >
                          <Trash2 size={12} />
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        );

      case 'settings':
        return (
          <div>
            <h3 className="text-lg font-medium mb-4">Settings</h3>
            <div className="space-y-4">
              <div className="flex justify-between items-center">
                <span className="text-sm">Toggle Sidebar</span>
                <button onClick={handleToggleSidebar} className="btn btn-primary text-xs">
                  <Sidebar size={12} />
                  Toggle
                </button>
              </div>
              
              <div className="border-t pt-4">
                <h4 className="text-sm font-medium mb-2">Data Management</h4>
                <div className="space-y-2">
                  <button onClick={handleExportData} className="btn btn-secondary text-xs w-full">
                    <Download size={12} />
                    Export Data
                  </button>
                  <button onClick={handleImportData} className="btn btn-secondary text-xs w-full">
                    <Upload size={12} />
                    Import Data
                  </button>
                </div>
              </div>
              
              <div className="border-t pt-4">
                <h4 className="text-sm font-medium mb-2">About</h4>
                <p className="text-xs text-gray-600">
                  Memtier Benchmark Extension v1.0.0
                </p>
                <p className="text-xs text-gray-600">
                  A productivity tool for Redis/Memcache benchmarking
                </p>
              </div>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="popup-container">
      {/* Header */}
      <div className="popup-header">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold text-gray-900">Memtier Benchmark</h1>
          {isRunning && (
            <div className="flex items-center space-x-2 text-blue-600">
              <Play size={16} className="animate-pulse" />
              <span className="text-sm">Running...</span>
            </div>
          )}
        </div>
        
        {/* Tabs */}
        <div className="tab-container mt-4">
          <div className="tab-list">
            <button
              className={`tab-button ${activeTab === 'config' ? 'active' : ''}`}
              onClick={() => setActiveTab('config')}
            >
              <Settings size={14} />
              Config
            </button>
            <button
              className={`tab-button ${activeTab === 'results' ? 'active' : ''}`}
              onClick={() => setActiveTab('results')}
            >
              <History size={14} />
              Results ({results.length})
            </button>
            <button
              className={`tab-button ${activeTab === 'presets' ? 'active' : ''}`}
              onClick={() => setActiveTab('presets')}
            >
              <BookOpen size={14} />
              Presets ({presets.length})
            </button>
            <button
              className={`tab-button ${activeTab === 'settings' ? 'active' : ''}`}
              onClick={() => setActiveTab('settings')}
            >
              <Settings size={14} />
              Settings
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="popup-content">
        <div className="tab-panel">
          {renderTabContent()}
        </div>
      </div>
    </div>
  );
};

// Initialize the popup
const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(<PopupApp />);
}
