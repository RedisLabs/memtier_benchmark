import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import { ExtensionSettings } from '../shared/types';
import { StorageManager } from '../shared/storage';
import { 
  Save, 
  RotateCcw, 
  Download, 
  Upload, 
  Trash2,
  Info,
  Keyboard,
  Database,
  Palette
} from 'lucide-react';

const OptionsApp: React.FC = () => {
  const [settings, setSettings] = useState<ExtensionSettings>({
    defaultConfig: {},
    showSidebar: false,
    autoSaveResults: true,
    maxStoredResults: 50,
    theme: 'auto'
  });
  const [storageInfo, setStorageInfo] = useState({
    syncUsed: 0,
    syncQuota: 0,
    localUsed: 0,
    localQuota: 0
  });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error' | 'info'; text: string } | null>(null);

  useEffect(() => {
    loadSettings();
    loadStorageInfo();
  }, []);

  const loadSettings = async () => {
    try {
      const loadedSettings = await StorageManager.loadSettings();
      setSettings(loadedSettings);
    } catch (error) {
      showMessage('error', 'Failed to load settings');
    } finally {
      setLoading(false);
    }
  };

  const loadStorageInfo = async () => {
    try {
      const info = await StorageManager.getStorageInfo();
      setStorageInfo(info);
    } catch (error) {
      console.error('Failed to load storage info:', error);
    }
  };

  const showMessage = (type: 'success' | 'error' | 'info', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 5000);
  };

  const handleSettingChange = (key: keyof ExtensionSettings, value: any) => {
    setSettings(prev => ({ ...prev, [key]: value }));
  };

  const handleSaveSettings = async () => {
    setSaving(true);
    try {
      await StorageManager.saveSettings(settings);
      showMessage('success', 'Settings saved successfully');
      await loadStorageInfo(); // Refresh storage info
    } catch (error) {
      showMessage('error', 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  const handleResetSettings = async () => {
    if (confirm('Are you sure you want to reset all settings to defaults?')) {
      const defaultSettings: ExtensionSettings = {
        defaultConfig: {},
        showSidebar: false,
        autoSaveResults: true,
        maxStoredResults: 50,
        theme: 'auto'
      };
      
      setSettings(defaultSettings);
      try {
        await StorageManager.saveSettings(defaultSettings);
        showMessage('success', 'Settings reset to defaults');
      } catch (error) {
        showMessage('error', 'Failed to reset settings');
      }
    }
  };

  const handleExportData = async () => {
    try {
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
      
      showMessage('success', 'Data exported successfully');
    } catch (error) {
      showMessage('error', 'Failed to export data');
    }
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
        await loadSettings();
        await loadStorageInfo();
        showMessage('success', 'Data imported successfully');
      } catch (error) {
        showMessage('error', 'Failed to import data: Invalid file format');
      }
    };
    input.click();
  };

  const handleClearAllData = async () => {
    if (confirm('Are you sure you want to clear ALL extension data? This cannot be undone.')) {
      try {
        await StorageManager.clearBenchmarkResults();
        await chrome.storage.sync.clear();
        await chrome.storage.local.clear();
        await loadSettings();
        await loadStorageInfo();
        showMessage('success', 'All data cleared successfully');
      } catch (error) {
        showMessage('error', 'Failed to clear data');
      }
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  };

  const getStoragePercentage = (used: number, quota: number) => {
    return quota > 0 ? (used / quota) * 100 : 0;
  };

  if (loading) {
    return <div className="loading"></div>;
  }

  return (
    <div>
      {message && (
        <div className={`alert alert-${message.type}`}>
          {message.text}
        </div>
      )}

      <div className="grid">
        {/* General Settings */}
        <div className="section">
          <h2>
            <Palette size={20} style={{ display: 'inline', marginRight: '8px' }} />
            General Settings
          </h2>
          <p>Configure basic extension behavior and appearance.</p>
          
          <div className="form-group">
            <label className="form-label">Theme</label>
            <select
              className="form-select"
              value={settings.theme}
              onChange={(e) => handleSettingChange('theme', e.target.value as 'light' | 'dark' | 'auto')}
            >
              <option value="auto">Auto (System)</option>
              <option value="light">Light</option>
              <option value="dark">Dark</option>
            </select>
          </div>

          <div className="checkbox-group">
            <input
              type="checkbox"
              id="showSidebar"
              className="form-checkbox"
              checked={settings.showSidebar}
              onChange={(e) => handleSettingChange('showSidebar', e.target.checked)}
            />
            <label htmlFor="showSidebar" className="form-label">Show sidebar by default</label>
          </div>

          <div className="checkbox-group">
            <input
              type="checkbox"
              id="autoSaveResults"
              className="form-checkbox"
              checked={settings.autoSaveResults}
              onChange={(e) => handleSettingChange('autoSaveResults', e.target.checked)}
            />
            <label htmlFor="autoSaveResults" className="form-label">Auto-save benchmark results</label>
          </div>

          <div className="form-group">
            <label className="form-label">Maximum stored results</label>
            <input
              type="number"
              className="form-input"
              min="10"
              max="1000"
              value={settings.maxStoredResults}
              onChange={(e) => handleSettingChange('maxStoredResults', parseInt(e.target.value))}
            />
            <p style={{ fontSize: '12px', color: '#6b7280', marginTop: '4px' }}>
              Older results will be automatically deleted when this limit is reached.
            </p>
          </div>
        </div>

        {/* Storage Information */}
        <div className="section">
          <h2>
            <Database size={20} style={{ display: 'inline', marginRight: '8px' }} />
            Storage Information
          </h2>
          <p>Monitor your extension's storage usage.</p>

          <div className="storage-info">
            <h4>Sync Storage (Settings & Presets)</h4>
            <div className="storage-bar">
              <div 
                className="storage-bar-fill" 
                style={{ width: `${getStoragePercentage(storageInfo.syncUsed, storageInfo.syncQuota)}%` }}
              ></div>
            </div>
            <div className="storage-text">
              {formatBytes(storageInfo.syncUsed)} / {formatBytes(storageInfo.syncQuota)} used
              ({getStoragePercentage(storageInfo.syncUsed, storageInfo.syncQuota).toFixed(1)}%)
            </div>
          </div>

          <div className="storage-info">
            <h4>Local Storage (Results & Cache)</h4>
            <div className="storage-bar">
              <div 
                className="storage-bar-fill" 
                style={{ width: `${getStoragePercentage(storageInfo.localUsed, storageInfo.localQuota)}%` }}
              ></div>
            </div>
            <div className="storage-text">
              {formatBytes(storageInfo.localUsed)} / {formatBytes(storageInfo.localQuota)} used
              ({getStoragePercentage(storageInfo.localUsed, storageInfo.localQuota).toFixed(1)}%)
            </div>
          </div>
        </div>
      </div>

      {/* Data Management */}
      <div className="section">
        <h2>
          <Database size={20} style={{ display: 'inline', marginRight: '8px' }} />
          Data Management
        </h2>
        <p>Export, import, or clear your extension data.</p>

        <div className="button-group">
          <button onClick={handleExportData} className="btn btn-secondary">
            <Download size={16} />
            Export Data
          </button>
          <button onClick={handleImportData} className="btn btn-secondary">
            <Upload size={16} />
            Import Data
          </button>
          <button onClick={handleClearAllData} className="btn btn-danger">
            <Trash2 size={16} />
            Clear All Data
          </button>
        </div>
      </div>

      {/* Keyboard Shortcuts */}
      <div className="section">
        <h2>
          <Keyboard size={20} style={{ display: 'inline', marginRight: '8px' }} />
          Keyboard Shortcuts
        </h2>
        <p>Available keyboard shortcuts for quick access.</p>

        <ul className="shortcut-list">
          <li>
            <span>Toggle Sidebar</span>
            <span className="keyboard-shortcut">Ctrl + Shift + M</span>
          </li>
          <li>
            <span>Open Extension Popup</span>
            <span className="keyboard-shortcut">Ctrl + Shift + B</span>
          </li>
          <li>
            <span>Focus on Extension Icon</span>
            <span className="keyboard-shortcut">Alt + Shift + M</span>
          </li>
        </ul>
      </div>

      {/* About */}
      <div className="section">
        <h2>
          <Info size={20} style={{ display: 'inline', marginRight: '8px' }} />
          About
        </h2>
        <p>Information about the Memtier Benchmark Extension.</p>

        <div style={{ fontSize: '14px', lineHeight: '1.6' }}>
          <p><strong>Version:</strong> 1.0.0</p>
          <p><strong>Description:</strong> A productivity tool for running Redis/Memcache benchmarks with memtier_benchmark</p>
          <p><strong>Features:</strong></p>
          <ul style={{ marginLeft: '20px' }}>
            <li>Configurable benchmark parameters</li>
            <li>Real-time results monitoring</li>
            <li>Configuration presets</li>
            <li>Cross-device sync</li>
            <li>Export/import functionality</li>
          </ul>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="section">
        <div className="button-group">
          <button 
            onClick={handleSaveSettings} 
            className="btn btn-primary"
            disabled={saving}
          >
            <Save size={16} />
            {saving ? 'Saving...' : 'Save Settings'}
          </button>
          <button onClick={handleResetSettings} className="btn btn-secondary">
            <RotateCcw size={16} />
            Reset to Defaults
          </button>
        </div>
      </div>
    </div>
  );
};

// Initialize the options page
const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(<OptionsApp />);
}
