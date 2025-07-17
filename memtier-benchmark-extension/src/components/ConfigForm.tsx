import React, { useState, useEffect } from 'react';
import { MemtierConfig, ParameterGroup, PARAMETER_GROUPS } from '../shared/types';
import { BenchmarkConfigManager } from '../shared/benchmark-config';
import { ChevronDown, ChevronRight, Save, Play, RotateCcw } from 'lucide-react';

interface ConfigFormProps {
  config: MemtierConfig;
  onChange: (config: MemtierConfig) => void;
  onRun: (config: MemtierConfig) => void;
  onSave?: (config: MemtierConfig) => void;
  isRunning?: boolean;
  compact?: boolean;
}

export const ConfigForm: React.FC<ConfigFormProps> = ({
  config,
  onChange,
  onRun,
  onSave,
  isRunning = false,
  compact = false
}) => {
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  useEffect(() => {
    const errors = BenchmarkConfigManager.validateConfig(config);
    setValidationErrors(errors);
  }, [config]);

  const toggleGroup = (groupId: string) => {
    const newCollapsed = new Set(collapsedGroups);
    if (newCollapsed.has(groupId)) {
      newCollapsed.delete(groupId);
    } else {
      newCollapsed.add(groupId);
    }
    setCollapsedGroups(newCollapsed);
  };

  const handleInputChange = (field: keyof MemtierConfig, value: any) => {
    onChange({ ...config, [field]: value });
  };

  const handleReset = () => {
    onChange(BenchmarkConfigManager.mergeWithDefaults({}));
  };

  const handleRun = () => {
    if (validationErrors.length === 0) {
      onRun(config);
    }
  };

  const renderInput = (field: keyof MemtierConfig, label: string, type: string = 'text', options?: string[]) => {
    const value = config[field];
    
    if (type === 'boolean') {
      return (
        <div className="flex items-center space-x-2">
          <input
            type="checkbox"
            id={field}
            checked={value as boolean}
            onChange={(e) => handleInputChange(field, e.target.checked)}
            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          <label htmlFor={field} className="text-sm font-medium text-gray-700">
            {label}
          </label>
        </div>
      );
    }

    if (type === 'select' && options) {
      return (
        <div>
          <label htmlFor={field} className="block text-sm font-medium text-gray-700 mb-1">
            {label}
          </label>
          <select
            id={field}
            value={value as string}
            onChange={(e) => handleInputChange(field, e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {options.map(option => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </div>
      );
    }

    return (
      <div>
        <label htmlFor={field} className="block text-sm font-medium text-gray-700 mb-1">
          {label}
        </label>
        <input
          type={type}
          id={field}
          value={value as string | number}
          onChange={(e) => {
            const newValue = type === 'number' ? 
              (e.target.value === '' ? 0 : Number(e.target.value)) : 
              e.target.value;
            handleInputChange(field, newValue);
          }}
          className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder={`Enter ${label.toLowerCase()}`}
        />
      </div>
    );
  };

  const renderParameterGroup = (group: ParameterGroup) => {
    const isCollapsed = collapsedGroups.has(group.id);
    
    return (
      <div key={group.id} className="border border-gray-200 rounded-lg mb-4">
        <button
          onClick={() => toggleGroup(group.id)}
          className="w-full px-4 py-3 flex items-center justify-between bg-gray-50 hover:bg-gray-100 rounded-t-lg"
        >
          <div className="flex items-center space-x-2">
            {isCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            <h3 className="font-medium text-gray-900">{group.name}</h3>
          </div>
          <span className="text-sm text-gray-500">{group.description}</span>
        </button>
        
        {!isCollapsed && (
          <div className="p-4 space-y-4">
            {group.parameters.map(param => {
              switch (param) {
                case 'server':
                  return renderInput('server', 'Server Address');
                case 'port':
                  return renderInput('port', 'Port', 'number');
                case 'protocol':
                  return renderInput('protocol', 'Protocol', 'select', 
                    ['redis', 'resp2', 'resp3', 'memcache_text', 'memcache_binary']);
                case 'authenticate':
                  return renderInput('authenticate', 'Authentication');
                case 'tls':
                  return renderInput('tls', 'Enable TLS', 'boolean');
                case 'clients':
                  return renderInput('clients', 'Number of Clients', 'number');
                case 'threads':
                  return renderInput('threads', 'Number of Threads', 'number');
                case 'requests':
                  return renderInput('requests', 'Total Requests', 'number');
                case 'ratio':
                  return renderInput('ratio', 'Set:Get Ratio (e.g., 1:10)');
                case 'pipeline':
                  return renderInput('pipeline', 'Pipeline Size', 'number');
                case 'dataSize':
                  return renderInput('dataSize', 'Data Size (bytes)', 'number');
                case 'keyPrefix':
                  return renderInput('keyPrefix', 'Key Prefix');
                case 'keyMinimum':
                  return renderInput('keyMinimum', 'Key Minimum', 'number');
                case 'keyMaximum':
                  return renderInput('keyMaximum', 'Key Maximum', 'number');
                case 'keyPattern':
                  return renderInput('keyPattern', 'Key Pattern (e.g., R:R)');
                case 'debug':
                  return renderInput('debug', 'Debug Mode', 'boolean');
                case 'randomData':
                  return renderInput('randomData', 'Random Data', 'boolean');
                default:
                  return null;
              }
            })}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={`${compact ? 'space-y-3' : 'space-y-4'}`}>
      {/* Action Buttons */}
      <div className="flex space-x-2 mb-4">
        <button
          onClick={handleRun}
          disabled={isRunning || validationErrors.length > 0}
          className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Play size={16} />
          <span>{isRunning ? 'Running...' : 'Run Benchmark'}</span>
        </button>
        
        {onSave && (
          <button
            onClick={() => onSave(config)}
            className="flex items-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700"
          >
            <Save size={16} />
            <span>Save Config</span>
          </button>
        )}
        
        <button
          onClick={handleReset}
          className="flex items-center space-x-2 px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700"
        >
          <RotateCcw size={16} />
          <span>Reset</span>
        </button>
      </div>

      {/* Validation Errors */}
      {validationErrors.length > 0 && (
        <div className="bg-red-50 border border-red-200 rounded-md p-3 mb-4">
          <h4 className="text-sm font-medium text-red-800 mb-2">Configuration Errors:</h4>
          <ul className="text-sm text-red-700 space-y-1">
            {validationErrors.map((error, index) => (
              <li key={index}>• {error}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Parameter Groups */}
      <div className="space-y-2">
        {PARAMETER_GROUPS.map(renderParameterGroup)}
      </div>

      {/* Command Preview */}
      <div className="mt-6 p-4 bg-gray-50 rounded-lg">
        <h4 className="text-sm font-medium text-gray-700 mb-2">Command Preview:</h4>
        <code className="text-xs text-gray-600 break-all">
          memtier_benchmark {BenchmarkConfigManager.configToArgs(config).join(' ')}
        </code>
      </div>
    </div>
  );
};
