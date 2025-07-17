import { MemtierConfig, BenchmarkResult, ConfigPreset, ExtensionSettings } from './types';
import { DEFAULT_CONFIG } from './benchmark-config';

export class StorageManager {
  private static readonly KEYS = {
    CURRENT_CONFIG: 'currentConfig',
    RESULTS: 'benchmarkResults',
    PRESETS: 'configPresets',
    SETTINGS: 'extensionSettings'
  };

  /**
   * Save current benchmark configuration
   */
  static async saveCurrentConfig(config: MemtierConfig): Promise<void> {
    await chrome.storage.sync.set({
      [this.KEYS.CURRENT_CONFIG]: config
    });
  }

  /**
   * Load current benchmark configuration
   */
  static async loadCurrentConfig(): Promise<MemtierConfig> {
    const result = await chrome.storage.sync.get(this.KEYS.CURRENT_CONFIG);
    return result[this.KEYS.CURRENT_CONFIG] || DEFAULT_CONFIG;
  }

  /**
   * Save benchmark result
   */
  static async saveBenchmarkResult(result: BenchmarkResult): Promise<void> {
    const results = await this.loadBenchmarkResults();
    results.unshift(result); // Add to beginning
    
    // Keep only the most recent results based on settings
    const settings = await this.loadSettings();
    const maxResults = settings.maxStoredResults || 50;
    const trimmedResults = results.slice(0, maxResults);
    
    await chrome.storage.local.set({
      [this.KEYS.RESULTS]: trimmedResults
    });
  }

  /**
   * Load all benchmark results
   */
  static async loadBenchmarkResults(): Promise<BenchmarkResult[]> {
    const result = await chrome.storage.local.get(this.KEYS.RESULTS);
    return result[this.KEYS.RESULTS] || [];
  }

  /**
   * Delete benchmark result by ID
   */
  static async deleteBenchmarkResult(id: string): Promise<void> {
    const results = await this.loadBenchmarkResults();
    const filteredResults = results.filter(r => r.id !== id);
    
    await chrome.storage.local.set({
      [this.KEYS.RESULTS]: filteredResults
    });
  }

  /**
   * Clear all benchmark results
   */
  static async clearBenchmarkResults(): Promise<void> {
    await chrome.storage.local.remove(this.KEYS.RESULTS);
  }

  /**
   * Save configuration preset
   */
  static async saveConfigPreset(preset: ConfigPreset): Promise<void> {
    const presets = await this.loadConfigPresets();
    const existingIndex = presets.findIndex(p => p.id === preset.id);
    
    if (existingIndex >= 0) {
      presets[existingIndex] = { ...preset, updatedAt: Date.now() };
    } else {
      presets.push(preset);
    }
    
    await chrome.storage.sync.set({
      [this.KEYS.PRESETS]: presets
    });
  }

  /**
   * Load all configuration presets
   */
  static async loadConfigPresets(): Promise<ConfigPreset[]> {
    const result = await chrome.storage.sync.get(this.KEYS.PRESETS);
    return result[this.KEYS.PRESETS] || [];
  }

  /**
   * Delete configuration preset by ID
   */
  static async deleteConfigPreset(id: string): Promise<void> {
    const presets = await this.loadConfigPresets();
    const filteredPresets = presets.filter(p => p.id !== id);
    
    await chrome.storage.sync.set({
      [this.KEYS.PRESETS]: filteredPresets
    });
  }

  /**
   * Load configuration preset by ID
   */
  static async loadConfigPreset(id: string): Promise<ConfigPreset | null> {
    const presets = await this.loadConfigPresets();
    return presets.find(p => p.id === id) || null;
  }

  /**
   * Save extension settings
   */
  static async saveSettings(settings: Partial<ExtensionSettings>): Promise<void> {
    const currentSettings = await this.loadSettings();
    const updatedSettings = { ...currentSettings, ...settings };
    
    await chrome.storage.sync.set({
      [this.KEYS.SETTINGS]: updatedSettings
    });
  }

  /**
   * Load extension settings
   */
  static async loadSettings(): Promise<ExtensionSettings> {
    const result = await chrome.storage.sync.get(this.KEYS.SETTINGS);
    const defaultSettings: ExtensionSettings = {
      defaultConfig: DEFAULT_CONFIG,
      showSidebar: false,
      autoSaveResults: true,
      maxStoredResults: 50,
      theme: 'auto'
    };
    
    return { ...defaultSettings, ...result[this.KEYS.SETTINGS] };
  }

  /**
   * Export all data for backup
   */
  static async exportData(): Promise<{
    config: MemtierConfig;
    results: BenchmarkResult[];
    presets: ConfigPreset[];
    settings: ExtensionSettings;
  }> {
    const [config, results, presets, settings] = await Promise.all([
      this.loadCurrentConfig(),
      this.loadBenchmarkResults(),
      this.loadConfigPresets(),
      this.loadSettings()
    ]);

    return { config, results, presets, settings };
  }

  /**
   * Import data from backup
   */
  static async importData(data: {
    config?: MemtierConfig;
    results?: BenchmarkResult[];
    presets?: ConfigPreset[];
    settings?: ExtensionSettings;
  }): Promise<void> {
    const promises: Promise<void>[] = [];

    if (data.config) {
      promises.push(this.saveCurrentConfig(data.config));
    }

    if (data.results) {
      promises.push(chrome.storage.local.set({
        [this.KEYS.RESULTS]: data.results
      }));
    }

    if (data.presets) {
      promises.push(chrome.storage.sync.set({
        [this.KEYS.PRESETS]: data.presets
      }));
    }

    if (data.settings) {
      promises.push(this.saveSettings(data.settings));
    }

    await Promise.all(promises);
  }

  /**
   * Get storage usage information
   */
  static async getStorageInfo(): Promise<{
    syncUsed: number;
    syncQuota: number;
    localUsed: number;
    localQuota: number;
  }> {
    const [syncUsage, localUsage] = await Promise.all([
      chrome.storage.sync.getBytesInUse(),
      chrome.storage.local.getBytesInUse()
    ]);

    return {
      syncUsed: syncUsage,
      syncQuota: chrome.storage.sync.QUOTA_BYTES,
      localUsed: localUsage,
      localQuota: chrome.storage.local.QUOTA_BYTES
    };
  }

  /**
   * Listen for storage changes
   */
  static onStorageChanged(callback: (changes: { [key: string]: chrome.storage.StorageChange }) => void): void {
    chrome.storage.onChanged.addListener(callback);
  }

  /**
   * Remove storage change listener
   */
  static removeStorageListener(callback: (changes: { [key: string]: chrome.storage.StorageChange }) => void): void {
    chrome.storage.onChanged.removeListener(callback);
  }
}
