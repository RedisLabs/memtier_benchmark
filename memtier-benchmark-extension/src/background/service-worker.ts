import { Message, BenchmarkResult, MemtierConfig } from '../shared/types';
import { BenchmarkConfigManager } from '../shared/benchmark-config';
import { StorageManager } from '../shared/storage';
import { generateId, parseBenchmarkOutput } from '../shared/utils';

class BenchmarkRunner {
  private runningBenchmarks = new Map<string, {
    process: any;
    result: BenchmarkResult;
  }>();

  constructor() {
    this.setupMessageHandlers();
  }

  private setupMessageHandlers(): void {
    chrome.runtime.onMessage.addListener((message: Message, sender, sendResponse) => {
      this.handleMessage(message, sender, sendResponse);
      return true; // Keep message channel open for async response
    });
  }

  private async handleMessage(message: Message, sender: chrome.runtime.MessageSender, sendResponse: (response?: any) => void): Promise<void> {
    try {
      switch (message.type) {
        case 'RUN_BENCHMARK':
          await this.runBenchmark(message.payload as MemtierConfig, sendResponse);
          break;
        
        case 'GET_RESULTS':
          const results = await StorageManager.loadBenchmarkResults();
          sendResponse({ results });
          break;
        
        case 'SAVE_CONFIG':
          await StorageManager.saveCurrentConfig(message.payload as MemtierConfig);
          sendResponse({ success: true });
          break;
        
        case 'LOAD_CONFIG':
          const config = await StorageManager.loadCurrentConfig();
          sendResponse({ config });
          break;
        
        case 'TOGGLE_SIDEBAR':
          await this.toggleSidebar(sender.tab?.id);
          sendResponse({ success: true });
          break;
        
        default:
          sendResponse({ error: 'Unknown message type' });
      }
    } catch (error) {
      console.error('Error handling message:', error);
      sendResponse({ error: error.message });
    }
  }

  private async runBenchmark(config: MemtierConfig, sendResponse: (response?: any) => void): Promise<void> {
    const benchmarkId = generateId();
    
    // Validate configuration
    const validationErrors = BenchmarkConfigManager.validateConfig(config);
    if (validationErrors.length > 0) {
      sendResponse({ 
        error: 'Configuration validation failed', 
        details: validationErrors 
      });
      return;
    }

    // Create benchmark result object
    const result: BenchmarkResult = {
      id: benchmarkId,
      timestamp: Date.now(),
      config,
      output: '',
      status: 'running'
    };

    // Convert config to command line arguments
    const args = BenchmarkConfigManager.configToArgs(config);
    const command = `memtier_benchmark ${args.join(' ')}`;

    try {
      // For Chrome extension, we'll simulate the benchmark execution
      // In a real implementation, you would need a native messaging host
      // or a local server to execute the actual memtier_benchmark command
      
      result.output = `Starting benchmark with command:\n${command}\n\n`;
      
      // Store the running benchmark
      this.runningBenchmarks.set(benchmarkId, { process: null, result });
      
      // Send initial response with benchmark ID
      sendResponse({ 
        success: true, 
        benchmarkId,
        message: 'Benchmark started'
      });

      // Simulate benchmark execution
      await this.simulateBenchmarkExecution(benchmarkId, config);
      
    } catch (error) {
      console.error('Error running benchmark:', error);
      result.status = 'failed';
      result.output += `\nError: ${error.message}`;
      
      await StorageManager.saveBenchmarkResult(result);
      this.runningBenchmarks.delete(benchmarkId);
      
      // Notify all tabs about the failure
      this.broadcastBenchmarkUpdate(benchmarkId, result);
    }
  }

  private async simulateBenchmarkExecution(benchmarkId: string, config: MemtierConfig): Promise<void> {
    const benchmarkData = this.runningBenchmarks.get(benchmarkId);
    if (!benchmarkData) return;

    const { result } = benchmarkData;
    const startTime = Date.now();

    try {
      // Simulate benchmark progress
      const totalSteps = 10;
      for (let step = 1; step <= totalSteps; step++) {
        await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
        
        const progress = (step / totalSteps) * 100;
        result.output += `Progress: ${progress.toFixed(0)}% - Processing requests...\n`;
        
        // Broadcast progress update
        this.broadcastBenchmarkUpdate(benchmarkId, result);
      }

      // Simulate final results
      const duration = Date.now() - startTime;
      const simulatedOutput = this.generateSimulatedOutput(config, duration);
      
      result.output += simulatedOutput;
      result.status = 'completed';
      result.duration = duration;
      result.stats = parseBenchmarkOutput(simulatedOutput);

      // Save the completed result
      await StorageManager.saveBenchmarkResult(result);
      
      // Clean up
      this.runningBenchmarks.delete(benchmarkId);
      
      // Broadcast completion
      this.broadcastBenchmarkUpdate(benchmarkId, result);
      
    } catch (error) {
      result.status = 'failed';
      result.output += `\nBenchmark failed: ${error.message}`;
      
      await StorageManager.saveBenchmarkResult(result);
      this.runningBenchmarks.delete(benchmarkId);
      
      this.broadcastBenchmarkUpdate(benchmarkId, result);
    }
  }

  private generateSimulatedOutput(config: MemtierConfig, duration: number): string {
    const requests = typeof config.requests === 'number' ? config.requests : 10000;
    const rps = Math.floor(requests / (duration / 1000));
    const avgLatency = Math.random() * 10 + 1; // 1-11ms
    
    return `
========== Results ==========
Total requests: ${requests}
Total time: ${(duration / 1000).toFixed(2)} seconds
Requests per second: ${rps}
Average latency: ${avgLatency.toFixed(2)} ms

Latency percentiles:
p50: ${(avgLatency * 0.8).toFixed(2)} ms
p90: ${(avgLatency * 1.5).toFixed(2)} ms
p95: ${(avgLatency * 2.0).toFixed(2)} ms
p99: ${(avgLatency * 3.0).toFixed(2)} ms
p99.9: ${(avgLatency * 5.0).toFixed(2)} ms

Errors: 0
============================
`;
  }

  private broadcastBenchmarkUpdate(benchmarkId: string, result: BenchmarkResult): void {
    // Send update to all tabs
    chrome.tabs.query({}, (tabs) => {
      tabs.forEach(tab => {
        if (tab.id) {
          chrome.tabs.sendMessage(tab.id, {
            type: 'BENCHMARK_STATUS',
            payload: { benchmarkId, result }
          }).catch(() => {
            // Ignore errors for tabs that don't have content script
          });
        }
      });
    });
  }

  private async toggleSidebar(tabId?: number): Promise<void> {
    if (!tabId) return;

    try {
      await chrome.scripting.executeScript({
        target: { tabId },
        func: () => {
          // Toggle sidebar visibility
          const sidebar = document.getElementById('memtier-benchmark-sidebar');
          if (sidebar) {
            sidebar.style.display = sidebar.style.display === 'none' ? 'block' : 'none';
          } else {
            // Create and inject sidebar
            const sidebarFrame = document.createElement('iframe');
            sidebarFrame.id = 'memtier-benchmark-sidebar';
            sidebarFrame.src = chrome.runtime.getURL('src/sidebar/sidebar.html');
            sidebarFrame.style.cssText = `
              position: fixed;
              top: 0;
              right: 0;
              width: 400px;
              height: 100vh;
              border: none;
              z-index: 10000;
              background: white;
              box-shadow: -2px 0 10px rgba(0,0,0,0.1);
            `;
            document.body.appendChild(sidebarFrame);
          }
        }
      });
    } catch (error) {
      console.error('Error toggling sidebar:', error);
    }
  }
}

// Initialize the benchmark runner
new BenchmarkRunner();

// Handle extension installation
chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === 'install') {
    console.log('Memtier Benchmark Extension installed');
    
    // Set up default configuration
    StorageManager.loadCurrentConfig().then(config => {
      if (!config) {
        StorageManager.saveCurrentConfig(BenchmarkConfigManager.mergeWithDefaults({}));
      }
    });
  }
});

// Handle extension startup
chrome.runtime.onStartup.addListener(() => {
  console.log('Memtier Benchmark Extension started');
});

// Export for testing
export { BenchmarkRunner };
