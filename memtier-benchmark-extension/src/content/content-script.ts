import { Message, BenchmarkResult } from '../shared/types';

class ContentScript {
  private sidebar: HTMLIFrameElement | null = null;
  private isInitialized = false;

  constructor() {
    this.initialize();
  }

  private initialize(): void {
    if (this.isInitialized) return;
    
    this.setupMessageHandlers();
    this.setupKeyboardShortcuts();
    this.isInitialized = true;
    
    console.log('Memtier Benchmark content script initialized');
  }

  private setupMessageHandlers(): void {
    // Listen for messages from background script
    chrome.runtime.onMessage.addListener((message: Message, sender, sendResponse) => {
      this.handleMessage(message, sender, sendResponse);
      return true;
    });

    // Listen for messages from sidebar iframe
    window.addEventListener('message', (event) => {
      if (event.source !== window && event.data?.source === 'memtier-sidebar') {
        this.handleSidebarMessage(event.data);
      }
    });
  }

  private setupKeyboardShortcuts(): void {
    document.addEventListener('keydown', (event) => {
      // Ctrl+Shift+M to toggle sidebar
      if (event.ctrlKey && event.shiftKey && event.key === 'M') {
        event.preventDefault();
        this.toggleSidebar();
      }
      
      // Ctrl+Shift+B to open popup (focus on extension icon)
      if (event.ctrlKey && event.shiftKey && event.key === 'B') {
        event.preventDefault();
        // This will be handled by the browser's built-in shortcut system
        // We can't programmatically open the popup, but we can notify the user
        this.showNotification('Press the extension icon to open Memtier Benchmark');
      }
    });
  }

  private handleMessage(message: Message, sender: chrome.runtime.MessageSender, sendResponse: (response?: any) => void): void {
    switch (message.type) {
      case 'BENCHMARK_STATUS':
        this.handleBenchmarkStatus(message.payload);
        break;
      
      case 'BENCHMARK_OUTPUT':
        this.handleBenchmarkOutput(message.payload);
        break;
      
      case 'BENCHMARK_COMPLETE':
        this.handleBenchmarkComplete(message.payload);
        break;
      
      default:
        console.log('Unknown message type:', message.type);
    }
    
    sendResponse({ received: true });
  }

  private handleSidebarMessage(data: any): void {
    switch (data.type) {
      case 'CLOSE_SIDEBAR':
        this.closeSidebar();
        break;
      
      case 'RESIZE_SIDEBAR':
        this.resizeSidebar(data.width);
        break;
      
      case 'RUN_BENCHMARK':
        // Forward to background script
        chrome.runtime.sendMessage({
          type: 'RUN_BENCHMARK',
          payload: data.config
        });
        break;
      
      default:
        console.log('Unknown sidebar message:', data.type);
    }
  }

  private handleBenchmarkStatus(payload: { benchmarkId: string; result: BenchmarkResult }): void {
    // Update sidebar if it's open
    if (this.sidebar) {
      this.sidebar.contentWindow?.postMessage({
        source: 'memtier-content',
        type: 'BENCHMARK_STATUS',
        payload
      }, '*');
    }

    // Show notification for status changes
    if (payload.result.status === 'completed') {
      this.showNotification('Benchmark completed successfully', 'success');
    } else if (payload.result.status === 'failed') {
      this.showNotification('Benchmark failed', 'error');
    }
  }

  private handleBenchmarkOutput(payload: { benchmarkId: string; output: string }): void {
    // Forward to sidebar
    if (this.sidebar) {
      this.sidebar.contentWindow?.postMessage({
        source: 'memtier-content',
        type: 'BENCHMARK_OUTPUT',
        payload
      }, '*');
    }
  }

  private handleBenchmarkComplete(payload: { benchmarkId: string; result: BenchmarkResult }): void {
    // Forward to sidebar
    if (this.sidebar) {
      this.sidebar.contentWindow?.postMessage({
        source: 'memtier-content',
        type: 'BENCHMARK_COMPLETE',
        payload
      }, '*');
    }

    // Show completion notification
    const stats = payload.result.stats;
    if (stats) {
      const message = `Benchmark completed: ${stats.requestsPerSecond.toFixed(0)} req/s, ${stats.avgLatency.toFixed(2)}ms avg latency`;
      this.showNotification(message, 'success');
    }
  }

  private toggleSidebar(): void {
    if (this.sidebar) {
      this.closeSidebar();
    } else {
      this.openSidebar();
    }
  }

  private openSidebar(): void {
    if (this.sidebar) return;

    // Create sidebar iframe
    this.sidebar = document.createElement('iframe');
    this.sidebar.id = 'memtier-benchmark-sidebar';
    this.sidebar.src = chrome.runtime.getURL('src/sidebar/sidebar.html');
    this.sidebar.style.cssText = `
      position: fixed !important;
      top: 0 !important;
      right: 0 !important;
      width: 400px !important;
      height: 100vh !important;
      border: none !important;
      z-index: 2147483647 !important;
      background: white !important;
      box-shadow: -2px 0 10px rgba(0,0,0,0.1) !important;
      transition: transform 0.3s ease !important;
      transform: translateX(0) !important;
    `;

    // Add to page
    document.body.appendChild(this.sidebar);

    // Add overlay to prevent interaction with page
    const overlay = document.createElement('div');
    overlay.id = 'memtier-benchmark-overlay';
    overlay.style.cssText = `
      position: fixed !important;
      top: 0 !important;
      left: 0 !important;
      width: 100vw !important;
      height: 100vh !important;
      background: rgba(0,0,0,0.3) !important;
      z-index: 2147483646 !important;
      backdrop-filter: blur(2px) !important;
    `;
    
    overlay.addEventListener('click', () => this.closeSidebar());
    document.body.appendChild(overlay);

    // Animate in
    requestAnimationFrame(() => {
      this.sidebar!.style.transform = 'translateX(0)';
    });
  }

  private closeSidebar(): void {
    if (!this.sidebar) return;

    // Animate out
    this.sidebar.style.transform = 'translateX(100%)';
    
    setTimeout(() => {
      // Remove sidebar
      if (this.sidebar) {
        this.sidebar.remove();
        this.sidebar = null;
      }

      // Remove overlay
      const overlay = document.getElementById('memtier-benchmark-overlay');
      if (overlay) {
        overlay.remove();
      }
    }, 300);
  }

  private resizeSidebar(width: number): void {
    if (this.sidebar) {
      this.sidebar.style.width = `${width}px`;
    }
  }

  private showNotification(message: string, type: 'info' | 'success' | 'error' = 'info'): void {
    // Create notification element
    const notification = document.createElement('div');
    notification.style.cssText = `
      position: fixed !important;
      top: 20px !important;
      right: 20px !important;
      padding: 12px 16px !important;
      border-radius: 6px !important;
      color: white !important;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
      font-size: 14px !important;
      font-weight: 500 !important;
      z-index: 2147483647 !important;
      max-width: 300px !important;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15) !important;
      transform: translateX(100%) !important;
      transition: transform 0.3s ease !important;
      ${type === 'success' ? 'background: #10b981 !important;' : ''}
      ${type === 'error' ? 'background: #ef4444 !important;' : ''}
      ${type === 'info' ? 'background: #3b82f6 !important;' : ''}
    `;
    
    notification.textContent = message;
    document.body.appendChild(notification);

    // Animate in
    requestAnimationFrame(() => {
      notification.style.transform = 'translateX(0)';
    });

    // Auto remove after 4 seconds
    setTimeout(() => {
      notification.style.transform = 'translateX(100%)';
      setTimeout(() => notification.remove(), 300);
    }, 4000);
  }

  // Public methods for external access
  public getSidebar(): HTMLIFrameElement | null {
    return this.sidebar;
  }

  public isSidebarOpen(): boolean {
    return this.sidebar !== null;
  }
}

// Initialize content script
const contentScript = new ContentScript();

// Export for testing
export { ContentScript };
