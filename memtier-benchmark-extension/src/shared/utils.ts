import { BenchmarkStats } from './types';

/**
 * Generate a unique ID
 */
export function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

/**
 * Format duration in milliseconds to human readable string
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  
  if (minutes < 60) {
    return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
  }
  
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  
  return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
}

/**
 * Format bytes to human readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

/**
 * Format number with thousands separator
 */
export function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Format percentage
 */
export function formatPercentage(value: number, total: number): string {
  if (total === 0) return '0%';
  return `${((value / total) * 100).toFixed(1)}%`;
}

/**
 * Parse memtier benchmark output to extract statistics
 */
export function parseBenchmarkOutput(output: string): BenchmarkStats | null {
  try {
    const lines = output.split('\n');
    const stats: Partial<BenchmarkStats> = {};
    
    // Look for summary statistics
    for (const line of lines) {
      // Total requests
      const requestsMatch = line.match(/(\d+)\s+requests/);
      if (requestsMatch) {
        stats.totalRequests = parseInt(requestsMatch[1]);
      }
      
      // Requests per second
      const rpsMatch = line.match(/([\d.]+)\s+requests\/sec/);
      if (rpsMatch) {
        stats.requestsPerSecond = parseFloat(rpsMatch[1]);
      }
      
      // Average latency
      const avgLatencyMatch = line.match(/avg:\s*([\d.]+)/);
      if (avgLatencyMatch) {
        stats.avgLatency = parseFloat(avgLatencyMatch[1]);
      }
      
      // Percentile latencies
      const p50Match = line.match(/p50:\s*([\d.]+)/);
      if (p50Match) {
        stats.p50Latency = parseFloat(p50Match[1]);
      }
      
      const p90Match = line.match(/p90:\s*([\d.]+)/);
      if (p90Match) {
        stats.p90Latency = parseFloat(p90Match[1]);
      }
      
      const p95Match = line.match(/p95:\s*([\d.]+)/);
      if (p95Match) {
        stats.p95Latency = parseFloat(p95Match[1]);
      }
      
      const p99Match = line.match(/p99:\s*([\d.]+)/);
      if (p99Match) {
        stats.p99Latency = parseFloat(p99Match[1]);
      }
      
      const p999Match = line.match(/p99\.9:\s*([\d.]+)/);
      if (p999Match) {
        stats.p999Latency = parseFloat(p999Match[1]);
      }
      
      // Errors
      const errorsMatch = line.match(/(\d+)\s+errors/);
      if (errorsMatch) {
        stats.errors = parseInt(errorsMatch[1]);
      }
      
      // Total time
      const timeMatch = line.match(/in\s+([\d.]+)\s+seconds/);
      if (timeMatch) {
        stats.totalTime = parseFloat(timeMatch[1]);
      }
    }
    
    // Return stats if we have at least some basic metrics
    if (stats.totalRequests || stats.requestsPerSecond) {
      return {
        totalRequests: stats.totalRequests || 0,
        totalTime: stats.totalTime || 0,
        requestsPerSecond: stats.requestsPerSecond || 0,
        avgLatency: stats.avgLatency || 0,
        p50Latency: stats.p50Latency || 0,
        p90Latency: stats.p90Latency || 0,
        p95Latency: stats.p95Latency || 0,
        p99Latency: stats.p99Latency || 0,
        p999Latency: stats.p999Latency || 0,
        errors: stats.errors || 0
      };
    }
    
    return null;
  } catch (error) {
    console.error('Error parsing benchmark output:', error);
    return null;
  }
}

/**
 * Validate email format
 */
export function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

/**
 * Validate URL format
 */
export function isValidUrl(url: string): boolean {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

/**
 * Debounce function calls
 */
export function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

/**
 * Throttle function calls
 */
export function throttle<T extends (...args: any[]) => any>(
  func: T,
  limit: number
): (...args: Parameters<T>) => void {
  let inThrottle: boolean;
  
  return (...args: Parameters<T>) => {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, limit);
    }
  };
}

/**
 * Deep clone an object
 */
export function deepClone<T>(obj: T): T {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }
  
  if (obj instanceof Date) {
    return new Date(obj.getTime()) as unknown as T;
  }
  
  if (obj instanceof Array) {
    return obj.map(item => deepClone(item)) as unknown as T;
  }
  
  if (typeof obj === 'object') {
    const cloned = {} as T;
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        cloned[key] = deepClone(obj[key]);
      }
    }
    return cloned;
  }
  
  return obj;
}

/**
 * Check if two objects are deeply equal
 */
export function deepEqual(a: any, b: any): boolean {
  if (a === b) return true;
  
  if (a == null || b == null) return false;
  
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false;
    }
    return true;
  }
  
  if (typeof a === 'object' && typeof b === 'object') {
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);
    
    if (keysA.length !== keysB.length) return false;
    
    for (const key of keysA) {
      if (!keysB.includes(key)) return false;
      if (!deepEqual(a[key], b[key])) return false;
    }
    
    return true;
  }
  
  return false;
}

/**
 * Escape HTML to prevent XSS
 */
export function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

/**
 * Download data as file
 */
export function downloadAsFile(data: string, filename: string, mimeType: string = 'text/plain'): void {
  const blob = new Blob([data], { type: mimeType });
  const url = URL.createObjectURL(blob);
  
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  
  URL.revokeObjectURL(url);
}

/**
 * Copy text to clipboard
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (error) {
    console.error('Failed to copy to clipboard:', error);
    return false;
  }
}
