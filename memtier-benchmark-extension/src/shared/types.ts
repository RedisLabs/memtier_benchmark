// Memtier Benchmark Configuration Types
export interface MemtierConfig {
  // Connection and General Options
  server: string;
  port: number;
  unixSocket?: string;
  ipv4: boolean;
  ipv6: boolean;
  protocol: 'redis' | 'resp2' | 'resp3' | 'memcache_text' | 'memcache_binary';
  authenticate?: string;
  
  // TLS Options
  tls: boolean;
  tlsCert?: string;
  tlsKey?: string;
  tlsCacert?: string;
  tlsSkipVerify: boolean;
  tlsProtocols?: string;
  sni?: string;
  
  // Test Options
  runCount: number;
  requests: number | 'allkeys';
  rateLimiting?: number;
  clients: number;
  threads: number;
  testTime?: number;
  ratio: string; // e.g., "1:10"
  pipeline: number;
  reconnectInterval?: number;
  multiKeyGet: number;
  selectDb?: number;
  distinctClientSeed: boolean;
  randomize: boolean;
  
  // Object Options
  dataSize?: number;
  dataOffset: number;
  randomData: boolean;
  dataSizeRange?: string; // e.g., "32-1024"
  dataSizeList?: string; // e.g., "32:1,64:2,128:1"
  dataSizePattern: 'R' | 'S';
  expiryRange?: string; // e.g., "10-3600"
  
  // Imported Data Options
  dataImport?: string;
  dataVerify: boolean;
  verifyOnly: boolean;
  generateKeys: boolean;
  noExpiry: boolean;
  
  // Key Options
  keyPrefix: string;
  keyMinimum: number;
  keyMaximum: number;
  keyPattern: string; // e.g., "R:R", "G:G", "S:S", "P:P", "Z:Z"
  keyStddev?: number;
  keyMedian?: number;
  keyZipfExp?: number;
  
  // WAIT Options
  waitRatio: string; // e.g., "1:0"
  numSlaves?: string; // e.g., "1-3"
  waitTimeout?: string; // e.g., "100-1000"
  
  // Arbitrary Command Options
  commands: ArbitraryCommand[];
  
  // Output and Debug Options
  debug: boolean;
  showConfig: boolean;
  hideHistogram: boolean;
  printPercentiles?: string; // e.g., "50,90,95,99,99.9"
  printAllRuns: boolean;
  clientStats?: string;
  outFile?: string;
  jsonOutFile?: string;
  hdrFilePrefix?: string;
  clusterMode: boolean;
}

export interface ArbitraryCommand {
  command: string;
  ratio: number;
  keyPattern: 'G' | 'R' | 'S' | 'P';
}

export interface BenchmarkResult {
  id: string;
  timestamp: number;
  config: MemtierConfig;
  output: string;
  status: 'running' | 'completed' | 'failed';
  duration?: number;
  stats?: BenchmarkStats;
}

export interface BenchmarkStats {
  totalRequests: number;
  totalTime: number;
  requestsPerSecond: number;
  avgLatency: number;
  p50Latency: number;
  p90Latency: number;
  p95Latency: number;
  p99Latency: number;
  p999Latency: number;
  errors: number;
}

export interface ConfigPreset {
  id: string;
  name: string;
  description: string;
  config: MemtierConfig;
  createdAt: number;
  updatedAt: number;
}

export interface ExtensionSettings {
  defaultConfig: Partial<MemtierConfig>;
  showSidebar: boolean;
  autoSaveResults: boolean;
  maxStoredResults: number;
  theme: 'light' | 'dark' | 'auto';
}

// Message types for communication between components
export type MessageType = 
  | 'RUN_BENCHMARK'
  | 'BENCHMARK_STATUS'
  | 'BENCHMARK_OUTPUT'
  | 'BENCHMARK_COMPLETE'
  | 'GET_RESULTS'
  | 'SAVE_CONFIG'
  | 'LOAD_CONFIG'
  | 'TOGGLE_SIDEBAR';

export interface Message {
  type: MessageType;
  payload?: any;
  id?: string;
}

// Parameter group definitions for UI organization
export interface ParameterGroup {
  id: string;
  name: string;
  description: string;
  parameters: string[];
  collapsed?: boolean;
}

export const PARAMETER_GROUPS: ParameterGroup[] = [
  {
    id: 'connection',
    name: 'Connection & General',
    description: 'Server connection and basic configuration',
    parameters: ['server', 'port', 'unixSocket', 'ipv4', 'ipv6', 'protocol', 'authenticate']
  },
  {
    id: 'tls',
    name: 'TLS/SSL Options',
    description: 'Transport Layer Security configuration',
    parameters: ['tls', 'tlsCert', 'tlsKey', 'tlsCacert', 'tlsSkipVerify', 'tlsProtocols', 'sni']
  },
  {
    id: 'test',
    name: 'Test Configuration',
    description: 'Test execution parameters',
    parameters: ['runCount', 'requests', 'rateLimiting', 'clients', 'threads', 'testTime', 'ratio', 'pipeline']
  },
  {
    id: 'object',
    name: 'Object Options',
    description: 'Data object configuration',
    parameters: ['dataSize', 'dataOffset', 'randomData', 'dataSizeRange', 'dataSizeList', 'dataSizePattern', 'expiryRange']
  },
  {
    id: 'key',
    name: 'Key Options',
    description: 'Key generation and distribution',
    parameters: ['keyPrefix', 'keyMinimum', 'keyMaximum', 'keyPattern', 'keyStddev', 'keyMedian', 'keyZipfExp']
  },
  {
    id: 'import',
    name: 'Data Import',
    description: 'Import and verify external data',
    parameters: ['dataImport', 'dataVerify', 'verifyOnly', 'generateKeys', 'noExpiry']
  },
  {
    id: 'wait',
    name: 'WAIT Commands',
    description: 'Redis WAIT command configuration',
    parameters: ['waitRatio', 'numSlaves', 'waitTimeout']
  },
  {
    id: 'command',
    name: 'Arbitrary Commands',
    description: 'Custom command execution',
    parameters: ['commands']
  },
  {
    id: 'output',
    name: 'Output & Debug',
    description: 'Output formatting and debugging',
    parameters: ['debug', 'showConfig', 'hideHistogram', 'printPercentiles', 'printAllRuns', 'outFile', 'jsonOutFile']
  }
];
