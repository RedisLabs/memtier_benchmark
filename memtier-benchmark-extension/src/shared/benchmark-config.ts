import { MemtierConfig, ArbitraryCommand } from './types';

// Default configuration based on memtier benchmark defaults
export const DEFAULT_CONFIG: MemtierConfig = {
  // Connection and General Options
  server: 'localhost',
  port: 6379,
  ipv4: false,
  ipv6: false,
  protocol: 'redis',
  
  // TLS Options
  tls: false,
  tlsSkipVerify: false,
  
  // Test Options
  runCount: 1,
  requests: 10000,
  clients: 50,
  threads: 4,
  ratio: '1:10',
  pipeline: 1,
  multiKeyGet: 0,
  distinctClientSeed: false,
  randomize: false,
  
  // Object Options
  dataSize: 32,
  dataOffset: 0,
  randomData: false,
  dataSizePattern: 'R',
  
  // Imported Data Options
  dataVerify: false,
  verifyOnly: false,
  generateKeys: false,
  noExpiry: false,
  
  // Key Options
  keyPrefix: 'memtier-',
  keyMinimum: 0,
  keyMaximum: 10000000,
  keyPattern: 'R:R',
  
  // WAIT Options
  waitRatio: '1:0',
  
  // Arbitrary Command Options
  commands: [],
  
  // Output and Debug Options
  debug: false,
  showConfig: false,
  hideHistogram: false,
  printAllRuns: false,
  clusterMode: false
};

export class BenchmarkConfigManager {
  /**
   * Converts MemtierConfig to command line arguments
   */
  static configToArgs(config: MemtierConfig): string[] {
    const args: string[] = [];
    
    // Connection and General Options
    if (config.server !== 'localhost') {
      args.push('--server', config.server);
    }
    if (config.port !== 6379) {
      args.push('--port', config.port.toString());
    }
    if (config.unixSocket) {
      args.push('--unix-socket', config.unixSocket);
    }
    if (config.ipv4) {
      args.push('--ipv4');
    }
    if (config.ipv6) {
      args.push('--ipv6');
    }
    if (config.protocol !== 'redis') {
      args.push('--protocol', config.protocol);
    }
    if (config.authenticate) {
      args.push('--authenticate', config.authenticate);
    }
    
    // TLS Options
    if (config.tls) {
      args.push('--tls');
    }
    if (config.tlsCert) {
      args.push('--cert', config.tlsCert);
    }
    if (config.tlsKey) {
      args.push('--key', config.tlsKey);
    }
    if (config.tlsCacert) {
      args.push('--cacert', config.tlsCacert);
    }
    if (config.tlsSkipVerify) {
      args.push('--tls-skip-verify');
    }
    if (config.tlsProtocols) {
      args.push('--tls-protocols', config.tlsProtocols);
    }
    if (config.sni) {
      args.push('--sni', config.sni);
    }
    
    // Test Options
    if (config.runCount !== 1) {
      args.push('--run-count', config.runCount.toString());
    }
    if (config.requests !== 10000) {
      args.push('--requests', config.requests.toString());
    }
    if (config.rateLimiting) {
      args.push('--rate-limiting', config.rateLimiting.toString());
    }
    if (config.clients !== 50) {
      args.push('--clients', config.clients.toString());
    }
    if (config.threads !== 4) {
      args.push('--threads', config.threads.toString());
    }
    if (config.testTime) {
      args.push('--test-time', config.testTime.toString());
    }
    if (config.ratio !== '1:10') {
      args.push('--ratio', config.ratio);
    }
    if (config.pipeline !== 1) {
      args.push('--pipeline', config.pipeline.toString());
    }
    if (config.reconnectInterval) {
      args.push('--reconnect-interval', config.reconnectInterval.toString());
    }
    if (config.multiKeyGet > 0) {
      args.push('--multi-key-get', config.multiKeyGet.toString());
    }
    if (config.selectDb !== undefined) {
      args.push('--select-db', config.selectDb.toString());
    }
    if (config.distinctClientSeed) {
      args.push('--distinct-client-seed');
    }
    if (config.randomize) {
      args.push('--randomize');
    }
    
    // Object Options
    if (config.dataSize && config.dataSize !== 32) {
      args.push('--data-size', config.dataSize.toString());
    }
    if (config.dataOffset > 0) {
      args.push('--data-offset', config.dataOffset.toString());
    }
    if (config.randomData) {
      args.push('--random-data');
    }
    if (config.dataSizeRange) {
      args.push('--data-size-range', config.dataSizeRange);
    }
    if (config.dataSizeList) {
      args.push('--data-size-list', config.dataSizeList);
    }
    if (config.dataSizePattern !== 'R') {
      args.push('--data-size-pattern', config.dataSizePattern);
    }
    if (config.expiryRange) {
      args.push('--expiry-range', config.expiryRange);
    }
    
    // Imported Data Options
    if (config.dataImport) {
      args.push('--data-import', config.dataImport);
    }
    if (config.dataVerify) {
      args.push('--data-verify');
    }
    if (config.verifyOnly) {
      args.push('--verify-only');
    }
    if (config.generateKeys) {
      args.push('--generate-keys');
    }
    if (config.noExpiry) {
      args.push('--no-expiry');
    }
    
    // Key Options
    if (config.keyPrefix !== 'memtier-') {
      args.push('--key-prefix', config.keyPrefix);
    }
    if (config.keyMinimum !== 0) {
      args.push('--key-minimum', config.keyMinimum.toString());
    }
    if (config.keyMaximum !== 10000000) {
      args.push('--key-maximum', config.keyMaximum.toString());
    }
    if (config.keyPattern !== 'R:R') {
      args.push('--key-pattern', config.keyPattern);
    }
    if (config.keyStddev) {
      args.push('--key-stddev', config.keyStddev.toString());
    }
    if (config.keyMedian) {
      args.push('--key-median', config.keyMedian.toString());
    }
    if (config.keyZipfExp) {
      args.push('--key-zipf-exp', config.keyZipfExp.toString());
    }
    
    // WAIT Options
    if (config.waitRatio !== '1:0') {
      args.push('--wait-ratio', config.waitRatio);
    }
    if (config.numSlaves) {
      args.push('--num-slaves', config.numSlaves);
    }
    if (config.waitTimeout) {
      args.push('--wait-timeout', config.waitTimeout);
    }
    
    // Arbitrary Commands
    config.commands.forEach(cmd => {
      args.push('--command', cmd.command);
      if (cmd.ratio !== 1) {
        args.push('--command-ratio', cmd.ratio.toString());
      }
      if (cmd.keyPattern !== 'R') {
        args.push('--command-key-pattern', cmd.keyPattern);
      }
    });
    
    // Output and Debug Options
    if (config.debug) {
      args.push('--debug');
    }
    if (config.showConfig) {
      args.push('--show-config');
    }
    if (config.hideHistogram) {
      args.push('--hide-histogram');
    }
    if (config.printPercentiles) {
      args.push('--print-percentiles', config.printPercentiles);
    }
    if (config.printAllRuns) {
      args.push('--print-all-runs');
    }
    if (config.clientStats) {
      args.push('--client-stats', config.clientStats);
    }
    if (config.outFile) {
      args.push('--out-file', config.outFile);
    }
    if (config.jsonOutFile) {
      args.push('--json-out-file', config.jsonOutFile);
    }
    if (config.hdrFilePrefix) {
      args.push('--hdr-file-prefix', config.hdrFilePrefix);
    }
    if (config.clusterMode) {
      args.push('--cluster-mode');
    }
    
    return args;
  }
  
  /**
   * Validates configuration and returns validation errors
   */
  static validateConfig(config: MemtierConfig): string[] {
    const errors: string[] = [];
    
    // Basic validation
    if (!config.server) {
      errors.push('Server address is required');
    }
    
    if (config.port < 1 || config.port > 65535) {
      errors.push('Port must be between 1 and 65535');
    }
    
    if (config.clients < 1) {
      errors.push('Number of clients must be at least 1');
    }
    
    if (config.threads < 1) {
      errors.push('Number of threads must be at least 1');
    }
    
    if (config.pipeline < 1) {
      errors.push('Pipeline must be at least 1');
    }
    
    if (typeof config.requests === 'number' && config.requests < 1) {
      errors.push('Number of requests must be at least 1');
    }
    
    // Ratio validation
    if (!/^\d+:\d+$/.test(config.ratio)) {
      errors.push('Ratio must be in format "number:number" (e.g., "1:10")');
    }
    
    // Key range validation
    if (config.keyMinimum >= config.keyMaximum) {
      errors.push('Key minimum must be less than key maximum');
    }
    
    // Data size validation
    if (config.dataSize && config.dataSize < 1) {
      errors.push('Data size must be at least 1 byte');
    }
    
    return errors;
  }
  
  /**
   * Merges partial config with defaults
   */
  static mergeWithDefaults(partialConfig: Partial<MemtierConfig>): MemtierConfig {
    return { ...DEFAULT_CONFIG, ...partialConfig };
  }
}
