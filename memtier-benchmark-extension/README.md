# Memtier Benchmark Chrome Extension

A comprehensive Chrome extension for running Redis/Memcache benchmarks with memtier_benchmark. This productivity tool provides a user-friendly interface for configuring and executing performance tests with real-time monitoring and results visualization.

## Features

### 🚀 Core Functionality
- **Complete Parameter Configuration**: All 80+ memtier_benchmark parameters organized in logical groups
- **Real-time Benchmark Execution**: Execute benchmarks with live output streaming
- **Results Visualization**: Display benchmark results with charts and statistics
- **Configuration Presets**: Save and load common benchmark configurations
- **Cross-device Sync**: Sync configurations using Chrome storage API

### 🎨 User Interface
- **Popup Interface**: Quick access via extension icon
- **Sidebar Mode**: Extended interface that overlays on web pages
- **Options Page**: Comprehensive settings and data management
- **Hot Reload Development**: Vite-based development with HMR
- **Responsive Design**: Works on different screen sizes

### 📊 Advanced Features
- **Output Parsing**: Extract statistics from benchmark output
- **Export/Import**: Backup and restore all extension data
- **Storage Management**: Monitor and manage extension storage usage
- **Keyboard Shortcuts**: Quick access with customizable shortcuts
- **Theme Support**: Light, dark, and auto themes

## Installation

### Development Setup

1. **Clone and Install Dependencies**
   ```bash
   cd memtier-benchmark-extension
   npm install
   ```

2. **Build the Extension**
   ```bash
   npm run build
   ```

3. **Load in Chrome**
   - Open Chrome and go to `chrome://extensions/`
   - Enable "Developer mode"
   - Click "Load unpacked" and select the `dist` folder

### Development Mode

For development with hot reload:
```bash
npm run dev
```

## Usage

### Basic Workflow

1. **Configure Parameters**: Use the popup or sidebar to set benchmark parameters
2. **Run Benchmark**: Click "Run Benchmark" to start execution
3. **Monitor Progress**: Watch real-time output in the console tab
4. **View Results**: Analyze results with built-in statistics and charts
5. **Save Presets**: Save frequently used configurations for quick access

### Interface Options

#### Popup Interface
- Click the extension icon in the toolbar
- Compact interface for quick benchmarks
- Tabs for Config, Results, Presets, and Settings

#### Sidebar Interface
- Press `Ctrl+Shift+M` to toggle sidebar
- Extended interface with more space
- Resizable and collapsible
- Real-time console output

#### Options Page
- Right-click extension icon → Options
- Comprehensive settings management
- Data export/import functionality
- Storage usage monitoring

### Parameter Groups

The extension organizes memtier parameters into logical groups:

1. **Connection & General** - Server connection and basic configuration
2. **TLS/SSL Options** - Transport Layer Security configuration
3. **Test Configuration** - Test execution parameters
4. **Object Options** - Data object configuration
5. **Key Options** - Key generation and distribution
6. **Data Import** - Import and verify external data
7. **WAIT Commands** - Redis WAIT command configuration
8. **Arbitrary Commands** - Custom command execution
9. **Output & Debug** - Output formatting and debugging

### Keyboard Shortcuts

- `Ctrl+Shift+M` - Toggle sidebar
- `Ctrl+Shift+B` - Focus on extension icon
- `Alt+Shift+M` - Alternative shortcut

## Configuration Examples

### Basic Redis Benchmark
```json
{
  "server": "localhost",
  "port": 6379,
  "protocol": "redis",
  "clients": 50,
  "threads": 4,
  "requests": 10000,
  "ratio": "1:10",
  "dataSize": 32
}
```

### High-Performance Test
```json
{
  "server": "redis-cluster.example.com",
  "port": 6379,
  "protocol": "redis",
  "clients": 200,
  "threads": 16,
  "requests": 1000000,
  "ratio": "1:3",
  "pipeline": 10,
  "dataSize": 1024,
  "clusterMode": true
}
```

### Memcache Binary Protocol
```json
{
  "server": "memcache.example.com",
  "port": 11211,
  "protocol": "memcache_binary",
  "authenticate": "user:password",
  "clients": 100,
  "threads": 8,
  "requests": 50000,
  "dataSize": 512
}
```

## Architecture

### Project Structure
```
memtier-benchmark-extension/
├── src/
│   ├── background/          # Service worker
│   ├── content/            # Content scripts
│   ├── popup/              # Popup UI
│   ├── sidebar/            # Sidebar UI
│   ├── options/            # Options page
│   ├── shared/             # Shared utilities
│   └── components/         # React components
├── public/                 # Static assets
├── dist/                   # Built extension
└── config files
```

### Technology Stack
- **TypeScript** - Type-safe development
- **React** - UI components and state management
- **Vite** - Build system with hot reload
- **Tailwind CSS** - Utility-first styling
- **Chrome Extension APIs** - Storage, messaging, scripting

### Key Components

#### Background Service Worker
- Handles benchmark execution
- Manages Chrome storage
- Coordinates between UI components
- Simulates memtier_benchmark execution

#### Content Script
- Injects sidebar into web pages
- Handles keyboard shortcuts
- Manages page interaction
- Forwards messages between components

#### Storage Manager
- Chrome storage API wrapper
- Sync and local storage management
- Data export/import functionality
- Storage usage monitoring

#### Configuration Manager
- Parameter validation
- Command-line argument generation
- Default configuration management
- Configuration merging utilities

## Development

### Building
```bash
npm run build          # Production build
npm run dev           # Development with hot reload
npm run type-check    # TypeScript checking
npm run lint          # ESLint checking
```

### Testing
The extension includes comprehensive TypeScript types and validation. For testing:

1. Load the extension in Chrome
2. Test all parameter combinations
3. Verify storage functionality
4. Check cross-tab communication
5. Test export/import features

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Limitations

### Current Implementation
- **Simulated Execution**: The current version simulates memtier_benchmark execution
- **No Native Binary**: Doesn't execute actual memtier_benchmark binary
- **Chrome Only**: Currently supports Chrome/Chromium browsers only

### Future Enhancements
- **Native Messaging**: Integration with local memtier_benchmark installation
- **Cross-browser Support**: Firefox, Safari, Edge compatibility
- **Advanced Visualization**: Charts and graphs for results
- **Benchmark Comparison**: Compare multiple benchmark runs
- **Automated Testing**: Scheduled benchmark execution

## Security Considerations

- All data is stored locally in Chrome storage
- No external network requests (except to configured Redis/Memcache servers)
- Configuration validation prevents command injection
- Secure handling of authentication credentials

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, feature requests, or contributions:
1. Check existing issues
2. Create detailed bug reports
3. Include configuration and error details
4. Test with minimal reproduction cases

## Changelog

### Version 1.0.0
- Initial release
- Complete parameter configuration
- Popup and sidebar interfaces
- Configuration presets
- Results visualization
- Export/import functionality
- Cross-device sync
- Hot reload development setup
