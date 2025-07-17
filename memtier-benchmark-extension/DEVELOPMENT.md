# Development Guide

## Quick Start

1. **Install Dependencies**
   ```bash
   cd memtier-benchmark-extension
   npm install
   ```

2. **Development Mode**
   ```bash
   npm run dev
   ```

3. **Build Extension**
   ```bash
   npm run build:extension
   ```

4. **Load in Chrome**
   - Open `chrome://extensions/`
   - Enable "Developer mode"
   - Click "Load unpacked"
   - Select the `dist` folder

## Development Workflow

### Hot Reload Development
```bash
npm run dev
```
This starts Vite in development mode with hot module replacement. Changes to source files will automatically rebuild the extension.

### Building for Production
```bash
npm run build:extension
```
This runs the complete build process including TypeScript checking and creates the final extension in the `dist` folder.

### Code Quality
```bash
npm run type-check    # TypeScript checking
npm run lint          # ESLint checking
```

## Project Structure

```
src/
├── background/           # Service worker (background script)
│   └── service-worker.ts
├── content/             # Content scripts (injected into pages)
│   └── content-script.ts
├── popup/               # Extension popup UI
│   ├── popup.html
│   ├── popup.tsx
│   └── popup.css
├── sidebar/             # Sidebar UI (injected into pages)
│   ├── sidebar.html
│   ├── sidebar.tsx
│   └── sidebar.css
├── options/             # Options page UI
│   ├── options.html
│   ├── options.tsx
│   └── options.css
├── components/          # Shared React components
│   ├── ConfigForm.tsx
│   ├── ResultsDisplay.tsx
│   └── ParameterGroup.tsx
└── shared/              # Shared utilities and types
    ├── types.ts
    ├── storage.ts
    ├── benchmark-config.ts
    └── utils.ts
```

## Key Components

### Background Service Worker
- Handles benchmark execution
- Manages Chrome storage
- Coordinates between UI components
- Located in `src/background/service-worker.ts`

### Content Script
- Injects sidebar into web pages
- Handles keyboard shortcuts
- Manages page interaction
- Located in `src/content/content-script.ts`

### UI Components
- **Popup**: Quick access interface (400px width)
- **Sidebar**: Extended interface (resizable, overlays pages)
- **Options**: Full settings page

### Shared Utilities
- **Types**: TypeScript interfaces and types
- **Storage**: Chrome storage API wrapper
- **Config**: Configuration management and validation
- **Utils**: Helper functions and utilities

## Development Tips

### Chrome Extension APIs
```typescript
// Storage
chrome.storage.sync.set({ key: value });
chrome.storage.local.get(['key']);

// Messaging
chrome.runtime.sendMessage({ type: 'ACTION', payload: data });
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {});

// Tabs
chrome.tabs.query({ active: true, currentWindow: true });
chrome.scripting.executeScript({ target: { tabId }, func: () => {} });
```

### React in Extension Context
- Use `createRoot` from React 18
- Handle Chrome extension lifecycle
- Manage state across different UI contexts
- Use proper TypeScript types for Chrome APIs

### Storage Management
```typescript
// Sync storage (settings, presets) - 100KB limit
await StorageManager.saveSettings(settings);

// Local storage (results, cache) - 5MB limit
await StorageManager.saveBenchmarkResult(result);
```

### Message Passing
```typescript
// Background ↔ Content Script
chrome.runtime.sendMessage({ type: 'RUN_BENCHMARK', payload: config });

// Content Script ↔ Sidebar (iframe)
window.postMessage({ source: 'memtier-sidebar', type: 'CLOSE' }, '*');
```

## Testing

### Manual Testing Checklist
- [ ] Extension loads without errors
- [ ] Popup opens and displays correctly
- [ ] Sidebar toggles with keyboard shortcut
- [ ] Configuration parameters save/load
- [ ] Benchmark execution works
- [ ] Results display properly
- [ ] Export/import functionality
- [ ] Options page settings persist
- [ ] Cross-tab communication works

### Browser Testing
- Test in Chrome (primary)
- Test in different screen sizes
- Test with different Chrome versions
- Verify permissions work correctly

### Development Tools
- Chrome DevTools for debugging
- Extension DevTools for background scripts
- React DevTools for component debugging
- Network tab for API calls

## Common Issues

### Build Issues
```bash
# Clear node_modules and reinstall
rm -rf node_modules package-lock.json
npm install

# Clear build cache
rm -rf dist .vite
npm run build:extension
```

### Extension Loading Issues
- Check manifest.json syntax
- Verify all files are in dist/
- Check Chrome extension permissions
- Look for console errors in background script

### Hot Reload Not Working
- Restart development server
- Reload extension in Chrome
- Check file watchers and permissions

## Debugging

### Background Script
- Open `chrome://extensions/`
- Click "Inspect views: background page"
- Use Chrome DevTools

### Content Script
- Open page where content script runs
- Open DevTools
- Check Console and Sources tabs

### Popup/Options
- Right-click extension icon → "Inspect popup"
- Or open options page and use F12

## Performance Considerations

### Bundle Size
- Use code splitting for large components
- Import only needed parts of libraries
- Optimize images and assets

### Memory Usage
- Clean up event listeners
- Avoid memory leaks in React components
- Monitor storage usage

### Permissions
- Request minimal permissions
- Use optional permissions when possible
- Document why permissions are needed

## Deployment

### Chrome Web Store
1. Build production version
2. Create store listing
3. Upload extension package
4. Submit for review

### Development Distribution
1. Build extension: `npm run build:extension`
2. Create ZIP of dist/ folder
3. Share with testers
4. Provide installation instructions

## Contributing

1. Fork repository
2. Create feature branch
3. Make changes with tests
4. Update documentation
5. Submit pull request

### Code Style
- Use TypeScript for all new code
- Follow existing naming conventions
- Add JSDoc comments for public APIs
- Use Prettier for formatting
- Follow React best practices
