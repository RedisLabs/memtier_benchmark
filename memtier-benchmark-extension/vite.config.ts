import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';
import { webExtension } from 'vite-plugin-web-extension';

export default defineConfig({
  plugins: [
    react(),
    webExtension({
      manifest: './public/manifest.json',
      watchFilePaths: ['src/**/*'],
      additionalInputs: [
        'src/popup/popup.html',
        'src/sidebar/sidebar.html',
        'src/options/options.html'
      ]
    })
  ],
  build: {
    outDir: 'dist',
    rollupOptions: {
      input: {
        popup: resolve(__dirname, 'src/popup/popup.html'),
        sidebar: resolve(__dirname, 'src/sidebar/sidebar.html'),
        options: resolve(__dirname, 'src/options/options.html'),
        'background/service-worker': resolve(__dirname, 'src/background/service-worker.ts'),
        'content/content-script': resolve(__dirname, 'src/content/content-script.ts')
      },
      output: {
        entryFileNames: (chunkInfo) => {
          const name = chunkInfo.name;
          if (name.includes('/')) {
            return `${name}.js`;
          }
          return `src/${name}/[name].js`;
        },
        chunkFileNames: 'chunks/[name]-[hash].js',
        assetFileNames: (assetInfo) => {
          const name = assetInfo.name || '';
          if (name.endsWith('.html')) {
            return `src/[name]/[name].[ext]`;
          }
          return 'assets/[name]-[hash].[ext]';
        }
      }
    },
    target: 'es2020',
    minify: false, // Disable minification for easier debugging
    sourcemap: true
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  define: {
    'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'development')
  }
});
