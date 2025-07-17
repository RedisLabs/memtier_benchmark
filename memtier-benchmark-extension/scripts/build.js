#!/usr/bin/env node

import { execSync } from 'child_process';
import { existsSync, mkdirSync, copyFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, '..');

console.log('🚀 Building Memtier Benchmark Extension...\n');

// Ensure directories exist
const distDir = join(rootDir, 'dist');
const iconsDir = join(rootDir, 'public', 'icons');

if (!existsSync(iconsDir)) {
  mkdirSync(iconsDir, { recursive: true });
}

// Create placeholder icons if they don't exist
const iconSizes = [16, 32, 48, 128];
iconSizes.forEach(size => {
  const iconPath = join(iconsDir, `icon-${size}.png`);
  if (!existsSync(iconPath)) {
    console.log(`📦 Creating placeholder icon: icon-${size}.png`);
    
    // Create a simple SVG and convert to PNG (placeholder)
    const svg = `
      <svg width="${size}" height="${size}" xmlns="http://www.w3.org/2000/svg">
        <rect width="${size}" height="${size}" fill="#3b82f6"/>
        <text x="50%" y="50%" text-anchor="middle" dy="0.3em" 
              fill="white" font-family="Arial" font-size="${Math.floor(size/4)}" font-weight="bold">
          MB
        </text>
      </svg>
    `;
    
    // For now, just create a note about the missing icon
    writeFileSync(iconPath.replace('.png', '.svg'), svg.trim());
    console.log(`   ⚠️  Created SVG placeholder. Convert to PNG manually or use an icon generator.`);
  }
});

try {
  // Clean dist directory
  console.log('🧹 Cleaning dist directory...');
  if (existsSync(distDir)) {
    execSync('rm -rf dist/*', { cwd: rootDir });
  }

  // Run TypeScript check
  console.log('🔍 Running TypeScript check...');
  execSync('npm run type-check', { cwd: rootDir, stdio: 'inherit' });

  // Run build
  console.log('🔨 Building extension...');
  execSync('npm run build', { cwd: rootDir, stdio: 'inherit' });

  // Copy additional files if needed
  console.log('📋 Copying additional files...');
  
  // Verify build output
  const manifestPath = join(distDir, 'manifest.json');
  if (existsSync(manifestPath)) {
    console.log('✅ Build completed successfully!');
    console.log('\n📁 Extension built in: dist/');
    console.log('\n🔧 To load in Chrome:');
    console.log('   1. Open chrome://extensions/');
    console.log('   2. Enable "Developer mode"');
    console.log('   3. Click "Load unpacked"');
    console.log('   4. Select the "dist" folder');
    console.log('\n🎯 For development:');
    console.log('   npm run dev');
  } else {
    throw new Error('Build failed: manifest.json not found in dist/');
  }

} catch (error) {
  console.error('❌ Build failed:', error.message);
  process.exit(1);
}
