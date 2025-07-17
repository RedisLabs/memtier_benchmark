# Extension Icons

This directory should contain the following icon files:

- `icon-16.png` - 16x16 pixels (toolbar icon)
- `icon-32.png` - 32x32 pixels (Windows)
- `icon-48.png` - 48x48 pixels (extension management page)
- `icon-128.png` - 128x128 pixels (Chrome Web Store)

## Icon Design Guidelines

### Theme
- Use a database/benchmark related icon
- Colors: Blue (#3b82f6) primary, with accent colors
- Style: Modern, clean, recognizable at small sizes

### Suggested Design Elements
- Database cylinder icon
- Performance graph/chart
- Speed/benchmark indicator
- Redis logo elements (if appropriate)

### Tools for Icon Creation
- Figma (recommended)
- Adobe Illustrator
- Canva
- Online icon generators

### Example SVG Base
```svg
<svg width="128" height="128" viewBox="0 0 128 128" xmlns="http://www.w3.org/2000/svg">
  <!-- Database cylinder -->
  <ellipse cx="64" cy="32" rx="40" ry="12" fill="#3b82f6"/>
  <rect x="24" y="32" width="80" height="64" fill="#3b82f6"/>
  <ellipse cx="64" cy="96" rx="40" ry="12" fill="#2563eb"/>
  
  <!-- Performance indicator -->
  <path d="M45 50 L55 45 L65 40 L75 35" stroke="#10b981" stroke-width="3" fill="none"/>
  <circle cx="75" cy="35" r="3" fill="#10b981"/>
</svg>
```

## Temporary Placeholder
Until proper icons are created, you can use simple colored squares or generate icons using online tools.

For development, create simple PNG files with the required dimensions and a solid color background with text "MB" (Memtier Benchmark).
