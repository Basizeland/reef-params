# Performance Optimization Notes

## Completed Optimizations

### 1. Deferred Non-Critical CSS Loading
- **Change**: Bootstrap CSS and Bootstrap Icons CSS now load asynchronously using preload
- **Impact**: Removes ~1,330ms of render-blocking time
- **Files Modified**: `templates/base.html`
- **Technique**: Using `<link rel="preload" as="style" onload="...">`with fallback for no-JS

### 2. CDN Preconnect Hints
- **Change**: Added preconnect and dns-prefetch for cdn.jsdelivr.net
- **Impact**: Faster connection establishment for CDN resources
- **Files Modified**: `templates/base.html`
- **Benefit**: Reduces latency for Bootstrap, Chart.js, and other CDN assets

### 3. Font Display Optimization
- **Change**: Added `font-display: swap` for Bootstrap Icons font
- **Impact**: Prevents invisible text during font loading (~20ms savings)
- **Files Modified**: `templates/base.html`
- **Technique**: CSS @font-face override

### 4. Conditional Chart.js Loading
- **Change**: Chart.js only loads on pages that actually use it
- **Impact**: Saves ~62 KiB of unused JavaScript on most pages
- **Files Modified**:
  - `templates/base.html` - Created `chart_scripts` block
  - `templates/tank_detail.html` - Overrides block to load Chart.js
- **Benefit**: Faster page load on dashboard, insights, and other pages

### 5. Critical CSS Path
- **Change**: Ensured `/static/style.css` loads synchronously as critical CSS
- **Impact**: Prevents flash of unstyled content while still deferring non-critical styles
- **Files Modified**: `templates/base.html`

## Manual Optimizations Needed

### Logo Image Optimization
- **Current State**: `static/logo-light.png` is 1801x592 pixels (53 KB)
- **Displayed Size**: 150x36 pixels
- **Required Action**: Resize and optimize the logo image
- **Recommended Steps**:
  1. Resize to appropriate dimensions (e.g., 300x72 for 2x retina display)
  2. Optimize with pngquant or similar tool
  3. Consider converting to WebP format with PNG fallback
  4. Expected savings: ~50 KB
- **Tools**: ImageMagick, pngquant, or online tools like TinyPNG

**Example command** (requires ImageMagick):
```bash
convert static/logo-light.png -resize 300x72 -quality 85 static/logo-light-optimized.png
pngquant --quality=65-80 static/logo-light-optimized.png
```

### Additional Optimizations to Consider

1. **Service Worker Caching Strategy**
   - Implement stale-while-revalidate for static assets
   - Cache Bootstrap and icon fonts in service worker

2. **Image Format Modernization**
   - Convert PNG logos to WebP with PNG fallback
   - Use `<picture>` element for responsive images

3. **Inline Critical CSS**
   - Extract and inline above-the-fold CSS (first ~14kb)
   - Defer all other CSS loading

4. **JavaScript Minification**
   - Minify custom JavaScript in templates
   - Consider bundling common scripts

5. **Resource Hints**
   - Add more specific preload hints for critical resources
   - Consider prefetch for likely navigation targets

## Performance Metrics Impact

### Expected Improvements
- **First Contentful Paint (FCP)**: -1.3s (from CSS deferral)
- **Largest Contentful Paint (LCP)**: -1.5s (from render blocking removal + logo optimization)
- **Total Blocking Time (TBT)**: Already 0ms, remains optimal
- **Speed Index**: -2s+ (from combined optimizations)

### Before vs After
| Metric | Before | After (Estimated) |
|--------|--------|-------------------|
| FCP | 2.7s | ~1.4s |
| LCP | 3.3s | ~1.8s |
| Speed Index | 4.6s | ~2.6s |
| TBT | 0ms | 0ms |
| CLS | 0 | 0 |

## Testing

To verify improvements:
1. Run Lighthouse audit on login page
2. Check Network panel for deferred CSS loading
3. Verify Chart.js only loads on tank detail pages
4. Confirm preconnect reduces CDN connection time

## References

- [Web.dev - Defer non-critical CSS](https://web.dev/defer-non-critical-css/)
- [Web.dev - Preconnect to required origins](https://web.dev/uses-rel-preconnect/)
- [Web.dev - Font Display](https://web.dev/font-display/)
- [Web.dev - Remove unused code](https://web.dev/remove-unused-code/)
