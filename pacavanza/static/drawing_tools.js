/**
 * drawing_tools.js — TradingView-style drawing toolbar
 * Integrates lightweight-charts-drawing with PACavanza charts.
 *
 * Usage:
 *   const dt = new DrawingToolbar(chartManagerState);
 *   dt.mount(document.getElementById('chart-future'));
 */

class DrawingToolbar {
  /** @param {object} cm  – chartManager state from PACChartApp.createChartManager */
  constructor(cm) {
    this._cm = cm;
    this._chart = cm.chart;
    this._series = cm.series.candle;
    this._container = null;         // mounted later
    this._toolbarEl = null;
    this._manager = null;           // DrawingManager instance
    this._registry = null;          // ToolRegistry

    this._currentTool = null;
    this._pendingAnchors = [];
    this._drawingIdCounter = 0;

    // preview (rubber-band) state
    this._previewDrawing = null;
    this._previewId = '__preview__';

    // SVG icon cache (tool-type → svg string)
    this._icons = DrawingToolbar._buildIcons();
  }

  // ── Public API ──────────────────────────────────────────────────
  mount(containerEl) {
    if (!window.LightweightChartsDrawing) {
      console.warn('[drawing-tools] LightweightChartsDrawing global not loaded');
      return;
    }

    this._container = containerEl;
    const LCD = window.LightweightChartsDrawing;

    if (this._cm.drawingManager && this._cm.toolRegistry) {
      this._manager = this._cm.drawingManager;
      this._registry = this._cm.toolRegistry;
    } else {
      this._manager = new LCD.DrawingManager();
      this._manager.attach(this._chart, this._series, containerEl);
      this._registry = LCD.getToolRegistry();
    }

    this._buildToolbar();
    this._wireChartInteraction();
    this._wireKeyboard();
  }

  static _formatKey(keyStr) {
    if (!keyStr) return '';
    const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0 || navigator.userAgent.toUpperCase().indexOf('MAC') >= 0;
    const altLabel = isMac ? '⌥' : 'Alt+';
    const shiftLabel = isMac ? '⇧' : 'Shift+';

    return keyStr
      .split('+')
      .map(part => {
        if (part === 'alt') return altLabel;
        if (part === 'shift') return shiftLabel;
        return part.toUpperCase();
      })
      .join('');
  }

  // ── Tool Definitions (ordered, grouped) with keyboard shortcuts ──
  static _toolDefs() {
    return [
      {
        group: 'Lines', tools: [
          { id: 'trend-line', label: 'Trend Line', key: 'alt+t' },
          { id: 'ray', label: 'Ray', key: 'alt+y' },
          { id: 'horizontal-line', label: 'Horizontal Line', key: 'alt+h' },
          { id: 'horizontal-ray', label: 'Horizontal Ray', key: 'alt+j' },
        ]
      },
      {
        group: 'Channels', tools: [
          { id: 'parallel-channel', label: 'Parallel Channel', key: 'alt+p' },
        ]
      },
      {
        group: 'Fibonacci', tools: [
          { id: 'fib-retracement', label: 'Fib Retracement', key: 'alt+f' },
        ]
      },
      {
        group: 'Positions', tools: [
          { id: 'long-position', label: 'Long Position', key: 'alt+l' },
          { id: 'short-position', label: 'Short Position', key: 'alt+s' },
          { id: 'date-price-range', label: 'Date & Price Range', key: 'alt+d' },
        ]
      },
      {
        group: 'Arrows', tools: [
          { id: 'arrow', label: 'Arrow', key: 'alt+a' },
          { id: 'arrow-mark-up', label: 'Arrow Up', key: 'alt+u' },
          { id: 'arrow-mark-down', label: 'Arrow Down', key: 'alt+v' },
        ]
      },
      {
        group: 'Shapes', tools: [
          { id: 'rectangle', label: 'Rectangle', key: 'alt+shift+r' },
          { id: 'rotated-rectangle', label: 'Rotated Rectangle', key: 'alt+shift+o' },
          { id: 'ellipse', label: 'Ellipse', key: 'alt+e' },
        ]
      },
      {
        group: 'Text', tools: [
          { id: 'text-annotation', label: 'Text', key: 'alt+x' },
          { id: 'anchored-text', label: 'Anchored Text', key: 'alt+shift+x' },
          { id: 'price-label', label: 'Price Label', key: 'alt+c' },
        ]
      },
    ];
  }

  // ── Build shortcut key → tool-id map ────────────────────────────
  static _shortcutMap() {
    const map = {};
    for (const g of DrawingToolbar._toolDefs())
      for (const t of g.tools) if (t.key) map[t.key] = t.id;
    return map;
  }

  // ── Tool Colors (TradingView palette) ───────────────────────────
  static _toolColor(type) {
    const map = {
      'trend-line': '#2962FF', 'ray': '#2962FF', 'horizontal-line': '#FF6D00',
      'horizontal-ray': '#FF6D00', 'parallel-channel': '#00BCD4',
      'fib-retracement': '#FFD700',
      'long-position': '#26A69A', 'short-position': '#EF5350',
      'date-price-range': '#9C27B0', 'arrow': '#26A69A',
      'arrow-mark-up': '#26A69A', 'arrow-mark-down': '#EF5350',
      'rectangle': '#2962FF', 'rotated-rectangle': '#2962FF',
      'ellipse': '#E91E63',
      'text-annotation': '#FFFFFF', 'anchored-text': '#FFFFFF',
      'price-label': '#2962FF',
    };
    return map[type] || '#2962FF';
  }

  // ── Required Anchors ────────────────────────────────────────────
  _requiredAnchors(toolType) {
    const def = this._registry.get(toolType);
    return def?.requiredAnchors ?? 2;
  }

  // ── Fib Options Override ────────────────────────────────────────
  _toolOptionsOverride(toolType) {
    if (toolType === 'fib-retracement') {
      return { levels: [0, 0.5, 1, 1.5, 2] };
    }
    return {};
  }

  // ── Build Toolbar DOM ───────────────────────────────────────────
  _buildToolbar() {
    const bar = document.createElement('div');
    bar.className = 'pac-drawing-toolbar';

    const groups = DrawingToolbar._toolDefs();
    groups.forEach((g, gi) => {
      if (gi > 0) {
        const sep = document.createElement('div');
        sep.className = 'pac-dt-sep';
        bar.appendChild(sep);
      }
      g.tools.forEach(t => {
        const btn = document.createElement('button');
        btn.className = 'pac-dt-btn';
        btn.dataset.tool = t.id;
        btn.title = t.label + (t.key ? ` [${DrawingToolbar._formatKey(t.key)}]` : '');
        btn.innerHTML = this._icons[t.id] || `<span style="font-size:10px">${t.label.substring(0, 3)}</span>`;
        btn.addEventListener('click', () => this._selectTool(t.id));
        bar.appendChild(btn);
      });
    });

    // ── Delete All button
    const sep2 = document.createElement('div');
    sep2.className = 'pac-dt-sep';
    bar.appendChild(sep2);

    const delBtn = document.createElement('button');
    delBtn.className = 'pac-dt-btn pac-dt-btn-danger';
    delBtn.title = 'Clear All Drawings';
    delBtn.innerHTML = DrawingToolbar._trashIcon();
    delBtn.addEventListener('click', () => {
      this._cancelPreview();
      this._manager.clearAll();
    });
    bar.appendChild(delBtn);

    this._container.style.position = 'relative';
    this._container.appendChild(bar);
    this._toolbarEl = bar;
  }

  // ── Select / Deselect Tool ──────────────────────────────────────
  _selectTool(toolType) {
    this._toolbarEl.querySelectorAll('.pac-dt-btn').forEach(b => b.classList.remove('active'));
    this._cancelPreview();

    if (this._currentTool === toolType) {
      this._currentTool = null;
      this._container.style.cursor = '';
      return;
    }

    this._currentTool = toolType;
    this._container.style.cursor = 'crosshair';
    const btn = this._toolbarEl.querySelector(`[data-tool="${toolType}"]`);
    if (btn) btn.classList.add('active');
  }

  // ── Magnet: snap price to nearest OHLC when Shift is held ──────
  _magnetSnap(time, price) {
    const data = this._cm.data;
    if (!data || data.size === 0) return price;

    // Find bar at or nearest to this time
    let bestBar = null, bestDist = Infinity;
    for (const [t, bar] of data) {
      const d = Math.abs(t - (typeof time === 'number' ? time : 0));
      if (d < bestDist) { bestDist = d; bestBar = bar; }
    }
    if (!bestBar) return price;

    // Snap to nearest of O, H, L, C
    let closest = bestBar.open, closestDist = Math.abs(price - bestBar.open);
    for (const v of [bestBar.high, bestBar.low, bestBar.close]) {
      const d = Math.abs(price - v);
      if (d < closestDist) { closestDist = d; closest = v; }
    }
    return closest;
  }

  // ── Chart Interaction ───────────────────────────────────────────
  _wireChartInteraction() {
    this._container.addEventListener('click', (e) => {
      if (!this._currentTool) return;
      if (e.target.closest('.pac-drawing-toolbar')) return;

      const rect = this._container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      const time = this._chart.timeScale().coordinateToTime(x);
      let price = this._series.coordinateToPrice(y);
      if (time === null || price === null) return;

      // Magnet: hold Shift to snap to nearest OHLC
      if (e.shiftKey) price = this._magnetSnap(time, price);

      const anchor = { time, price };
      this._pendingAnchors.push(anchor);

      const required = this._requiredAnchors(this._currentTool);
      if (this._pendingAnchors.length >= required) {
        this._removePreview();
        this._createDrawing(this._currentTool, [...this._pendingAnchors]);
        this._pendingAnchors = [];
        // Auto-deselect tool after drawing is placed
        this._selectTool(null);
        this._container.style.cursor = '';
      } else {
        this._updatePreview(anchor);
      }
    });

    this._container.addEventListener('mousemove', (e) => {
      if (!this._currentTool || this._pendingAnchors.length === 0) return;
      if (e.target.closest('.pac-drawing-toolbar')) return;

      const rect = this._container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      const time = this._chart.timeScale().coordinateToTime(x);
      let price = this._series.coordinateToPrice(y);
      if (time === null || price === null) return;

      if (e.shiftKey) price = this._magnetSnap(time, price);

      this._updatePreviewWithMouse({ time, price });
    });
  }

  // ── Keyboard ────────────────────────────────────────────────────
  _wireKeyboard() {
    const shortcuts = DrawingToolbar._shortcutMap();

    document.addEventListener('keydown', (e) => {
      // Don't trigger shortcuts when typing in inputs
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

      if (e.key === 'Escape') {
        this._cancelPreview();
        this._selectTool(null);
        this._container.style.cursor = '';
        return;
      }
      if (e.key === 'Delete' || e.key === 'Backspace') {
        const sel = this._manager.getSelectedDrawing();
        if (sel) { this._manager.removeDrawing(sel.id); return; }
      }

      if (e.ctrlKey || e.metaKey) return;

      let baseKey = '';
      if (e.code && e.code.startsWith('Key')) {
        baseKey = e.code.slice(3).toLowerCase();
      } else if (e.code && e.code.startsWith('Digit')) {
        baseKey = e.code.slice(5);
      } else {
        baseKey = e.key.toLowerCase();
      }

      if (['alt', 'shift', 'control', 'meta'].includes(baseKey)) return;

      const parts = [];
      if (e.altKey) parts.push('alt');
      if (e.shiftKey) parts.push('shift');
      parts.push(baseKey);
      const combo = parts.join('+');

      const toolId = shortcuts[combo];
      if (toolId) {
        e.preventDefault();
        this._selectTool(toolId);
      }
    });
  }

  // ── Preview ─────────────────────────────────────────────────────
  _updatePreview(newAnchor) {
    if (!this._currentTool) return;
    const required = this._requiredAnchors(this._currentTool);
    const color = DrawingToolbar._toolColor(this._currentTool);

    const previewAnchors = [...this._pendingAnchors];
    while (previewAnchors.length < required) {
      previewAnchors.push({ ...newAnchor });
    }

    this._removePreview();

    const style = { lineColor: color, lineWidth: 2, fillColor: color + '33' };
    const opts = this._toolOptionsOverride(this._currentTool);
    this._previewDrawing = this._registry.createDrawing(
      this._currentTool, this._previewId, previewAnchors, style, opts
    );
    if (this._previewDrawing) this._manager.addDrawing(this._previewDrawing);
  }

  _updatePreviewWithMouse(mouseAnchor) {
    if (!this._previewDrawing || !this._currentTool) return;
    const required = this._requiredAnchors(this._currentTool);
    const idx = this._pendingAnchors.length;
    if (idx < required) this._previewDrawing.updateAnchor(idx, mouseAnchor);
  }

  _removePreview() {
    if (this._previewDrawing) {
      this._manager.removeDrawing(this._previewId);
      this._previewDrawing = null;
    }
  }

  _cancelPreview() {
    this._removePreview();
    this._pendingAnchors = [];
  }

  // ── Create Final Drawing ────────────────────────────────────────
  _createDrawing(toolType, anchors) {
    const id = `d-${++this._drawingIdCounter}`;
    const color = DrawingToolbar._toolColor(toolType);
    const style = { lineColor: color, lineWidth: 2, fillColor: color + '33' };
    const opts = this._toolOptionsOverride(toolType);

    const drawing = this._registry.createDrawing(toolType, id, anchors, style, opts);
    if (drawing) {
      // Patch fib-retracement to remove the dashed projection line
      if (toolType === 'fib-retracement') {
        const origPV = drawing.paneViews.bind(drawing);
        drawing.paneViews = () => {
          const views = origPV();
          return views.map(v => {
            const origR = v.renderer.bind(v);
            return {
              zOrder: v.zOrder.bind(v),
              renderer: () => {
                const r = origR();
                if (!r || !r.drawImpl) return r;
                const origDrawImpl = r.drawImpl.bind(r);
                return {
                  draw: (target) => {
                    target.useBitmapCoordinateSpace((scope) => {
                      const ctx = scope.context;
                      // The projection line is drawn via: setLineDash([5,5]), drawLine, setLineDash([])
                      // Block any drawing while a dash pattern is active
                      const realSetLineDash = ctx.setLineDash.bind(ctx);
                      const realMoveTo = ctx.moveTo.bind(ctx);
                      const realLineTo = ctx.lineTo.bind(ctx);
                      const realStroke = ctx.stroke.bind(ctx);
                      let dashing = false;
                      ctx.setLineDash = (pattern) => {
                        dashing = pattern && pattern.length > 0;
                        realSetLineDash(pattern);
                      };
                      ctx.moveTo = (...a) => { if (!dashing) realMoveTo(...a); };
                      ctx.lineTo = (...a) => { if (!dashing) realLineTo(...a); };
                      ctx.stroke = (...a) => { if (!dashing) realStroke(...a); };
                      origDrawImpl(scope);
                      ctx.setLineDash = realSetLineDash;
                      ctx.moveTo = realMoveTo;
                      ctx.lineTo = realLineTo;
                      ctx.stroke = realStroke;
                    });
                  }
                };
              }
            };
          });
        };
      }
      this._manager.addDrawing(drawing);
      this._manager.selectDrawing(drawing.id);
    }
  }

  // ── SVG Icons ───────────────────────────────────────────────────
  static _buildIcons() {
    const s = (inner) => `<svg viewBox="0 0 28 28" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.5">${inner}</svg>`;
    return {
      'trend-line': s('<line x1="4" y1="22" x2="24" y2="6"/>'),
      'ray': s('<line x1="4" y1="20" x2="24" y2="8"/><circle cx="4" cy="20" r="2" fill="currentColor"/>'),
      'horizontal-line': s('<line x1="2" y1="14" x2="26" y2="14"/>'),
      'horizontal-ray': s('<line x1="4" y1="14" x2="26" y2="14"/><circle cx="4" cy="14" r="2" fill="currentColor"/><polyline points="22,10 26,14 22,18" stroke-width="1.5"/>'),
      'parallel-channel': s('<line x1="3" y1="18" x2="25" y2="10"/><line x1="3" y1="24" x2="25" y2="16"/>'),
      'fib-retracement': s('<line x1="3" y1="6" x2="25" y2="6"/><line x1="3" y1="14" x2="25" y2="14"/><line x1="3" y1="22" x2="25" y2="22"/><text x="2" y="5" font-size="4" stroke="none" fill="currentColor">0</text><text x="2" y="13" font-size="4" stroke="none" fill="currentColor">.5</text><text x="2" y="21" font-size="4" stroke="none" fill="currentColor">1</text>'),
      'long-position': s('<rect x="6" y="4" width="16" height="8" fill="rgba(38,166,154,0.3)" stroke="#26A69A"/><rect x="6" y="16" width="16" height="8" fill="rgba(239,83,80,0.3)" stroke="#EF5350"/><line x1="4" y1="12" x2="24" y2="12" stroke="#2196F3" stroke-width="2"/>'),
      'short-position': s('<rect x="6" y="4" width="16" height="8" fill="rgba(239,83,80,0.3)" stroke="#EF5350"/><rect x="6" y="16" width="16" height="8" fill="rgba(38,166,154,0.3)" stroke="#26A69A"/><line x1="4" y1="12" x2="24" y2="12" stroke="#2196F3" stroke-width="2"/>'),
      'date-price-range': s('<rect x="4" y="6" width="20" height="16" rx="1" stroke-dasharray="3 2"/><line x1="8" y1="14" x2="20" y2="14"/><line x1="14" y1="8" x2="14" y2="20"/>'),
      'arrow': s('<line x1="6" y1="22" x2="22" y2="6"/><polyline points="16,6 22,6 22,12"/>'),
      'arrow-mark-up': s('<line x1="14" y1="22" x2="14" y2="6"/><polyline points="8,12 14,6 20,12"/>'),
      'arrow-mark-down': s('<line x1="14" y1="6" x2="14" y2="22"/><polyline points="8,16 14,22 20,16"/>'),
      'rectangle': s('<rect x="4" y="6" width="20" height="16" rx="1"/>'),
      'rotated-rectangle': s('<rect x="7" y="5" width="18" height="14" rx="1" transform="rotate(15 14 14)"/>'),
      'ellipse': s('<ellipse cx="14" cy="14" rx="12" ry="7"/>'),
      'text-annotation': s('<text x="6" y="20" font-size="18" font-weight="bold" stroke="none" fill="currentColor">T</text>'),
      'anchored-text': s('<text x="6" y="18" font-size="14" stroke="none" fill="currentColor">Aa</text><line x1="4" y1="22" x2="24" y2="22" stroke-dasharray="2 2"/>'),
      'price-label': s('<rect x="3" y="8" width="22" height="12" rx="3"/><text x="8" y="17" font-size="9" stroke="none" fill="currentColor">$</text>'),
    };
  }

  static _trashIcon() {
    return `<svg viewBox="0 0 28 28" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.5">
      <polyline points="6,8 22,8"/><line x1="10" y1="8" x2="10" y2="5"/><line x1="18" y1="8" x2="18" y2="5"/>
      <rect x="7" y="8" width="14" height="15" rx="1"/><line x1="11" y1="12" x2="11" y2="20"/>
      <line x1="14" y1="12" x2="14" y2="20"/><line x1="17" y1="12" x2="17" y2="20"/></svg>`;
  }
}
