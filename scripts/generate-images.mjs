/**
 * Generates all required Homey app PNG images from inline SVG sources.
 * Run with: node scripts/generate-images.mjs
 *
 * Required sizes (measured from reference Homey app):
 *   App images      : small 250×175  large 500×350   xlarge 600×664
 *   Driver images   : small 75×75    large 500×500   xlarge 600×664
 */

import { Resvg } from '@resvg/resvg-js';
import { writeFileSync, mkdirSync } from 'fs';
import { dirname } from 'path';

// ─── Colour palette ────────────────────────────────────────────────────────
const BLUE   = '#00a0e1';
const DARK   = '#005a80';
const WHITE  = '#ffffff';

// ─── Bird path from frigate.video/images/logo.svg (first path only) ────────
const BIRD_PATH = `M2289 2326 c-6 -7 -372 -122 -478 -151 c-98 -27 -378 -134 -418 -160
c-13 -9 -23 -24 -23 -34 0 -10 25 -56 55 -102 68 -104 70 -129 19 -249
-50 -116 -81 -153 -162 -191 -37 -17 -78 -42 -92 -54 -14 -12 -50 -32 -80
-43 -30 -11 -56 -27 -58 -35 -3 -13 2 -13 36 -2 54 19 82 10 82 -26 0 -32
-40 -60 -146 -104 -254 -103 -328 -136 -362 -164 -52 -40 -59 -77 -32 -161
11 -34 20 -73 20 -85 0 -21 53 -173 135 -389 33 -86 101 -205 118 -206 5 0
6 23 2 51 -6 45 -15 292 -17 504 -2 108 20 159 100 237 76 75 191 154 293
203 68 32 75 34 153 28 84 -6 223 13 299 40 27 10 52 11 80 6 54 -10 397
-11 397 -1 0 4 -32 13 -71 19 -105 16 -107 30 -5 38 166 14 75 35 -152 35
-149 0 -178 8 -231 63 -52 55 -77 199 -50 287 6 19 14 100 19 180 4 80 11
153 15 163 9 23 121 88 226 131 46 19 120 50 164 69 70 30 166 71 230 97
15 7 11 9 -21 9 -22 1 -43 -1 -45 -3z`;

// Bird rendered using original SVG transform from the official logo
const bird = (fill = WHITE) =>
  `<g transform="translate(0,256) scale(0.1,-0.1)" fill="${fill}"><path d="${BIRD_PATH}"/></g>`;

// ─── SVG builders ──────────────────────────────────────────────────────────

/**
 * App marketing image (landscape).
 * Bird on left, "Frigate NVR" label on right, solid blue bg.
 */
function appImageSvg(w, h) {
  // Scale bird to ~75% of height, centred vertically
  // Original bird SVG bbox: x[63,235] y[23,239] in a 900×256 viewBox
  const birdH = 256; // natural height
  const targetH = h * 0.72;
  const scale = targetH / birdH;
  const birdW = 172 * scale;   // natural width ≈ 172
  const birdX = w * 0.07;
  const birdY = (h - targetH) / 2;

  // Font sizes relative to height
  const fs1 = Math.round(h * 0.18);
  const fs2 = Math.round(h * 0.10);
  const textX = birdX + birdW + w * 0.06;
  const textY = h / 2;

  return `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="${DARK}"/>
      <stop offset="100%" stop-color="${BLUE}"/>
    </linearGradient>
  </defs>
  <rect width="${w}" height="${h}" fill="url(#bg)"/>
  <!-- Bird: shift viewBox to bird region [28,0,256,256], scale to fit -->
  <g transform="translate(${birdX - 28 * scale}, ${birdY}) scale(${scale})">
    ${bird(WHITE)}
  </g>
  <!-- Text -->
  <text x="${textX}" y="${textY - fs1 * 0.3}" font-family="sans-serif" font-weight="700"
        font-size="${fs1}" fill="${WHITE}">Frigate</text>
  <text x="${textX}" y="${textY + fs1 * 0.65}" font-family="sans-serif" font-weight="300"
        font-size="${fs2}" fill="${WHITE}" opacity="0.85">NVR Integration</text>
</svg>`;
}

/**
 * Camera driver image (square or near-square).
 * Camera icon centred on blue bg, Frigate bird watermark bottom-right.
 */
function driverImageSvg(w, h) {
  const cx = w / 2;
  const cy = h * 0.47;
  const camW = Math.min(w, h) * 0.64;
  const camH = camW * 0.66;
  const r = camW * 0.08;
  const lensR = camW * 0.23;
  const bumpH = camH * 0.22;
  const bumpW = camW * 0.32;
  const bumpY = cy - camH * 0.5 - bumpH * 0.5;

  // Bird watermark — only at larger sizes (invisible at 75×75)
  const showBird = Math.min(w, h) >= 150;
  const bmSize = Math.min(w, h) * 0.26;
  const bmScale = bmSize / 256;
  const bmX = w - bmSize * 0.85;
  const bmY = h - bmSize * 0.55;

  return `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
  <defs>
    <radialGradient id="lens" cx="38%" cy="35%" r="65%">
      <stop offset="0%" stop-color="#d0eef8"/>
      <stop offset="45%" stop-color="#0088bb"/>
      <stop offset="100%" stop-color="#003355"/>
    </radialGradient>
  </defs>
  <!-- Solid blue background — clearly visible at all sizes -->
  <rect width="${w}" height="${h}" fill="${BLUE}"/>
  ${showBird ? `<g transform="translate(${bmX},${bmY}) scale(${bmScale})" opacity="0.15">${bird(WHITE)}</g>` : ''}
  <!-- Camera body -->
  <rect x="${cx - camW/2}" y="${cy - camH/2 + bumpH/2}" width="${camW}" height="${camH}" rx="${r}" fill="${WHITE}"/>
  <!-- Viewfinder bump -->
  <rect x="${cx - bumpW/2}" y="${bumpY}" width="${bumpW}" height="${bumpH}" rx="${r * 0.6}" fill="${WHITE}"/>
  <!-- Lens rings -->
  <circle cx="${cx}" cy="${cy + camH * 0.07}" r="${lensR}" fill="${BLUE}"/>
  <circle cx="${cx}" cy="${cy + camH * 0.07}" r="${lensR * 0.75}" fill="url(#lens)"/>
  <circle cx="${cx}" cy="${cy + camH * 0.07}" r="${lensR * 0.37}" fill="#002244" opacity="0.9"/>
  <circle cx="${cx - lensR*0.28}" cy="${cy + camH * 0.07 - lensR*0.24}" r="${lensR * 0.14}" fill="${WHITE}" opacity="0.5"/>
  <!-- Flash dot -->
  <circle cx="${cx + camW * 0.38}" cy="${cy - camH * 0.26}" r="${camW * 0.06}" fill="${BLUE}"/>
</svg>`;
}

// ─── Render helper ──────────────────────────────────────────────────────────

function render(svgString, outPath, w, h) {
  mkdirSync(dirname(outPath), { recursive: true });
  const resvg = new Resvg(svgString, { fitTo: { mode: 'width', value: w } });
  const png = resvg.render().asPng();
  writeFileSync(outPath, png);
  console.log(`✓  ${outPath}  (${w}×${h})`);
}

// ─── Generate all images ───────────────────────────────────────────────────

const root = new URL('..', import.meta.url).pathname.replace(/\/$/, '');

// App marketing images
render(appImageSvg(250, 175),  `${root}/assets/images/small.png`,  250, 175);
render(appImageSvg(500, 350),  `${root}/assets/images/large.png`,  500, 350);
render(appImageSvg(600, 664),  `${root}/assets/images/xlarge.png`, 600, 664);

// Camera driver images
render(driverImageSvg(75,  75),  `${root}/drivers/camera/assets/images/small.png`,  75,  75);
render(driverImageSvg(500, 500), `${root}/drivers/camera/assets/images/large.png`,  500, 500);
render(driverImageSvg(600, 664), `${root}/drivers/camera/assets/images/xlarge.png`, 600, 664);

console.log('\nAll images generated successfully.');
