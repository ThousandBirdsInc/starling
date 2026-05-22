#!/usr/bin/env python3
"""Generate a cleanly-looping starling-murmuration (boids) banner SVG.

A real boids simulation never returns to its initial configuration, so a naive
recording does not loop. To get a *provably* seamless, C1-continuous loop we
record one period T of the simulation and then blend each boid's trajectory
with a half-period-shifted copy of itself using a raised-cosine window:

    w(t)  = 1/2 (1 + cos(2*pi*t/T))                     # 1 at t=0,T ; 0 at T/2
    f(t)  = (1 - w(t)) * g(t) + w(t) * g((t + T/2) mod T)

At t=0 and t=T, w=1 and w'=0, so the (unrelated) endpoints of the raw
trajectory g are masked with both zero weight and zero slope; the surviving
term g((t+T/2) mod T) is identical at both ends. The result is therefore equal
in value and first derivative across the seam -> the murmuration loops with no
pop, while still reading as flocking (the blend only diffuses mid-loop, which
looks like the flock breathing). Output is emitted as one <animateMotion> per
bird with rotate="auto" so each bird orients along its own travel direction.
"""

import math
import random

random.seed(7)

# ---- canvas (matches the sibling palimpsest banner) -----------------------
W, H = 1200, 360

# ---- simulation parameters ------------------------------------------------
N = 80                  # birds
FRAMES = 120            # samples per blend period T
WARMUP = 500            # let the flock settle before recording
DT = 1.0
MARGIN = 60
XMIN, XMAX = MARGIN, W - MARGIN
YMIN, YMAX = MARGIN - 20, H - MARGIN + 10

MAX_SPEED = 3.1
MIN_SPEED = 1.7
NEIGH = 78.0            # alignment / cohesion radius
SEP = 20.0             # separation radius
W_ALIGN = 0.045
W_COH = 0.0032
W_SEP = 0.08
W_BOUND = 0.045
JITTER = 0.06

# pull the flock loosely toward a roost so it stays framed around the wordmark
ROOST = (W * 0.5, H * 0.46)
W_ROOST = 0.0009

birds = []
for _ in range(N):
    a = random.uniform(0, 2 * math.pi)
    s = random.uniform(MIN_SPEED, MAX_SPEED)
    birds.append({
        "x": random.uniform(XMIN, XMAX),
        "y": random.uniform(YMIN, YMAX),
        "vx": math.cos(a) * s,
        "vy": math.sin(a) * s,
    })


def limit_speed(b):
    s = math.hypot(b["vx"], b["vy"]) or 1e-9
    if s > MAX_SPEED:
        b["vx"] *= MAX_SPEED / s
        b["vy"] *= MAX_SPEED / s
    elif s < MIN_SPEED:
        b["vx"] *= MIN_SPEED / s
        b["vy"] *= MIN_SPEED / s


def step():
    for b in birds:
        ax = ay = 0.0
        cx = cy = 0.0
        sx = sy = 0.0
        n = 0
        for o in birds:
            if o is b:
                continue
            dx = o["x"] - b["x"]
            dy = o["y"] - b["y"]
            d2 = dx * dx + dy * dy
            if d2 < NEIGH * NEIGH:
                ax += o["vx"]
                ay += o["vy"]
                cx += o["x"]
                cy += o["y"]
                n += 1
                if d2 < SEP * SEP:
                    inv = 1.0 / (math.sqrt(d2) + 1e-6)
                    sx -= dx * inv
                    sy -= dy * inv
        if n:
            b["vx"] += (ax / n - b["vx"]) * W_ALIGN
            b["vy"] += (ay / n - b["vy"]) * W_ALIGN
            b["vx"] += (cx / n - b["x"]) * W_COH
            b["vy"] += (cy / n - b["y"]) * W_COH
        b["vx"] += sx * W_SEP
        b["vy"] += sy * W_SEP
        # roost attraction
        b["vx"] += (ROOST[0] - b["x"]) * W_ROOST
        b["vy"] += (ROOST[1] - b["y"]) * W_ROOST
        # soft inward push near edges
        if b["x"] < XMIN:
            b["vx"] += (XMIN - b["x"]) * W_BOUND
        elif b["x"] > XMAX:
            b["vx"] -= (b["x"] - XMAX) * W_BOUND
        if b["y"] < YMIN:
            b["vy"] += (YMIN - b["y"]) * W_BOUND
        elif b["y"] > YMAX:
            b["vy"] -= (b["y"] - YMAX) * W_BOUND
        b["vx"] += random.uniform(-JITTER, JITTER)
        b["vy"] += random.uniform(-JITTER, JITTER)
        limit_speed(b)
    for b in birds:
        b["x"] += b["vx"] * DT
        b["y"] += b["vy"] * DT


for _ in range(WARMUP):
    step()

# record one period: raw[bird][frame] = (x, y)
raw = [[] for _ in range(N)]
for _ in range(FRAMES):
    for i, b in enumerate(birds):
        raw[i].append((b["x"], b["y"]))
    step()

# ---- seamless-loop blend --------------------------------------------------
# The raised-cosine window makes f(t + T/2) == f(t), so the blended motion has
# period T/2: the two halves are identical. We exploit that and emit only the
# first half (HALF frames) as the visual loop, halving the SVG size for free.
half = FRAMES // 2
looped = [[] for _ in range(N)]
for i in range(N):
    for f in range(half):
        w = 0.5 * (1 + math.cos(2 * math.pi * f / FRAMES))
        gx, gy = raw[i][f]
        sx, sy = raw[i][(f + half) % FRAMES]
        looped[i].append(((1 - w) * gx + w * sx, (1 - w) * gy + w * sy))

# ---- emit SVG -------------------------------------------------------------
DUR = 11.0  # seconds per loop


def fmt(v):
    return f"{v:.1f}"


def bird_motion(pts):
    """Build (path, keyPoints, keyTimes) for one bird.

    All birds share the same uniform keyTimes, and each bird's keyPoints are its
    own cumulative arc-length fractions. With calcMode="linear" this places every
    bird at sample f at wall-clock time f/FRAMES * dur simultaneously, so the
    flock stays *coherent* (neighbours that aligned in the sim still align on
    screen) instead of each bird racing along its own path at its own pace.
    """
    verts = pts + [pts[0]]  # explicit return to the start closes the loop
    # path
    seg = [f"M{fmt(verts[0][0])},{fmt(verts[0][1])}"]
    for x, y in verts[1:]:
        seg.append(f"L{fmt(x)},{fmt(y)}")
    path = "".join(seg)
    # cumulative arc-length fractions -> keyPoints
    cum = [0.0]
    for j in range(1, len(verts)):
        cum.append(cum[-1] + math.hypot(verts[j][0] - verts[j - 1][0],
                                        verts[j][1] - verts[j - 1][1]))
    total = cum[-1] or 1.0
    key_points = ";".join(f"{c / total:.4f}" for c in cum)
    n = len(verts) - 1
    key_times = ";".join(f"{j / n:.3f}" for j in range(len(verts)))
    return path, key_points, key_times


parts = []
parts.append(
    f'<svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" fill="none" '
    'xmlns="http://www.w3.org/2000/svg" role="img" '
    'aria-label="Starling — a murmuration of boids flocking">'
)
parts.append('''
  <defs>
    <linearGradient id="sky" x1="0" y1="0" x2="0" y2="360" gradientUnits="userSpaceOnUse">
      <stop offset="0" stop-color="#070a12"/>
      <stop offset="0.5" stop-color="#0b0d12"/>
      <stop offset="1" stop-color="#0d0b08"/>
    </linearGradient>
    <radialGradient id="dusk" cx="600" cy="150" r="520" gradientUnits="userSpaceOnUse" gradientTransform="matrix(1 0 0 0.42 0 87)">
      <stop offset="0" stop-color="#ffb01a" stop-opacity="0.20"/>
      <stop offset="0.45" stop-color="#ff7e1a" stop-opacity="0.08"/>
      <stop offset="1" stop-color="#ff7e1a" stop-opacity="0"/>
    </radialGradient>
    <radialGradient id="vignette" cx="600" cy="180" r="720" gradientUnits="userSpaceOnUse">
      <stop offset="0" stop-color="#000" stop-opacity="0"/>
      <stop offset="0.7" stop-color="#000" stop-opacity="0"/>
      <stop offset="1" stop-color="#000" stop-opacity="0.72"/>
    </radialGradient>
    <style>
      .wm { font-family: 'JetBrains Mono','SF Mono','DejaVu Sans Mono',Menlo,Consolas,monospace; }
    </style>
  </defs>

  <rect width="1200" height="360" fill="url(#sky)"/>
  <rect width="1200" height="360" fill="url(#dusk)"/>

  <!-- wordmark the flock wheels around -->
  <text class="wm" x="600" y="206" text-anchor="middle" font-size="92" font-weight="700"
        letter-spacing="14" fill="#f4c24a" fill-opacity="0.10">STARLING</text>
  <text class="wm" x="600" y="246" text-anchor="middle" font-size="15" font-weight="500"
        letter-spacing="7" fill="#c9a24a" fill-opacity="0.30">A&#160;&#160;LOCAL&#160;&#160;DEV&#160;&#160;ORCHESTRATOR</text>
''')

# birds — a tiny dart drawn pointing +x; rotate="auto" aligns it with travel
parts.append('  <g fill="#f0c558">')
for i in range(N):
    path, key_points, key_times = bird_motion(looped[i])
    # static per-bird depth variation
    scale = random.uniform(0.8, 1.45)
    op = random.uniform(0.55, 0.95)
    s = fmt(4.6 * scale)
    half_s = fmt(2.6 * scale)
    shape = f'M{s},0 L-{half_s},-{half_s} L-{fmt(1.0*scale)},0 L-{half_s},{half_s} Z'
    parts.append(
        f'    <g fill-opacity="{op:.2f}"><path d="{shape}"/>'
        f'<animateMotion dur="{DUR}s" repeatCount="indefinite" rotate="auto" '
        f'calcMode="linear" keyPoints="{key_points}" keyTimes="{key_times}" '
        f'path="{path}"/></g>'
    )
parts.append('  </g>')

parts.append('  <rect width="1200" height="360" fill="url(#vignette)"/>')
parts.append('</svg>\n')

out = "\n".join(parts)
with open(".github/starling-banner.svg", "w") as fh:
    fh.write(out)
print(f"wrote .github/starling-banner.svg  ({len(out)} bytes, {N} birds, {FRAMES} frames)")
