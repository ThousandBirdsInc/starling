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
N = 96                  # birds
FRAMES = 168            # samples per blend period T
WARMUP = 700            # let the flock settle before recording
DT = 1.0
MARGIN = 30
XMIN, XMAX = MARGIN, W - MARGIN
YMIN, YMAX = MARGIN - 24, H - MARGIN + 16

# Birds move fast enough (sim units) to actually keep up with the swept
# attractor; the long animation duration makes this read as smooth gliding, not
# darting. Speed clamp + separation keep the blob shimmering rather than rigid.
MAX_SPEED = 13.0
MIN_SPEED = 6.5
NEIGH = 120.0          # alignment / cohesion radius
SEP = 32.0            # separation radius (bigger -> looser, airier cloud)
W_ALIGN = 0.05
W_COH = 0.0026
W_SEP = 0.18
W_BOUND = 0.06
JITTER = 0.55          # livelier flicker (scaled with the higher speed)

# A moving attractor sweeps the flock across the banner on a figure-eight, and a
# vortex term curls velocity around that point so the flock *wheels* like a real
# murmuration. The sweep completes once per *visible* loop (period T/2) so it
# survives the raised-cosine blend instead of averaging out; a strong chase
# keeps the flock centre tracking the attractor (also T/2-periodic) so the whole
# murmuration translates and banks across the frame.
ROOST_Y = H * 0.46
ATTRACT_AX = 250.0      # horizontal sweep amplitude
ATTRACT_AY = 78.0       # vertical figure-eight amplitude
W_ROOST = 0.020         # chase strength toward the moving attractor
VORTEX_MAG = 2.2        # rotational curl around the attractor (wheeling)


def attractor(phase):
    # The visible loop has period T/2 (the raised-cosine blend makes the two
    # halves identical), so the sweep must complete in T/2 to survive blending:
    # at phase f and f+1/2 the attractor must be in the *same* place, hence the
    # 4*pi (one full horizontal sweep per visible loop) and 8*pi (figure-eight).
    return (W * 0.5 + ATTRACT_AX * math.sin(4 * math.pi * phase),
            ROOST_Y + ATTRACT_AY * math.sin(8 * math.pi * phase))

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


def step(phase):
    rx, ry = attractor(phase)
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
        # chase the moving attractor
        adx = rx - b["x"]
        ady = ry - b["y"]
        b["vx"] += adx * W_ROOST
        b["vy"] += ady * W_ROOST
        # vortex: steady curl perpendicular to the attractor radius (CCW wheel)
        inv_r = 1.0 / (math.hypot(adx, ady) + 1e-6)
        b["vx"] += -ady * inv_r * VORTEX_MAG
        b["vy"] += adx * inv_r * VORTEX_MAG
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


# warm up with the attractor already cycling so the flock is mid-wheel on frame 0
for k in range(WARMUP):
    step((k % FRAMES) / FRAMES)

# record one period: raw[bird][frame] = (x, y)
raw = [[] for _ in range(N)]
for f in range(FRAMES):
    for i, b in enumerate(birds):
        raw[i].append((b["x"], b["y"]))
    step(f / FRAMES)

import os
if os.environ.get("DEBUG"):
    rcx = [sum(raw[i][f][0] for i in range(N)) / N for f in range(FRAMES)]
    rcy = [sum(raw[i][f][1] for i in range(N)) / N for f in range(FRAMES)]
    import sys
    print(f"[dbg] raw flock-centre x[{min(rcx):.0f}..{max(rcx):.0f}] "
          f"y[{min(rcy):.0f}..{max(rcy):.0f}]", file=sys.stderr)

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

  <!-- wordmark the flock wheels around: bright fill + dark stroke halo so it
       stays high-contrast and legible even as birds pass in front of it -->
  <text class="wm" x="600" y="206" text-anchor="middle" font-size="92" font-weight="700"
        letter-spacing="14" paint-order="stroke" stroke="#000000" stroke-opacity="0.6"
        stroke-width="6" fill="#ffe6a0" fill-opacity="0.97">STARLING</text>
  <text class="wm" x="600" y="246" text-anchor="middle" font-size="15" font-weight="600"
        letter-spacing="7" paint-order="stroke" stroke="#000000" stroke-opacity="0.5"
        stroke-width="3" fill="#f3cf78" fill-opacity="0.85">A&#160;&#160;LOCAL&#160;&#160;DEV&#160;&#160;ORCHESTRATOR</text>
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
