# prices-repo

This repository hosts the static middle pricing layer for BrickWorth.

- **Repo:** `https://github.com/Brujupeek1997/prices-repo`
- **GitHub Pages base:** `https://brujupeek1997.github.io/prices-repo/`
- **App-consumed file:** `https://brujupeek1997.github.io/prices-repo/prices.json`

## What it does

`prices.json` stores real condition-aware pricing snapshots for tracked popular LEGO sets and minifigures.

The Android app uses this file as its **layer 2** lookup path:

1. local on-device item cache
2. downloaded `prices.json`
3. direct BrickLink fetch for rare misses

## Updating

Tracked items live in `tracked_items.json`.

Run:

```bash
python update_prices.py
```

The script:

1. loads the tracked set/minifigure manifest
2. fetches BrickLink listing data
3. computes arithmetic averages by condition bucket
4. uses official LEGO Store pricing for upcoming sets that still have no BrickLink market data
5. writes the generated `prices.json`

## Automation

GitHub Actions runs `.github/workflows/update-prices.yml` nightly and on manual dispatch.

The workflow only commits when `prices.json` materially changes.
