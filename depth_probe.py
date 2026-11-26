#!/usr/bin/env python3
"""Depth probe — fills the one data gap that decides whether any of this is tradeable.

For every token we hold or have just signalled, fetch live pool depth from DexScreener
(keyless, free) and write state/token_depth.json. No new workflow, no agent step, no
credits: it rides the existing cron run.

Why: our cost model has been a flat 4% guess. Depth turns it into a measurement —
a $100 clip into a $40k pool costs roughly 0.25% one way, into a $2k pool roughly 5%.
"""
import json
import os
import sys
import time
import urllib.request

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')
MAX_LOG_LINES = 20000
CHAIN_IDS = {1399811149: 'solana'}   # DexScreener slugs; unknown chains are logged, never guessed
SOLANA = {1399811149, '1399811149', 'solana'}
BATCH = 30


def _load(name, default):
    p = os.path.join(STATE, name)
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return default


def candidate_tokens():
    """Held Solana positions plus every token in the majority log (both are Solana-side)."""
    addrs = set()
    port = _load('paper_portfolio.json', {})
    for pos in port.get('positions', []):
        if str(pos.get('networkId')) in {str(x) for x in SOLANA}:
            addrs.add(pos.get('tokenAddress'))
    maj = _load('majority_log.json', {})
    for clan in (maj.get('clans') or {}).values():
        for addr in (clan.get('tokens') or {}):
            addrs.add(addr)
    return sorted(a for a in addrs if a and not a.startswith('So11111111111111111111111111111111111111112'))


def fetch_depth(addrs):
    out = {}
    for i in range(0, len(addrs), BATCH):
        chunk = addrs[i:i + BATCH]
        url = 'https://api.dexscreener.com/tokens/v1/solana/' + ','.join(chunk)
        req = urllib.request.Request(url, headers={'User-Agent': 'fomo-depth/1.0'})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                pairs = json.load(r)
        except Exception as e:
            print(f'depth batch {i // BATCH} failed: {e}', file=sys.stderr)
            continue
        for p in pairs or []:
            addr = (p.get('baseToken') or {}).get('address')
            if not addr:
                continue
            liq = ((p.get('liquidity') or {}).get('usd') or 0)
            vol = ((p.get('volume') or {}).get('h24') or 0)
            rec = out.setdefault(addr, {'liqUsd': 0, 'vol24h': 0, 'pairs': 0, 'priceUsd': None, 'createdAt': None})
            rec['liqUsd'] += liq
            rec['vol24h'] = max(rec['vol24h'], vol)
            rec['pairs'] += 1
            if rec['priceUsd'] is None:
                rec['priceUsd'] = p.get('priceUsd')
                rec['createdAt'] = p.get('pairCreatedAt')
        time.sleep(0.3)
    return out


def cost_bps(liq_usd, notional=100.0):
    """Rough one-way price impact in bps for a market order of `notional`."""
    if not liq_usd or liq_usd <= 0:
        return None
    return round(10000.0 * notional / (0.5 * liq_usd), 1)


def main():
    addrs = candidate_tokens()
    if not addrs:
        print('depth: nothing to probe')
        return 0
    depth = fetch_depth(addrs)
    now = int(time.time())
    for addr, rec in depth.items():
        rec['impactBps100'] = cost_bps(rec['liqUsd'])
        rec['at'] = now
    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, 'token_depth.json'), 'w') as f:
        json.dump(depth, f, indent=1, sort_keys=True)
    # rolling history: one line per run so depth-at-signal-time can be joined to outcomes later
    with open(os.path.join(STATE, 'depth_log.jsonl'), 'a') as f:
        for addr, rec in depth.items():
            f.write(json.dumps({'at': now, 'addr': addr, **rec}) + '\n')
    _rotate(os.path.join(STATE, 'depth_log.jsonl'))
    print(f'depth: probed {len(addrs)} tokens, got {len(depth)} with pools')
    thin = [a for a, r in depth.items() if (r.get('impactBps100') or 0) > 200]
    print(f'depth: {len(thin)} tokens would cost >2% per $100 clip')
    return 0


def _rotate(path):
    """Keep the append-only logs from growing the repo without bound."""
    try:
        with open(path) as f:
            lines = f.readlines()
        if len(lines) > MAX_LOG_LINES:
            with open(path, 'w') as f:
                f.writelines(lines[-MAX_LOG_LINES:])
    except Exception as e:
        print(f"depth: rotation skipped: {e}", flush=True)


def _run_pool_probe():
    """Phase 2 capture rides this script so the runner and workflow stay untouched."""
    try:
        import new_pool_probe
        new_pool_probe.main()
    except Exception as e:
        print(f"depth: pool probe skipped: {e}", flush=True)


if __name__ == '__main__':
    _rc = main()
    _run_pool_probe()
    sys.exit(_rc)
