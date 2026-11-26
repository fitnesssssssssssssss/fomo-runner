#!/usr/bin/env python3
"""Candle probe: real OHLCV from GeckoTerminal (keyless, free) for every token we hold or signal.

This replaces the trade-print price proxy that biased every earlier analysis: forward returns
are computed from actual candles, at 5m/1h/6h/24h horizons, and the record carries pool reserve
and 24h volume so depth and outcome can be joined later.
"""
import json
import os
import time
import urllib.error
import urllib.request

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')
MAX_TOKENS = 6           # 2 calls each; free tier 429s hard, so stay well under 30/min
MAX_LOG_LINES = 40000
GT = 'https://api.geckoterminal.com/api/v2'
NET_BY_CHAIN = {'solana': 'solana'}   # geckoterminal network slugs; unknown chains are skipped

HORIZONS = [('r5m', 300), ('r1h', 3600), ('r6h', 21600), ('r24h', 86400)]


def _get(url, retries=1):
    req = urllib.request.Request(url, headers={'user-agent': 'fomo-candles/1.0', 'accept': 'application/json'})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(30)
                continue
            raise


def _rotate(path):
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            lines = f.readlines()
        if len(lines) > MAX_LOG_LINES:
            with open(path, 'w') as f:
                f.writelines(lines[-MAX_LOG_LINES:])
    except Exception:
        pass


def top_pool(net, addr):
    d = _get(f"{GT}/networks/{net}/tokens/{addr}/pools?page=1")
    best = None
    for p in (d.get('data') or []):
        a = p.get('attributes') or {}
        try:
            res = float(a.get('reserve_in_usd') or 0)
        except Exception:
            res = 0.0
        if best is None or res > best['reserve']:
            best = {'pool': a.get('address'), 'reserve': res,
                    'vol24h': float((a.get('volume_usd') or {}).get('h24') or 0),
                    'name': a.get('name')}
    return best


def candles(net, pool, limit=100):
    d = _get(f"{GT}/networks/{net}/pools/{pool}/ohlcv/minute?aggregate=5&limit={limit}&currency=usd")
    return (d.get('data') or {}).get('attributes', {}).get('ohlcv_list') or []


def summarise(lst, now):
    """lst is newest-first: [ts, o, h, l, c, v]."""
    if not lst:
        return {}
    close_now = float(lst[0][4])
    out = {'close': close_now, 'candles': len(lst)}
    for key, secs in HORIZONS:
        target = now - secs
        past = [c for c in lst if c[0] <= target]
        if past:
            base = float(past[0][4])
            if base:
                out[key] = round(100.0 * (close_now / base - 1), 2)
    if len(lst) > 1:
        out['high'] = max(float(c[2]) for c in lst)
        out['low'] = min(float(c[3]) for c in lst)
    return out


def main():
    p = os.path.join(STATE, 'token_depth.json')
    if not os.path.exists(p):
        print('candles: no depth file yet')
        return 0
    depth = json.load(open(p))
    now = int(time.time())
    logp = os.path.join(STATE, 'candles_log.jsonl')
    rows, ok, skipped = 0, 0, 0
    for addr, rec in list(depth.items())[:MAX_TOKENS]:
        net = NET_BY_CHAIN.get(rec.get('chain'))
        if not net:
            skipped += 1
            continue
        try:
            tp = top_pool(net, addr)
            if not tp or not tp.get('pool'):
                skipped += 1
                continue
            lst = candles(net, tp['pool'])
            s = summarise(lst, now)
            if not s:
                skipped += 1
                continue
            row = {'at': now, 'addr': addr, 'net': net, 'pool': tp['pool'],
                   'reserveUsd': round(tp['reserve'], 1), 'vol24h': round(tp['vol24h'], 1),
                   'symbol': rec.get('symbol'), 'source': rec.get('source'), 'pid': rec.get('pid'), **s}
            with open(logp, 'a') as f:
                f.write(json.dumps(row) + '\n')
            rows += 1
            ok += 1
        except Exception as e:
            print(f"candles: {addr[:8]} failed: {str(e)[:80]}", flush=True)
            skipped += 1
        time.sleep(2.5)
    _rotate(logp)
    print(f"candles: {ok} tokens captured, {skipped} skipped")
    return 0


def _run_chain():
    try:
        import new_pool_probe
        new_pool_probe.main()
    except Exception as e:
        print(f"candles: pool probe skipped: {e}", flush=True)


if __name__ == '__main__':
    rc = main()
    _run_chain()
    raise SystemExit(rc)
