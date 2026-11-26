#!/usr/bin/env python3
"""New-pool cohort capture (phase 2).

Runs inside the existing GitHub cycle, no extra workflow, no agent step.

Why this exists: the clan signal fires after a pump has started. The only way to know
whether a leading signal is possible with our cadence is to record, at the moment we
first see a pool, how old it already was. That turns "we are hours late" from an opinion
into a number, and it gives a birth cohort we can score forward.

Writes:
  state/new_pools_latest.json  newest snapshot keyed by pool
  state/new_pools_log.jsonl    append-only, rotated to the last MAX_LINES records
"""
import json
import os
import time
import urllib.request

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')
MAX_LINES = 20000
URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1"
UA = "Mozilla/5.0 (compatible; fomo-runner/1.0)"


def log(msg):
    print(f"[newpools] {msg}", flush=True)


def fetch():
    req = urllib.request.Request(URL, headers={"accept": "application/json", "user-agent": UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.load(r)


def num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def main():
    now = time.time()
    try:
        payload = fetch()
    except Exception as e:
        log(f"fetch failed: {e}")
        return

    rows = []
    for item in payload.get('data') or []:
        a = item.get('attributes') or {}
        base = a.get('base_token') or {}
        created = a.get('pool_created_at') or ''
        try:
            created_ts = time.mktime(time.strptime(created[:19], '%Y-%m-%dT%H:%M:%S')) - time.timezone
        except Exception:
            created_ts = 0
        reserve = num(a.get('reserve_in_usd'))
        if reserve < 5000:            # dust pools are not tradable at $100
            continue
        tx = (a.get('transactions') or {}).get('h24') or {}
        rows.append({
            'pool': a.get('address') or item.get('id', '').split('_', 1)[-1],
            'token': base.get('address') or '',
            'symbol': base.get('symbol') or '',
            'createdAt': created,
            'ageH': round((now - created_ts) / 3600.0, 2) if created_ts else None,
            'reserveUsd': round(reserve, 0),
            'vol24h': round(num((a.get('volume_usd') or {}).get('h24')), 0),
            'buys24h': int(num(tx.get('buys'))),
            'sells24h': int(num(tx.get('sells'))),
            'priceUsd': (a.get('base_token_price_usd') or ''),
        })

    if not rows:
        log("nothing above the reserve floor this cycle")
        return

    latest = {r['token'] or r['pool']: r for r in rows}
    with open(os.path.join(STATE, 'new_pools_latest.json'), 'w') as f:
        json.dump({'at': now, 'pools': latest}, f, indent=1, sort_keys=True)

    path = os.path.join(STATE, 'new_pools_log.jsonl')
    with open(path, 'a') as f:
        for r in rows:
            f.write(json.dumps({'firstSeen': now, **r}) + '\n')
    try:
        with open(path) as f:
            lines = f.readlines()
        if len(lines) > MAX_LINES:
            with open(path, 'w') as f:
                f.writelines(lines[-MAX_LINES:])
    except Exception as e:
        log(f"rotation skipped: {e}")

    ages = [r['ageH'] for r in rows if r.get('ageH') is not None]
    med = sorted(ages)[len(ages) // 2] if ages else None
    log(f"captured {len(rows)} pools above ${5000:,.0f}; median age at first sight {med}h")


if __name__ == '__main__':
    main()
