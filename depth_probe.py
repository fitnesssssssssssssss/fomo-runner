#!/usr/bin/env python3
"""Depth probe: what does a $100 clip actually cost, measured at the moment we would trade.

Writes state/token_depth.json (latest) and appends state/depth_log.jsonl (rolling, 20k lines).
Every record carries the position it belongs to, so depth can be joined to outcomes later.
Free, keyless, rides the existing run. No new workflow, no agent step.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')
MAX_LOG_LINES = 20000
NOTIONAL = 100.0

# DexScreener chain ids by fomo networkId. Anything unmapped is recorded as unsupported
# instead of being silently dropped, so coverage gaps stay visible.
CHAIN_BY_NETWORK = {
    1399811149: 'solana',
    4663: 'robinhood',   # verified against DexScreener 2026-11-26: 1 pair returned
}


def _load(name, default):
    p = os.path.join(STATE, name)
    if not os.path.exists(p):
        return default
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return default


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


def candidate_tokens():
    """Held positions (with their identity) plus majority-log tokens, keyed by address."""
    cands = {}
    port = _load('paper_portfolio.json', {})
    for pos in port.get('positions', []):
        addr = pos.get('tokenAddress')
        if not addr:
            continue
        chain = CHAIN_BY_NETWORK.get(int(pos.get('networkId') or 0))
        cands[addr] = {
            'source': 'position',
            'pid': pos.get('pid'),
            'clan': pos.get('clan'),
            'symbol': pos.get('symbol'),
            'sizeUsd': pos.get('sizeUsd'),
            'openedAt': pos.get('openedAt'),
            'chain': chain,
            'networkId': pos.get('networkId'),
        }
    maj = _load('majority_log.json', {})
    clan_meta = _load('clan_meta.json', {})
    for clan_name, clan in (maj.get('clans') or {}).items():
        toks = clan.get('tokens') or {}
        members = 0
        for meta in (clan_meta or {}).values():
            if meta.get('clanId') == clan_name:
                members = meta.get('memberCount') or 0
        for addr, info in toks.items():
            if addr in cands:
                cands[addr].setdefault('holders', info.get('holders'))
                continue
            if addr.startswith('So11111111111111111111111111111111111111112'):
                continue
            holders = info.get('holders') or 0
            cands[addr] = {
                'source': 'majority',
                'chain': 'solana',
                'symbol': info.get('symbol'),
                'holders': holders,
                'memberCount': members,
                'majorityShare': round(holders / members, 3) if members else None,
            }
    return cands


def fetch_depth(addrs, chain='solana'):
    out = {}
    for i in range(0, len(addrs), 25):
        chunk = addrs[i:i + 25]
        url = f"https://api.dexscreener.com/tokens/v1/{chain}/{','.join(chunk)}"
        req = urllib.request.Request(url, headers={'user-agent': 'fomo-depth/1.0'})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                pairs = json.loads(r.read().decode())
        except Exception as e:
            print(f"depth: fetch failed for chunk {i}: {e}", flush=True)
            continue
        if not isinstance(pairs, list):
            continue
        for p in pairs:
            addr = ((p.get('baseToken') or {}).get('address')) or ''
            liq = ((p.get('liquidity') or {}).get('usd')) or 0
            vol = ((p.get('volume') or {}).get('h24')) or 0
            cur = out.get(addr) or {'liqUsd': 0, 'vol24h': 0, 'pairs': 0}
            cur['liqUsd'] = max(cur['liqUsd'], float(liq or 0))
            cur['vol24h'] = max(cur['vol24h'], float(vol or 0))
            cur['pairs'] += 1
            cur['priceUsd'] = p.get('priceUsd')
            cur['dexId'] = p.get('dexId')
            cur['createdAt'] = p.get('pairCreatedAt')
            out[addr] = cur
    return out


def cost_bps(liq_usd, notional=NOTIONAL):
    """Constant-product estimate: one side of the pool is ~half the quoted liquidity."""
    if not liq_usd or liq_usd <= 0:
        return None
    return round(10000.0 * notional / (0.5 * liq_usd), 1)


def main():
    cands = candidate_tokens()
    if not cands:
        print('depth: nothing to probe')
        return 0
    by_chain = {}
    unsupported = []
    for addr, meta in cands.items():
        chain = meta.get('chain')
        if chain:
            by_chain.setdefault(chain, []).append(addr)
        else:
            unsupported.append({'addr': addr, 'networkId': meta.get('networkId'),
                                'pid': meta.get('pid'), 'symbol': meta.get('symbol')})
    depth = {}
    # DexScreener echoes EVM addresses in checksum case; match case-insensitively or the
    # position linkage is silently lost for every non-Solana token.
    lower_map = {a.lower(): a for a in cands}
    for chain, addrs in by_chain.items():
        for addr, rec in fetch_depth(addrs, chain).items():
            canon = addr if addr in cands else lower_map.get(addr.lower())
            rec.update(cands.get(canon) or {})
            rec['chain'] = chain
            rec['impactBps100'] = cost_bps(rec['liqUsd'])
            depth[addr] = rec
    now = int(time.time())
    for rec in depth.values():
        rec['at'] = now
        opened = rec.get('openedAt')
        if opened:
            try:
                from datetime import datetime
                rec['hoursSinceOpen'] = round(
                    (now - datetime.fromisoformat(opened.replace('Z', '+00:00')).timestamp()) / 3600.0, 2)
            except Exception:
                pass
    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, 'token_depth.json'), 'w') as f:
        json.dump(depth, f, indent=1, sort_keys=True)
    logp = os.path.join(STATE, 'depth_log.jsonl')
    with open(logp, 'a') as f:
        for addr, rec in depth.items():
            f.write(json.dumps({'at': now, 'addr': addr, **rec}) + '\n')
        for u in unsupported:
            f.write(json.dumps({'at': now, 'addr': u['addr'], 'chain': None,
                                'unsupportedChain': True, 'networkId': u['networkId'],
                                'pid': u['pid'], 'symbol': u['symbol']}) + '\n')
    _rotate(logp)
    print(f"depth: probed {len(cands)} tokens across {len(by_chain)} chain(s), "
          f"got {len(depth)} with pools, {len(unsupported)} on unmapped chains")
    thin = [a for a, r in depth.items() if (r.get('impactBps100') or 0) > 200]
    print(f"depth: {len(thin)} tokens would cost >2% per $100 clip")
    return 0


def _run_pool_probe():
    for mod in ('candle_probe', 'new_pool_probe'):
        try:
            __import__(mod).main()
        except Exception as e:
            print(f"depth: {mod} skipped: {e}", flush=True)


if __name__ == '__main__':
    _rc = main()
    _run_pool_probe()
    sys.exit(_rc)
