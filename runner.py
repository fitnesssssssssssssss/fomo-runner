#!/usr/bin/env python3
"""
Fomo clan data runner — one fetch cycle per invocation, built for GitHub Actions
scheduled runs (or `--loop` under systemd on a VPS).

Reuses the battle-tested logic from fomo_pipeline.py (fetch_clan, extract_trade_events,
analyze_clan, BotBlock handling). Differences from the sandbox pipeline:

  * State (seen-event IDs) lives in ./state/seen.json — a plain file the
    workflow commits back to the repo after each run. First run auto-initializes:
    it marks the existing feed window as seen (no duplicate stores).
  * The Privy session token lives in Base44 (AuthState entity, via the gated
    authState function) — never in the repo or in GitHub secrets.
  * PAPER TRADING SIMULATOR (added 2026-09-06): rides along each cycle at zero
    extra cost. Five virtual wallets (one per clan, $500 each, $100 per position):
      entry  -> when a meme crosses clan-majority (>50% of members hold it)
      exit   -> +100% (2x target), -50% (stop), or meme drops out of majority
      marks  -> mark-to-market each cycle from price index = value/humanAmount
    Portfolio state lives in ./state/paper_portfolio.json (committed back like
    seen.json); every open/mark/close is mirrored to the PaperTrade entity via
    the gated storePaperTrades function for the 6-hourly/daily reports.
    Non-memes (WETH, SOL, stables, tokenized stocks) are skipped by design.

v3 (2026-09-09):
  * Sim upgrade: majority timestamps (state/majority_log.json) — legacy positions
    (majority formed before v3) are no longer aped unless they show fresh bullish
    validation (holders rising + net buy flow + no recent whale profit-taking).
  * Majority-sell exit: if >=2 distinct members (or >=30% of a meme's holders, or
    sell volume >=20% of the clan's holding value) sell within 6h while we are
    below the 2x target, we exit with reason 'majority_sell_exit' — mirroring the
    clan majority instead of riding it down.
  * Fee model: 1% per side (DEX fee + slippage) applied to entries, exits, and marks.
  * Flow log: state/meme_flow.json (48h rolling buy/sell window per token, built
    from the events we already fetch — zero extra fomo requests).
  * Leaderboard + @rasmr watchlist: hourly probe of leaderboard API candidates
    (results + diagnostics stored via gated storeLeaderData for the reports to
    surface; free platform-side, no agent steps).

Exit codes: 0 = ok (or expected bot-block), 1 = failure (visible in Actions).
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

from curl_cffi import requests

import fomo_pipeline as fp

STATE_DIR = os.environ.get('FOMO_STATE_DIR',
                           os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state'))
AUTH_URL = 'https://elara-0ec48c47.base44.app/functions/authState'
BACKEND_URL = 'https://elara-0ec48c47.base44.app/functions/storeClanData'
TRADE_STORE_URL = 'https://elara-0ec48c47.base44.app/functions/storeTradeEvents'
PAPER_STORE_URL = 'https://elara-0ec48c47.base44.app/functions/storePaperTrades'
LEADER_STORE_URL = 'https://elara-0ec48c47.base44.app/functions/storeLeaderData'
SEEN_CAP = fp.MAX_SEEN

# ---- paper trading config (defaults confirmed by owner 2026-09-06) ----
PAPER_WALLET_USD = float(os.environ.get('FOMO_PAPER_WALLET', '500'))
PAPER_TRADE_USD = float(os.environ.get('FOMO_PAPER_TRADE', '100'))
PAPER_TARGET_PCT = float(os.environ.get('FOMO_PAPER_TARGET', '100'))    # +100% = 2x sell
PAPER_STOP_PCT = float(os.environ.get('FOMO_PAPER_STOP', '-50'))        # -50% stop loss
SKIP_SYMBOLS = {'WETH', 'SOL', 'WSOL', 'USDC', 'USDT', 'DAI', 'PYUSD', 'WBTC', 'WSTETH',
                'CBBTC', 'USDE', 'USDS', 'WSTETH', 'STETH', 'RETH', 'CBETH'}
SKIP_NAME_MARKERS = ('robinhood token', 'backed',)
PAPER_FEE_PCT = float(os.environ.get('FOMO_PAPER_FEE', '1.0'))   # 1% per side (fee + slippage)
WATCH_HANDLE = os.environ.get('FOMO_WATCH_HANDLE', 'rasmr').lower()
LEADER_MIN_INTERVAL = 55 * 60  # leaderboard fetch at most hourly (with natural cron jitter)

KEY = os.environ.get('FOMO_PIPELINE_KEY', fp.PIPELINE_KEY)
if not KEY:
    print("FATAL: FOMO_PIPELINE_KEY not set", flush=True)
    sys.exit(1)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def kheaders():
    return {'Content-Type': 'application/json', 'X-Pipeline-Key': KEY}


# ---------- state ----------

def log_gap_repo(reason, pause_min):
    """Persist a backoff window to the repo state dir (visible to the daily report
    and the heartbeat's expected-backoff check). fp.log_gap alone writes to the
    sandbox path, which does not exist on GitHub Actions."""
    gaps = load_state('gaps.json', [])
    gaps.append({'at': datetime.now(timezone.utc).isoformat(),
                 'reason': reason, 'pause_min': round(pause_min, 1)})
    save_state('gaps.json', gaps[-200:])


def load_state(name, default):
    p = os.path.join(STATE_DIR, name)
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return default


def save_state(name, data):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = os.path.join(STATE_DIR, name + '.tmp')
    with open(tmp, 'w') as f:
        json.dump(data, f)
    os.replace(tmp, os.path.join(STATE_DIR, name))


# ---------- auth (token lives in Base44 AuthState) ----------

def load_auth(s):
    r = s.get(AUTH_URL, headers=kheaders(), impersonate='chrome', timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"authState read failed: HTTP {r.status_code}")
    raw = (r.json() or {}).get('token') or ''
    if not raw:
        raise RuntimeError("AuthState token is empty — run the cutover step first "
                           "(seed the token from the sandbox pipeline)")
    try:
        blob = json.loads(raw)
        if isinstance(blob, dict) and blob.get('token'):
            return blob
    except json.JSONDecodeError:
        pass
    raise RuntimeError("AuthState token is not a valid {token, refresh_token} JSON blob")


def save_auth(s, blob):
    r = s.post(AUTH_URL, headers=kheaders(), impersonate='chrome', timeout=30,
               json={'token': json.dumps(blob), 'note': 'runner refresh'})
    if r.status_code != 200:
        raise RuntimeError(f"authState save failed: HTTP {r.status_code}")


def get_valid_token(s):
    blob = load_auth(s)
    left = fp.token_seconds_left(blob['token'])
    if left < 300:  # under 5 min -> refresh (rotates both tokens)
        log(f"Token has {left:.0f}s left — refreshing via Privy")
        try:
            r = s.post('https://auth.privy.io/api/v1/sessions',
                       json={"refresh_token": blob.get('refresh_token')},
                       headers={
                           'Authorization': f"Bearer {blob['token']}",
                           'Content-Type': 'application/json',
                           'Origin': 'https://fomo.family',
                           'privy-app-id': fp.PRIVY_APP_ID,
                           'privy-client-id': fp.PRIVY_CLIENT_ID,
                           'privy-client': 'react-auth:3.34.0',
                       }, impersonate='chrome', timeout=30)
            if r.status_code in fp.BLOCK_STATUSES:
                raise fp.BotBlock(f"refresh HTTP {r.status_code}")
            if r.status_code == 200:
                d = r.json()
                new_tok = d.get('token') or d.get('privy_access_token')
                if new_tok:
                    blob['token'] = new_tok
                    nr = d.get('refresh_token')
                    if nr:
                        blob['refresh_token'] = nr
                    save_auth(s, blob)
                    log("Token refreshed OK")
                    return blob['token']
            if left <= 0:
                raise RuntimeError(f"token expired and refresh failed (HTTP {r.status_code})")
            log("Refresh failed but current token still valid — continuing")
        except fp.BotBlock:
            raise
    return blob['token']


# ---------- paper trading simulator v3 ----------

def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _price_idx(h):
    """Per-token price index from the holdings item: USD value / token quantity."""
    ha = h.get('humanAmount') or 0
    val = h.get('value') or 0
    if ha and ha > 0 and val > 0:
        return val / ha
    return None


def _is_nonmeme(h):
    sym = (h.get('symbol') or '').upper().strip()
    name = (h.get('name') or '').lower()
    return sym in SKIP_SYMBOLS or any(m in name for m in SKIP_NAME_MARKERS)


def _pnl_pct(exit_idx, entry_idx):
    """Mark-to-market including the 1%-per-side fee model (fee + slippage)."""
    if not entry_idx:
        return 0.0
    f = PAPER_FEE_PCT / 100.0
    return (exit_idx * (1 - f) / (entry_idx * (1 + f)) - 1) * 100.0


def _flow_key(clan_id, addr):
    return f"{clan_id}|{addr}"


def _flow_update(all_events, flow):
    """Accumulate per-token buy/sell flow from this cycle's NEW events (no extra
    fomo requests — built from the events the fetch already returned). Prune 48h."""
    now_ts = time.time()
    for ev in all_events or []:
        typ = ev.get('type', '')
        if typ in ('large_buy', 'multi_user_buy'):
            side, pnl = 'buy', None
        elif typ in ('large_sell', 'multi_user_sell'):
            side, pnl = 'sell', (ev.get('percentPnl') or 0)
        else:
            continue
        k = _flow_key(ev.get('clanId', ''), ev.get('tokenAddress', ''))
        flow.setdefault(k, []).append({
            'ts': now_ts, 'side': side, 'user': ev.get('userHandle', ''),
            'usd': ev.get('positionSizeUsd') or 0, 'pnl': pnl})
    for k in list(flow.keys()):
        flow[k] = [f for f in flow[k] if now_ts - f['ts'] <= 48 * 3600][-60:]
        if not flow[k]:
            del flow[k]


def _flow_since(flow, clan_id, addr, hours):
    now_ts = time.time()
    return [f for f in flow.get(_flow_key(clan_id, addr), [])
            if now_ts - f['ts'] <= hours * 3600]


def _bullish_gate(info, flow48, flow24, holders, prev_holders):
    """Entry gate (owner rules 2026-09-08):
    - never ape a legacy majority (formed before v3 tracking, never dropped out)
      unless it shows fresh bullish validation: holders rising + net buy flow
      + >=2 buys in 48h;
    - fresh majority crossings pass on basic health;
    - nobody gets entered while holders are shrinking or a whale is profit-taking
      (+150% or better sell in the last 24h = distribution marker).
    Returns (ok, reason)."""
    for f in flow24:
        if f['side'] == 'sell' and (f.get('pnl') or 0) >= 150:
            return False, 'whale profit-taking within 24h'
    if info and info.get('sellExitAt') and time.time() - info['sellExitAt'] < 48 * 3600:
        return False, 'majority-sell exit cooldown (48h)'
    if prev_holders is not None and holders < prev_holders:
        return False, 'holder count shrinking'
    if info and info.get('wasLegacy') and not info.get('reCrossed'):
        buys48 = [f for f in flow48 if f['side'] == 'buy']
        net = sum(f['usd'] for f in buys48) - \
              sum(f['usd'] for f in flow48 if f['side'] == 'sell')
        if len(buys48) < 2 or net <= 0:
            return False, 'legacy majority without fresh accumulation'
    return True, ''


def _paper_close(port, pos, exit_idx, reason, now, actions):
    pnl_pct = _pnl_pct(exit_idx, pos['entryPriceIdx'])
    pnl_usd = pos['sizeUsd'] * (pnl_pct / 100.0)
    port['wallets'][pos['clan']] = port['wallets'].get(pos['clan'], 0) + pos['sizeUsd'] + pnl_usd
    port['positions'].remove(pos)
    closed = dict(pos)
    closed.update({'closedAt': now, 'exitPriceIdx': exit_idx, 'pnlPct': round(pnl_pct, 2),
                  'pnlUsd': round(pnl_usd, 2), 'reason': reason})
    port['closed'].append(closed)
    actions.append({'type': 'close', 'pid': pos['pid'], 'clan': pos['clan'],
                    'symbol': pos['symbol'], 'tokenAddress': pos['tokenAddress'],
                    'networkId': pos['networkId'], 'openedAt': pos['openedAt'],
                    'sizeUsd': pos['sizeUsd'], 'entryPriceIdx': pos['entryPriceIdx'],
                    'closedAt': now, 'exitPriceIdx': exit_idx,
                    'pnlPct': round(pnl_pct, 2), 'pnlUsd': round(pnl_usd, 2), 'reason': reason})
    log(f"PAPER SELL {pos['clan']}: {pos['symbol']} reason={reason} "
        f"pnl={pnl_pct:+.1f}% (${pnl_usd:+.2f}) wallet=${port['wallets'][pos['clan']]:.2f}")


def run_paper_cycle(raw_clans, s, all_events):
    """Simulate the majority-follow strategy on this cycle's fresh data (v3).

    raw_clans: list of raw clan dicts ({'id','name','info','holdings'}) for fetches
    that succeeded. all_events: this cycle's NEW trade events (builds the 48h flow
    log). Uses only data already fetched — zero extra fomo requests."""
    if not raw_clans:
        return
    port = load_state('paper_portfolio.json', None)
    if port is None:
        port = {'wallets': {n: PAPER_WALLET_USD for n in fp.CLANS},
                'positions': [], 'closed': [], 'started': _now_iso()}
        log(f"Paper portfolio initialized: {len(port['wallets'])} x ${PAPER_WALLET_USD:.0f} wallets")

    mlog = load_state('majority_log.json', None)
    if mlog is None:
        mlog = {'clans': {}}
    flow = load_state('meme_flow.json', {})
    _flow_update(all_events, flow)

    actions = []
    exited_this_cycle = set()
    now = _now_iso()

    for clan in raw_clans:
        name = clan['name']
        clan_id = clan.get('id') or name
        members = (clan.get('info') or {}).get('memberCount', 0) or 0
        if members < 2:
            continue
        half = members / 2.0

        price, meta = {}, {}
        for h in clan.get('holdings', []):
            addr = h.get('tokenAddress')
            idx = _price_idx(h)
            if addr and idx is not None:
                price[addr] = idx
                meta[addr] = h

        majority = {a for a, h in meta.items() if (h.get('memberCount') or 0) > half}

        # ---- majority log: timestamps, legacy flags, re-cross detection ----
        clog = mlog['clans'].setdefault(clan_id, {'legacyDone': False, 'tokens': {}})
        toks = clog['tokens']
        for a, ent in toks.items():
            if a not in majority and ent.get('droppedAt') is None:
                ent['droppedAt'] = now
        for a in majority:
            ent = toks.get(a)
            if ent is None:
                toks[a] = {'symbol': meta[a].get('symbol', ''), 'holders': meta[a].get('memberCount') or 0,
                           'prevHolders': None, 'firstAt': now, 'droppedAt': None,
                           'wasLegacy': False, 'reCrossed': False}
                log(f"MAJORITY LOG {name}: fresh majority {meta[a].get('symbol')}")
            else:
                ent['prevHolders'] = ent.get('holders')
                ent['holders'] = meta[a].get('memberCount') or 0
                if ent.get('droppedAt'):
                    ent['droppedAt'] = None
                    if ent.get('sellExitAt'):
                        ent['sellExitAt'] = None
                        log(f"MAJORITY LOG {name}: {ent.get('symbol')} re-crossed — majority-sell cooldown cleared")
                    if not ent.get('reCrossed'):
                        ent['reCrossed'] = True
                        log(f"MAJORITY LOG {name}: {ent.get('symbol')} re-crossed into majority")
        if not clog['legacyDone']:
            for a in majority:
                toks[a]['wasLegacy'] = True
            clog['legacyDone'] = True
            log(f"MAJORITY LOG {name}: legacy init — {len(majority)} existing majority meme(s) "
                f"marked legacy (no fresh entries without bullish validation)")

        # ---- exits first (frees wallet cash) ----
        for pos in [p for p in port['positions'] if p['clan'] == name]:
            addr = pos['tokenAddress']
            idx = price.get(addr)
            if idx is not None:
                pos['lastIdx'] = idx
                pnl_pct = _pnl_pct(idx, pos['entryPriceIdx'])
                if pnl_pct >= PAPER_TARGET_PCT:
                    exited_this_cycle.add((name, addr))
                    _paper_close(port, pos, idx, 'target_2x', now, actions)
                elif pnl_pct <= PAPER_STOP_PCT:
                    exited_this_cycle.add((name, addr))
                    _paper_close(port, pos, idx, 'stop_-50', now, actions)
                else:
                    # majority-sell exit: visible distribution while below target
                    h = meta.get(addr) or {}
                    holders = h.get('memberCount') or 0
                    flow6 = _flow_since(flow, clan_id, addr, 6)
                    sellers = {f['user'] for f in flow6 if f['side'] == 'sell' and f['user']}
                    sell_usd = sum(f['usd'] for f in flow6 if f['side'] == 'sell')
                    hval = h.get('value') or 0
                    need = max(2, int(0.3 * holders + 0.999)) if holders else 2
                    if len(sellers) >= need or (hval > 0 and sell_usd >= 0.2 * hval):
                        exited_this_cycle.add((name, addr))
                        toks.setdefault(addr, {}).update(
                            {'symbol': pos['symbol'], 'sellExitAt': time.time()})
                        _paper_close(port, pos, idx, 'majority_sell_exit', now, actions)
                        log(f"  majority-sell signal: {len(sellers)} seller(s) "
                            f"({', '.join(sorted(sellers))}) in 6h, need {need}")
                    elif addr not in majority:
                        exited_this_cycle.add((name, addr))
                        _paper_close(port, pos, idx, 'majority_exit', now, actions)
            else:
                last = pos.get('lastIdx') or pos['entryPriceIdx']
                exited_this_cycle.add((name, addr))
                _paper_close(port, pos, last, 'majority_exit', now, actions)

        # ---- entries (gated) ----
        for addr in sorted(majority):
            h = meta[addr]
            if _is_nonmeme(h):
                continue
            if any(p['clan'] == name and p['tokenAddress'] == addr for p in port['positions']):
                continue
            if (name, addr) in exited_this_cycle:
                continue  # cooldown: no same-cycle re-entry after an exit
            if port['wallets'].get(name, 0) < PAPER_TRADE_USD:
                continue
            info = toks.get(addr)
            flow48 = _flow_since(flow, clan_id, addr, 48)
            flow24 = _flow_since(flow, clan_id, addr, 24)
            ok, why = _bullish_gate(info, flow48, flow24,
                                    h.get('memberCount') or 0,
                                    info.get('prevHolders') if info else None)
            if not ok:
                log(f"PAPER SKIP {name}: {h.get('symbol')} — {why}")
                continue
            port['wallets'][name] -= PAPER_TRADE_USD
            pos = {'pid': f"{name}|{addr}|{now}", 'clan': name, 'symbol': h.get('symbol', ''),
                   'tokenAddress': addr, 'networkId': h.get('networkId', 0), 'openedAt': now,
                   'entryPriceIdx': price[addr], 'sizeUsd': PAPER_TRADE_USD, 'lastIdx': price[addr]}
            port['positions'].append(pos)
            actions.append({'type': 'open', **pos})
            log(f"PAPER BUY {name}: ${PAPER_TRADE_USD:.0f} {h.get('symbol')} "
                f"@idx {price[addr]:.4e} wallet=${port['wallets'][name]:.2f}")

        # ---- marks for remaining open positions (fee-adjusted) ----
        for pos in [p for p in port['positions'] if p['clan'] == name]:
            mark_pct = _pnl_pct(pos.get('lastIdx', pos['entryPriceIdx']), pos['entryPriceIdx'])
            actions.append({'type': 'mark', 'pid': pos['pid'], 'lastMarkPct': round(mark_pct, 2)})

    if actions:
        try:
            r = s.post(PAPER_STORE_URL, json={'actions': actions}, headers=kheaders(),
                       impersonate='chrome', timeout=60)
            rdata = r.json() if r.status_code == 200 else {}
            log(f"Paper trades store: {r.status_code} actions={len(actions)} "
                f"stored={rdata.get('stored')} updated={rdata.get('updated')} "
                f"failed={len(rdata.get('failed') or [])}")
        except Exception as e:
            log(f"Paper store error (portfolio state still saved locally): {e}")

    save_state('paper_portfolio.json', port)
    save_state('majority_log.json', mlog)
    save_state('meme_flow.json', flow)
    open_pos = len(port['positions'])
    open_pnl = sum(pos['sizeUsd'] * _pnl_pct(pos.get('lastIdx', pos['entryPriceIdx']),
                                             pos['entryPriceIdx']) / 100.0
                   for pos in port['positions'] if pos['entryPriceIdx'])
    log(f"Paper book: {open_pos} open, {len(port['closed'])} closed, "
        f"unrealized ${open_pnl:+.2f} (1%/side fee model)")


# ---------- leaderboard + watchlist fetch (v3) ----------

LEADER_CANDIDATES = [
    'https://prod-api.fomo.family/v2/leaderboard?limit=25',
    'https://prod-api.fomo.family/v2/leaderboards?limit=25',
    'https://prod-api.fomo.family/v2/leaderboard/all?limit=25',
    'https://prod-api.fomo.family/v2/users/leaderboard?scope=all&limit=25',
]
USER_PROBE_TEMPLATES = [
    'https://prod-api.fomo.family/v2/users?handle={h}',
    'https://prod-api.fomo.family/v2/users/handle/{h}',
    'https://prod-api.fomo.family/v2/users/{h}',
]


def _leader_store(s, items):
    """Best-effort store of LeaderData items; never fails the cycle."""
    try:
        r = s.post(LEADER_STORE_URL, json={'items': items}, headers=kheaders(),
                   impersonate='chrome', timeout=60)
        log(f"Leader store: {r.status_code} items={len(items)}")
    except Exception as e:
        log(f"Leader store error (non-fatal): {e}")


def _find_watch_user(obj, handle):
    """Walk a JSON payload for the first dict whose string fields mention the
    watchlist handle (case-insensitive). Returns the dict or None."""
    target = handle.lower()
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for v in cur.values():
                if isinstance(v, str) and target in v.lower():
                    return cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def fetch_leader_and_rasmr(s, token):
    """Hourly: probe leaderboard endpoints (auth'd, paced) and fetch the watchlist
    profile (@rasmr). All results + probe diagnostics go to LeaderData via the
    gated storeLeaderData function so reports can surface them. Free: no LLM,
    no agent steps. BotBlocks bubble up so the cycle-level gap handling fires."""
    meta = load_state('leader_meta.json', {})
    now_ts = time.time()
    if now_ts - (meta.get('lastFetch') or 0) < LEADER_MIN_INTERVAL:
        return
    meta['lastFetch'] = now_ts
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json',
               'Origin': 'https://fomo.family', 'Referer': 'https://fomo.family/'}
    now = _now_iso()
    probes, payload, ok_url = [], None, None

    for url in LEADER_CANDIDATES:
        try:
            r = s.get(url, headers=headers, impersonate='chrome', timeout=30)
            probes.append({'url': url.split('?')[0], 'status': r.status_code})
            if r.status_code in fp.BLOCK_STATUSES:
                raise fp.BotBlock(f"leader probe HTTP {r.status_code}")
            if r.status_code == 200:
                d = r.json()
                ro = d.get('responseObject') if isinstance(d, dict) else d
                if ro:
                    payload, ok_url = d, url
                    log(f"Leaderboard fetch OK via {url.split('?')[0]}")
                    break
            time.sleep(random.uniform(1.5, 3.0))
        except fp.BotBlock:
            raise
        except Exception as e:
            probes.append({'url': url.split('?')[0], 'error': str(e)[:80]})
            time.sleep(random.uniform(1.5, 3.0))

    items = []
    if payload is not None:
        meta['lastOkUrl'] = ok_url
        items.append({'key': 'leaderboard_all',
                      'payload': json.dumps(payload)[:200000],
                      'fetchedAt': now,
                      'note': f"endpoint: {ok_url}; probe results: {json.dumps(probes)[:600]}"})
        wuser = _find_watch_user(payload, WATCH_HANDLE)
        wsrc = 'leaderboard entry'
    else:
        meta['lastOkUrl'] = None
        items.append({'key': 'leaderboard_probe',
                      'payload': json.dumps({'probes': probes}),
                      'fetchedAt': now,
                      'note': 'all candidate endpoints failed — probe diagnostics for iteration'})
        wuser, wsrc = None, None

    if wuser is None:
        for tpl in USER_PROBE_TEMPLATES:
            url = tpl.format(h=WATCH_HANDLE)
            try:
                r = s.get(url, headers=headers, impersonate='chrome', timeout=30)
                probes.append({'url': url.split('?')[0], 'status': r.status_code})
                if r.status_code in fp.BLOCK_STATUSES:
                    raise fp.BotBlock(f"watchlist probe HTTP {r.status_code}")
                if r.status_code == 200:
                    d = r.json()
                    ro = d.get('responseObject') if isinstance(d, dict) else d
                    cand = _find_watch_user(d, WATCH_HANDLE) or (ro if ro else None)
                    if cand:
                        wuser, wsrc = cand, url
                        log(f"Watchlist {WATCH_HANDLE}: found via {url.split('?')[0]}")
                        break
                time.sleep(random.uniform(1.5, 3.0))
            except fp.BotBlock:
                raise
            except Exception as e:
                probes.append({'url': url.split('?')[0], 'error': str(e)[:80]})
                time.sleep(random.uniform(1.5, 3.0))

    if wuser is not None:
        items.append({'key': f'profile:{WATCH_HANDLE}',
                      'payload': json.dumps(wuser)[:100000],
                      'fetchedAt': now,
                      'note': f"source: {wsrc}"})
    else:
        items.append({'key': f'profile:{WATCH_HANDLE}',
                      'payload': json.dumps({'found': False, 'probes': probes[-6:]}),
                      'fetchedAt': now,
                      'note': 'not found yet — endpoint probes logged for iteration'})

    meta['probes'] = probes[-12:]
    _leader_store(s, items)
    save_state('leader_meta.json', meta)

# ---------- one cycle ----------

def run_cycle(s, token, dry=False):
    log("=== Fetch cycle start ===")
    seen_map = load_state('seen.json', {})     # {clanId: [eventId, ...] insertion order}
    # First run ever: mark the whole ~4-day feed window as already-seen WITHOUT
    # storing (it is already in the database) so we never create duplicates.
    first_run = not os.path.exists(os.path.join(STATE_DIR, 'seen.json'))

    results, all_events, raw_ok = [], [], []
    ev_clan = {}  # eventId -> clanId (to un-mark failed stores)
    all_ok = True

    for name, clan_id in fp.CLANS.items():
        clan, ok = fp.fetch_clan(s, token, clan_id, name)  # raises BotBlock on edge block
        all_ok = all_ok and ok
        time.sleep(random.uniform(1.5, 3.0))

        seen = set(seen_map.get(clan_id, []))
        events, new_ids = fp.extract_trade_events(clan, seen)
        if ok:
            results.append(fp.analyze_clan(clan))
            raw_ok.append(clan)
            h = json.loads(results[-1]['holdings'])
            log(f"{name}: {h['total']} holdings, {len(h['common_memes'])} common, "
                f"majority: {', '.join(h['majority_memes']) or 'none'}, "
                f"{len(events)} new trade events")
        else:
            log(f"{name}: fetch FAILED — snapshot skipped, keeping last good data")

        if first_run:
            log(f"{name}: INIT — marked {len(new_ids)} existing feed events as seen (not stored)")
        else:
            all_events.extend(events)
            for ev in events:
                ev_clan[ev['eventId']] = clan_id
        # update seen: new ids appended in order; failed stores get un-marked below
        seen_map[clan_id] = list(seen) + new_ids

    if dry:
        log(f"DRY RUN — would store {len(all_events)} events, {len(results)} snapshots; no writes performed")
        return True

    if not results:
        log("ALL CLAN FETCHES FAILED — nothing stored")
        return False

    # ---- snapshots (storeClanData finds each clan's record by clanId) ----
    failed_ids = set()
    try:
        r = s.post(BACKEND_URL, json={'clans': results}, headers=kheaders(),
                   impersonate='chrome', timeout=60)
        rdata = r.json() if r.status_code == 200 else {}
        log(f"Snapshots: {r.status_code} {rdata.get('message', r.text[:60])}")
        for item in (rdata.get('results') or []):
            if item.get('action') == 'skipped_store_failed':
                all_ok = False
    except Exception as e:
        log(f"Snapshot store error: {e}")
        all_ok = False

    # ---- trade events (chunked) ----
    by_clan = {}
    for ev in all_events:
        by_clan.setdefault(ev['clanId'], []).append(ev)
    for clan_id, evs in by_clan.items():
        for i in range(0, len(evs), 30):
            chunk = evs[i:i + 30]
            ok_chunk = False
            for attempt in range(2):
                try:
                    r = s.post(TRADE_STORE_URL, json={'events': chunk}, headers=kheaders(),
                               impersonate='chrome', timeout=120)
                    rdata = r.json() if r.status_code == 200 else {}
                    log(f"Trade events {clan_id[:8]} chunk{i // 30 + 1} ({len(chunk)}): "
                        f"{r.status_code} stored={rdata.get('stored')} failed={len(rdata.get('failed') or [])}")
                    ok_chunk = r.status_code == 200
                    failed_ids.update(rdata.get('failed') or [])
                    break
                except Exception as e:
                    log(f"Trade store attempt {attempt + 1} error: {str(e)[:100]}")
            if not ok_chunk:
                failed_ids.update(ev['eventId'] for ev in chunk)
                all_ok = False

    # un-mark events the backend could not insert; prune to SEEN_CAP
    if failed_ids:
        for eid in failed_ids:
            cid = ev_clan.get(eid)
            if cid and eid in seen_map.get(cid, []):
                seen_map[cid].remove(eid)
    total = sum(len(v) for v in seen_map.values())
    if total > SEEN_CAP:
        excess = total - SEEN_CAP
        for k in list(seen_map):
            if excess <= 0:
                break
            drop = min(excess, len(seen_map[k]))
            seen_map[k] = seen_map[k][drop:]
            excess -= drop

    save_state('seen.json', seen_map)

    # ---- paper trading simulator (uses this cycle's raw data; no new fomo calls) ----
    try:
        run_paper_cycle(raw_ok, s, all_events)
    except Exception as e:
        log(f"Paper sim error (non-fatal): {e}")

    # ---- leaderboard + @rasmr watchlist (hourly, free: no fomo load beyond probes) ----
    try:
        fetch_leader_and_rasmr(s, token)
    except fp.BotBlock as b:
        pause = random.uniform(30, 45)
        log_gap_repo(f"leader fetch bot-block: {b}", pause)
        log(f"LEADER BOT-BLOCK: {b} — gap logged, retry next cycle")
    except Exception as e:
        log(f"Leader fetch error (non-fatal): {e}")

    return all_ok


# ---------- modes ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true',
                    help='fetch + parse only; no stores, no state changes')
    ap.add_argument('--loop', action='store_true', help='run forever (VPS/systemd mode)')
    ap.add_argument('--token-file', help='(testing) read token JSON from a file instead of AuthState')
    args = ap.parse_args()

    s = requests.Session(impersonate='chrome')

    def get_token():
        if args.token_file:
            blob = json.load(open(args.token_file))
            return blob['token']
        return get_valid_token(s)

    while True:
        try:
            token = get_token()
            ok = run_cycle(s, token, dry=args.dry_run)
            if not args.loop:
                sys.exit(0 if ok else 1)
        except fp.BotBlock as b:
            pause = random.uniform(30, 45)
            log_gap_repo(str(b), pause)  # repo state/gaps.json — visible to reports + heartbeat
            if args.loop:
                # Loop mode (VPS/systemd): actually back off, do NOT exit — a clean
                # exit would stop systemd from restarting the daemon.
                log(f"BOT-BLOCK: {b} — pausing {pause:.0f} min (gap logged)")
                time.sleep(pause * 60)
                continue
            log(f"BOT-BLOCK: {b} — would pause {pause:.0f} min (gap logged to state/gaps.json)")
            sys.exit(0)  # once mode: green run, next GitHub schedule retries
        except RuntimeError as e:
            log(f"FATAL: {e}")
            sys.exit(1)
        except Exception as e:
            log(f"Cycle exception: {e}")
            if not args.loop:
                sys.exit(1)
        if args.loop:
            time.sleep(300 + random.uniform(-45, 45))


if __name__ == '__main__':
    main()
