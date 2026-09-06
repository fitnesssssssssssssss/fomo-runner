#!/usr/bin/env python3
"""
Fomo.family autonomous clan fetch pipeline.
- Auto-refreshes the Privy token when close to expiry (server-side, curl_cffi)
- Fetches holdings, feed, and info for the top 5 clans
- Computes common/majority memes per clan
- Upserts snapshots to the ClanData entity via the storeClanData backend function
- Accumulates individual member trade events (buys/sells/theses) to the TradeEvent
  entity via the storeTradeEvents backend function, with local dedupe by event id
"""

from curl_cffi import requests
import json
import random
import os
import sys
import time
import base64
from urllib.parse import urlencode
from datetime import datetime, timezone

TOKEN_FILE = '/app/.agents/fomo_session/tokens.json'
SEEN_FILE = '/app/.agents/fomo_session/seen_events.json'
RECORDS_FILE = '/app/.agents/fomo_session/clan_records.json'
GAPS_FILE = '/app/.agents/fomo_session/gaps.json'
LOCK_FILE = '/app/.agents/fomo_session/pipeline.lock'
LOCK_MAX_AGE = 900  # seconds; a normal cycle takes ~2 min, so >15min means a stale lock

class BotBlock(Exception):
    """Raised when Cloudflare signals a block/challenge — must back off, not retry."""

BLOCK_STATUSES = {403, 429, 430}
CHALLENGE_MARKERS = ('just a moment', 'challenge-platform', 'cf-chl', 'turnstile', 'attention required')

def check_block(r):
    if r.status_code in BLOCK_STATUSES:
        raise BotBlock(f"HTTP {r.status_code}")
    body = r.text[:2000].lower()
    if any(m in body for m in CHALLENGE_MARKERS):
        raise BotBlock(f"challenge page (HTTP {r.status_code})")

def log_gap(reason, pause_min):
    """Record a backoff window so the daily report can show data gaps."""
    try:
        gaps = json.load(open(GAPS_FILE))
    except Exception:
        gaps = []
    gaps.append({'at': datetime.now(timezone.utc).isoformat(),
                 'reason': reason, 'pause_min': round(pause_min, 1)})
    try:
        json.dump(gaps[-200:], open(GAPS_FILE, 'w'))
    except Exception as e:
        print(f"WARN: cannot persist gaps log: {e}", flush=True)
BACKEND_URL = 'https://elara-0ec48c47.base44.app/functions/storeClanData'
def _pipeline_key():
    k = os.environ.get('FOMO_PIPELINE_KEY')
    if k:
        return k.strip()
    try:
        return open('/app/.agents/fomo_session/pipeline_key.txt').read().strip()
    except Exception:
        return ''
PIPELINE_KEY = _pipeline_key()
TRADE_STORE_URL = 'https://elara-0ec48c47.base44.app/functions/storeTradeEvents'

CLANS = {
    'Fantom Troupe': '715a68a6-657e-4424-8c65-68ad3af899b1',
    'Nobi Ventures': '11e5d07c-6f1d-4aa7-8a0d-aacf1c5f2033',
    'Dabal': '75cefc60-e6f6-4189-8073-18e7cb37db5d',
    'Sparsity': '87c22801-d5a6-4e9e-8f18-bceadf8b1040',
    'BOOGLE': 'ec2aef5b-7370-4a5e-87c9-3053dba02458',
}

FEED_TYPES = ["multi_user_buy", "multi_user_sell", "large_buy", "large_sell", "thesis_created", "new_token_listing"]

PRIVY_APP_ID = 'cm6h485o300n3zj9yl6vpedq7'
PRIVY_CLIENT_ID = 'client-WY5gFSayQjxnQhG4rP6SnwPAyPZWZpNRhJ6b9rzMnYwqH'

MAX_SEEN = 20000


def load_tokens():
    with open(TOKEN_FILE) as f:
        return json.load(f)


def save_tokens(tokens):
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(tokens, f, indent=2)
    except Exception as e:
        print(f"WARN: cannot persist token state (isolated/read-only env?): {e}", flush=True)


def load_seen():
    try:
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_seen(seen):
    if len(seen) > MAX_SEEN:
        seen = set(list(seen)[-MAX_SEEN:])
    try:
        with open(SEEN_FILE, 'w') as f:
            json.dump(sorted(seen), f)
    except Exception as e:
        print(f"WARN: cannot persist seen-events state (isolated/read-only env?): {e}", flush=True)


def load_records():
    try:
        with open(RECORDS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_records(records):
    try:
        with open(RECORDS_FILE, 'w') as f:
            json.dump(records, f, indent=2)
    except Exception as e:
        print(f"WARN: cannot persist records state (isolated/read-only env?): {e}", flush=True)


def token_seconds_left(token):
    try:
        parts = token.split('.')
        payload = parts[1] + '=' * (4 - len(parts[1]) % 4)
        data = json.loads(base64.b64decode(payload))
        return data.get('exp', 0) - time.time()
    except Exception:
        return -1


def refresh_tokens(session, tokens):
    """Refresh the Privy access token server-side."""
    r = session.post(
        'https://auth.privy.io/api/v1/sessions',
        json={"refresh_token": tokens['refresh_token']},
        headers={
            'Authorization': f"Bearer {tokens['token']}",
            'Content-Type': 'application/json',
            'Origin': 'https://fomo.family',
            'privy-app-id': PRIVY_APP_ID,
            'privy-client-id': PRIVY_CLIENT_ID,
            'privy-client': 'react-auth:3.34.0',
        }
    )
    if r.status_code in BLOCK_STATUSES:
        raise BotBlock(f"refresh HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        new_token = data.get('token') or data.get('privy_access_token')
        new_refresh = data.get('refresh_token')
        if not new_token:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] REFRESH EMPTY: 200 but no token in response", flush=True)
            return False
        tokens['token'] = new_token
        if new_refresh:
            tokens['refresh_token'] = new_refresh
        save_tokens(tokens)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Token refreshed OK", flush=True)
        return True
    print(f"[{datetime.now().strftime('%H:%M:%S')}] REFRESH FAILED: {r.status_code} {r.text[:150]}", flush=True)
    return False


def get_valid_token(session):
    try:
        tokens = load_tokens()
    except Exception as e:
        print(f"Token file unreadable: {e}", flush=True)
        return None
    left = token_seconds_left(tokens['token'])
    if left < 300:  # less than 5 min left -> refresh
        if not refresh_tokens(session, tokens):
            if left > 0:
                return tokens['token']  # use old one, might still work
            return None
    return tokens['token']


def fetch_clan(session, token, clan_id, clan_name):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Origin': 'https://fomo.family',
        'Referer': 'https://fomo.family/',
    }
    clan = {'id': clan_id, 'name': clan_name}
    ok = True

    # Pacing helper: keep request bursts human-looking
    pace = lambda: time.sleep(random.uniform(1.0, 2.2))

    # Holdings (max useful limit = 100)
    try:
        r = session.get(f'https://prod-api.fomo.family/v2/clans/{clan_id}/holdings?limit=100', headers=headers)
        check_block(r)
        d = r.json()
        clan['holdings'] = d.get('responseObject', {}).get('holdings', []) if d.get('success') else []
    except BotBlock:
        raise
    except Exception as e:
        print(f"  {clan_name} holdings error: {e}", flush=True)
        clan['holdings'] = []
        ok = False
    pace()

    # Feed (max useful limit = 100; covers ~4 days of history per fetch)
    try:
        params = [('limit', '100')] + [('feedTypes', ft) for ft in FEED_TYPES]
        r = session.get(f'https://prod-api.fomo.family/v2/clans/{clan_id}/feed?' + urlencode(params), headers=headers)
        check_block(r)
        d = r.json()
        feed = d.get('responseObject', {}).get('feed', []) if d.get('success') else []
        clan['feed'] = feed if isinstance(feed, list) else []
    except BotBlock:
        raise
    except Exception as e:
        print(f"  {clan_name} feed error: {e}", flush=True)
        clan['feed'] = []
        ok = False
    pace()

    # Clan info
    try:
        r = session.get(f'https://prod-api.fomo.family/v2/clans/{clan_id}', headers=headers)
        check_block(r)
        d = r.json()
        clan['info'] = d.get('responseObject', {}) if d.get('success') else {}
    except BotBlock:
        raise
    except Exception as e:
        print(f"  {clan_name} info error: {e}", flush=True)
        clan['info'] = {}
        ok = False

    return clan, ok


def extract_trade_events(clan_data, seen):
    """Turn raw feed events into TradeEvent records, skipping already-seen ids."""
    events = []
    new_ids = []
    for f in clan_data.get('feed', []):
        eid = f.get('id')
        if not eid or eid in seen:
            continue
        b = f.get('body', {}) or {}
        events.append({
            'eventId': eid,
            'clanId': clan_data.get('id', ''),
            'clanName': clan_data.get('name', ''),
            'userId': f.get('userId', ''),
            'userHandle': b.get('userHandle', ''),
            'type': f.get('type', ''),
            'tokenAddress': f.get('tokenAddress', ''),
            'symbol': b.get('ticker', ''),
            'networkId': f.get('networkId', 0),
            'createdAt': f.get('createdAt', ''),
            'positionSizeUsd': b.get('currentSizeUsd', 0),
            'avgCost': b.get('avgCost', 0),
            'percentPnl': b.get('percentPnl', 0),
            'realizedPnlUsd': b.get('realizedPnlUsd', 0),
            'isFirstBuy': b.get('isFirstBuy', False),
            'marketCap': b.get('marketCap', 0) if isinstance(b.get('marketCap'), (int, float)) else 0,
            'rawBody': json.dumps(b)[:2500],
        })
        new_ids.append(eid)
    return events, new_ids


def analyze_clan(clan_data):
    info = clan_data.get('info', {})
    holdings = clan_data.get('holdings', [])
    feed = clan_data.get('feed', [])
    total_members = info.get('memberCount', 0)

    min_threshold = max(2, total_members * 0.3)
    widely_held = sorted(
        [h for h in holdings if (h.get('memberCount') or 0) >= min_threshold],
        key=lambda h: h.get('memberCount', 0), reverse=True
    )[:20]
    majority = [h.get('symbol') for h in holdings if (h.get('memberCount') or 0) > total_members / 2]

    theses = [f for f in feed if f.get('type') == 'thesis_created'][:15]
    buys = [f for f in feed if f.get('type') in ('large_buy', 'multi_user_buy')][:10]
    sells = [f for f in feed if f.get('type') in ('large_sell', 'multi_user_sell')][:10]

    return {
        'clanId': clan_data.get('id', ''),
        'name': clan_data.get('name', ''),
        'description': info.get('description', ''),
        'rank': info.get('rank', 0),
        'memberCount': total_members,
        'pnl': float(info.get('pnl', 0) or 0),
        'holdings': json.dumps({
            'total': len(holdings),
            'common_memes': [{
                'symbol': h.get('symbol', ''), 'name': h.get('name', ''),
                'tokenAddress': h.get('tokenAddress', ''), 'networkId': h.get('networkId', ''),
                'memberCount': h.get('memberCount', 0), 'value': h.get('value', 0),
                'pnl': h.get('pnl', 0), 'percentagePnl': h.get('percentagePnl', 0),
            } for h in widely_held],
            'majority_memes': majority,
        }),
        'feed': json.dumps({
            'recent_buys': [{'ticker': f.get('body', {}).get('ticker', ''), 'createdAt': f.get('createdAt', '')} for f in buys],
            'recent_sells': [{'ticker': f.get('body', {}).get('ticker', ''), 'createdAt': f.get('createdAt', '')} for f in sells],
            'recent_theses': [{'ticker': t.get('body', {}).get('ticker', ''), 'comment': (t.get('body', {}).get('comment', '') or '')[:200]} for t in theses],
        }),
        'theses': json.dumps([{
            'ticker': t.get('body', {}).get('ticker', ''), 'createdAt': t.get('createdAt', ''),
            'comment': (t.get('body', {}).get('comment', '') or '')[:200],
            'userHandle': t.get('body', {}).get('userHandle', ''),
            'realizedPnl': t.get('body', {}).get('realizedPnlUsd', 0),
            'unrealizedPnl': t.get('body', {}).get('unrealizedPnlUsd', 0),
        } for t in theses]),
        'topMembers': json.dumps(info.get('members', [])[:10]),
        'fetchedAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


def acquire_lock():
    try:
        if os.path.exists(LOCK_FILE):
            holder_alive = False
            try:
                pid = int(open(LOCK_FILE).read().strip())
                os.kill(pid, 0)  # raises ProcessLookupError if pid is gone
                holder_alive = True
            except (ValueError, ProcessLookupError):
                holder_alive = False
            except PermissionError:
                holder_alive = True  # exists, owned by another user
            age = time.time() - os.path.getmtime(LOCK_FILE)
            if holder_alive and age < LOCK_MAX_AGE:
                return False
            print(f"Taking over lock (holder_alive={holder_alive}, age={age:.0f}s)", flush=True)
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        print(f"WARN: lock not available ({e}) — proceeding", flush=True)
    return True


SESSION_DIR = '/app/.agents/fomo_session'


def state_writable():
    try:
        probe = os.path.join(SESSION_DIR, '.write_probe')
        with open(probe, 'w') as f:
            f.write('ok')
        os.remove(probe)
        return True
    except Exception:
        return False


def release_lock():
    try:
        os.remove(LOCK_FILE)
    except Exception:
        pass


def run_cycle():
    if not acquire_lock():
        print("Another fetch cycle is running — skipping (lock held)", flush=True)
        return None
    try:
        return _run_cycle_inner()
    finally:
        release_lock()


def _run_cycle_inner():
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{ts}] === Fetch cycle start ===", flush=True)
    s = requests.Session(impersonate='chrome')

    token = get_valid_token(s)
    if not token:
        print("FATAL: no valid token and refresh failed", flush=True)
        return False

    seen = load_seen()
    results = []
    all_events = []
    all_ok = True

    for name, clan_id in CLANS.items():
        clan, ok = fetch_clan(s, token, clan_id, name)
        all_ok = all_ok and ok
        time.sleep(random.uniform(1.5, 3.0))
        if ok:
            results.append(analyze_clan(clan))
        else:
            print(f"  {name}: fetch FAILED — snapshot skipped (keeping last good data)", flush=True)

        events, new_ids = extract_trade_events(clan, seen)
        all_events.extend(events)
        seen.update(new_ids)

        h = json.loads(results[-1]['holdings'])
        print(f"  {name}: {h['total']} holdings, {len(h['common_memes'])} common, "
              f"majority: {', '.join(h['majority_memes']) or 'none'}, "
              f"{len(events)} new trade events", flush=True)

    # Store clan snapshots (update-in-place via tracked recordIds)
    if not results:
        print("  ALL CLAN FETCHES FAILED — no snapshots stored, keeping last good data", flush=True)
        save_seen(seen)
        return False
    records = load_records()
    for res in results:
        rid = records.get(res['clanId'])
        if rid:
            res['recordId'] = rid
    try:
        r = requests.post(BACKEND_URL, json={'clans': results},
                          headers={'Content-Type': 'application/json', 'X-Pipeline-Key': PIPELINE_KEY}, impersonate='chrome', timeout=60)
        rdata = r.json() if r.status_code == 200 else {}
        print(f"  Snapshots: {r.status_code} {rdata.get('message', r.text[:60])}", flush=True)
        # Update tracked record ids (handles create fallback after failed update)
        name_to_clanid = {res['name']: res['clanId'] for res in results}
        for item in (rdata.get('results') or []):
            cid = name_to_clanid.get(item.get('clan'))
            if cid and item.get('id'):
                records[cid] = item['id']
        save_records(records)
    except Exception as e:
        print(f"  Snapshot store error: {e}", flush=True)
        all_ok = False

    # Store new trade events: chunked per clan (<=50 per call to stay under timeouts)
    failed_ids = set()
    by_clan = {}
    for ev in all_events:
        by_clan.setdefault(ev['clanId'], []).append(ev)
    for clan_id, evs in by_clan.items():
        for i in range(0, len(evs), 30):
            chunk = evs[i:i+30]
            ok_chunk = False
            for attempt in range(2):
                try:
                    r = requests.post(TRADE_STORE_URL, json={'events': chunk},
                                      headers={'Content-Type': 'application/json', 'X-Pipeline-Key': PIPELINE_KEY},
                                      impersonate='chrome', timeout=120)
                    rdata = r.json() if r.status_code == 200 else {}
                    print(f"  Trade events {clan_id[:8]} chunk{i//50} ({len(chunk)}): {r.status_code} stored={rdata.get('stored')} skipped={rdata.get('skipped')} failed={len(rdata.get('failed') or [])}", flush=True)
                    ok_chunk = r.status_code == 200
                    # un-mark events the backend could not insert (retry next cycle)
                    backend_failed = rdata.get('failed') or []
                    if backend_failed:
                        failed_ids.update(backend_failed)
                    break
                except Exception as e:
                    print(f"  Trade store attempt {attempt+1} error: {str(e)[:100]}", flush=True)
            if not ok_chunk:
                failed_ids.update(ev['eventId'] for ev in chunk)
                all_ok = False
    # Only persist seen-ids for events that stored OK; failed ones retry next cycle
    if failed_ids:
        save_seen(seen - failed_ids)
    else:
        save_seen(seen)

    return all_ok


if __name__ == '__main__':
    if not state_writable():
        print("State files not writable in this environment — refusing to run "
              "(running here would duplicate-store events; restart from the main agent sandbox instead)", flush=True)
        sys.exit(1)
    if '--once' in sys.argv:
        result = None
        try:
            result = run_cycle()
        except BotBlock as b:
            pause = random.uniform(30, 45)
            log_gap(f"once-mode: {b}", pause)
            print(f"BOT-BLOCK: {b} — would pause {pause:.0f} min (gap logged)", flush=True)
        sys.exit(0 if result else 1)
    # Loop mode: ~5-minute cycles with human-like jitter (anti-bot measure)
    while True:
        try:
            run_cycle()
        except BotBlock as b:
            pause = random.uniform(30, 45)
            log_gap(str(b), pause)
            print(f"BOT-BLOCK: {b} — pausing {pause:.0f} min (gap logged)", flush=True)
            time.sleep(pause * 60)
            continue
        except Exception as e:
            print(f"Cycle exception: {e}", flush=True)
        time.sleep(300 + random.uniform(-45, 45))
