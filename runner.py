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
  * No lockfile needed: the workflow serializes runs; --loop runs one process.

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
SEEN_CAP = fp.MAX_SEEN

KEY = os.environ.get('FOMO_PIPELINE_KEY', fp.PIPELINE_KEY)
if not KEY:
    print("FATAL: FOMO_PIPELINE_KEY not set", flush=True)
    sys.exit(1)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def kheaders():
    return {'Content-Type': 'application/json', 'X-Pipeline-Key': KEY}


# ---------- state ----------

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


# ---------- one cycle ----------

def run_cycle(s, token, dry=False):
    log("=== Fetch cycle start ===")
    seen_map = load_state('seen.json', {})     # {clanId: [eventId, ...] insertion order}
    # First run ever: mark the whole ~4-day feed window as already-seen WITHOUT
    # storing (it is already in the database) so we never create duplicates.
    first_run = not os.path.exists(os.path.join(STATE_DIR, 'seen.json'))

    results, all_events = [], []
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
            if args.dry_run:
                sys.exit(0 if ok else 1)
            if not ok and not args.loop:
                sys.exit(1)
        except fp.BotBlock as b:
            pause = random.uniform(30, 45)
            log(f"BOT-BLOCK: {b} — logging gap and backing off {pause:.0f} min")
            try:
                gaps = load_state('gaps.json', [])
                gaps.append({'at': datetime.now(timezone.utc).isoformat(),
                             'reason': str(b), 'pause_min': round(pause, 1)})
                save_state('gaps.json', gaps[-200:])
            except Exception:
                pass
            if not args.loop:
                sys.exit(0)  # expected transient; next scheduled run retries
            time.sleep(pause * 60)
            continue
        except RuntimeError as e:
            log(f"FATAL: {e}")
            if not args.loop:
                sys.exit(1)
        except Exception as e:
            log(f"FATAL: unexpected error: {e}")
            if not args.loop:
                sys.exit(1)
        if not args.loop:
            sys.exit(0 if ok else 1)
        time.sleep(random.uniform(270, 360))  # 4.5-6 min between cycles


if __name__ == '__main__':
    main()
