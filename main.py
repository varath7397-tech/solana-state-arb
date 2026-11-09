"""
=============================================================================================
SOLANA STATE-BASED ARBITRAGE ENGINE (HFT PRODUCTION RELEASE)
Pure Helius RPC (Zero 3rd-party APIs) | Raydium AMM V4 <-> Raydium CPMM
Features:
  • 4 Parallel WebSocket Workers with 8-byte dataSlice ({offset: 64, length: 8})
  • Slot-Consistency Filter (Prevents half-updated phantom spreads & 6005 slippage reverts)
  • Analytical Closed-Form Sizing Math: x* = (sqrt(KM) - M) / N
  • 25% Thinner-Pool Sizing Cap (Protects against AMM curve price-impact blowout)
  • Dynamic Tip Model (40% profit share clamped 10k-100k lamports)
  • Real Cost Threshold (C ≈ 0.000035 SOL)
  • Built-in Cloud Health Check Server on $PORT for Render Keep-Alive (/health)
=============================================================================================
"""
import asyncio
import base64
import json
import math
import os
import struct
import sys
import time
from datetime import datetime
import aiohttp
from aiohttp import web
import websockets

# Force UTF-8 encoding on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

# Configuration & Secrets
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "6e01a6b4-7dfb-409d-b314-7bdd99f1c03d").strip()
HTTP_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_WSS = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

PORT = int(os.environ.get("PORT", 10000))
LAMPORTS = 1_000_000_000
MIN_PROFIT_LAMPORTS = int(0.0003 * LAMPORTS)  # 0.0003 SOL minimum net profit
NUM_WORKERS = 4

# Core Mathematical AMM Optimizer (Closed-Form Analytical Solution)
def calc_optimal(buy, sell):
    g1 = 1.0 - buy['fee']
    g2 = 1.0 - sell['fee']
    
    K = sell['sol'] * g2 * buy['tok'] * g1
    M = sell['tok'] * buy['sol']
    N = g1 * (sell['tok'] + g2 * buy['tok'])
    
    KM = K * M
    if KM <= 0 or N <= 0:
        return None
    
    sqrt_KM = math.isqrt(int(KM))
    if sqrt_KM <= M:
        return None  # No profit after pool fees
    
    x = (sqrt_KM - M) / N
    if x <= 0:
        return None
    
    # Cap trade size at 25% of the thinner pool's depth to avoid curve price-impact blowout
    thinner_sol = min(buy['sol'], sell['sol'])
    x_cap = int(thinner_sol * 0.25)
    if x > x_cap:
        x = x_cap
        
    # Simulate swap outputs
    y = (g1 * x * buy['tok']) / (buy['sol'] + g1 * x)
    z = (g2 * y * sell['sol']) / (sell['tok'] + g2 * y)
    
    gross_profit = z - x
    if gross_profit <= 0:
        return None
        
    # Dynamic Tip: 40% of gross profit, clamped between 10k and 100k lamports
    dynamic_tip = max(10_000, min(int(gross_profit * 0.40), 100_000))
    # Real Cost: Base tx fee (5k) + 120k CU price (6k) + dynamic tip
    real_cost = 5_000 + 6_000 + dynamic_tip
    net_profit = gross_profit - real_cost
    
    return {
        'in_sol': x / LAMPORTS,
        'gross_profit_sol': gross_profit / LAMPORTS,
        'net_profit_sol': net_profit / LAMPORTS,
        'in_lamports': int(x),
        'gross_profit_lamports': int(gross_profit),
        'net_profit_lamports': int(net_profit),
        'tip_lamports': dynamic_tip,
        'is_viable': net_profit >= MIN_PROFIT_LAMPORTS
    }

def evaluate_pair(coin):
    t0 = time.perf_counter_ns()
    p1 = coin['v4']
    p2 = coin['cpmm']
    
    if p1['sol'] <= 0 or p1['tok'] <= 0 or p2['sol'] <= 0 or p2['tok'] <= 0:
        return None
    
    price1 = p1['sol'] / p1['tok']
    price2 = p2['sol'] / p2['tok']
    
    # Direction 1: Buy on V4 (cheaper), Sell on CPMM (expensive)
    res1 = calc_optimal(p1, p2) if price1 < price2 else None
    # Direction 2: Buy on CPMM (cheaper), Sell on V4 (expensive)
    res2 = calc_optimal(p2, p1) if price2 < price1 else None
    
    best = res1 or res2
    direction = "V4 -> CPMM" if res1 else "CPMM -> V4"
    
    # Harmonic Liquidity Depth L
    a1 = p1['sol'] / LAMPORTS
    a2 = p2['sol'] / LAMPORTS
    harmonic_depth = (a1 * a2) / (a1 + a2) if (a1 + a2) > 0 else 0
    
    # Theoretical required spread to beat real cost C (Base + CU + Tip = ~0.000035 SOL)
    cost_sol = 0.000035
    min_net_spread = 2.0 * math.sqrt(cost_sol / harmonic_depth) * 100.0 if harmonic_depth > 0 else 999.0
    min_gross_spread = min_net_spread + (p1['fee'] + p2['fee']) * 100.0 # Dynamic real pool fees
    
    spread_pct = abs(price1 - price2) / min(price1, price2) * 100.0
    elapsed_us = (time.perf_counter_ns() - t0) / 1000.0
    
    return {
        'token': coin['token'],
        'spread_pct': spread_pct,
        'harmonic_depth': harmonic_depth,
        'min_gross_spread': min_gross_spread,
        'direction': direction,
        'best': best,
        'elapsed_us': elapsed_us,
        'v4_sol': a1,
        'cpmm_sol': a2
    }

async def fetch_reserves_chunk(session, vault_keys):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [
            vault_keys,
            {"encoding": "base64"}
        ]
    }
    async with session.post(HTTP_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()
        results = {}
        for k, acc in zip(vault_keys, data.get("result", {}).get("value", [])):
            if acc and "data" in acc:
                raw = base64.b64decode(acc["data"][0])
                if len(raw) >= 72:
                    amt = struct.unpack_from("<Q", raw, 64)[0]
                    results[k] = amt
        return results

async def fetch_all_reserves(session, all_keys):
    results = {}
    for i in range(0, len(all_keys), 100):
        chunk = all_keys[i:i+100]
        res = await fetch_reserves_chunk(session, chunk)
        results.update(res)
    return results

# Shared Global State
shared_state = {
    'coins': [],
    'vault_map': {},
    'total_ws_events': 0,
    'total_recalcs': 0,
    'viable_events': [],
    'max_spread_seen': {},
    'running': True
}

def log_viable_opportunity(ev):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry = {
        'timestamp': ts,
        'token': ev['token'],
        'direction': ev['direction'],
        'spread_pct': ev['spread_pct'],
        'min_gross_spread': ev['min_gross_spread'],
        'harmonic_depth_sol': ev['harmonic_depth'],
        'optimal_in_sol': ev['best']['in_sol'],
        'gross_profit_sol': ev['best']['gross_profit_sol'],
        'net_profit_sol': ev['best']['net_profit_sol'],
        'tip_lamports': ev['best']['tip_lamports']
    }
    shared_state['viable_events'].append(entry)
    with open("arb_events.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
        
    print(f"\n" + "🔥" * 40)
    print(f"🎯 [REAL-TIME ARBITRAGE OPPORTUNITY FIRED @ {ts}]")
    print(f"   Token:             {ev['token']}")
    print(f"   Direction:         {ev['direction']}")
    print(f"   Spread:            {ev['spread_pct']:.4f}% (Required: {ev['min_gross_spread']:.2f}%)")
    print(f"   Harmonic Depth L:  {ev['harmonic_depth']:.2f} SOL")
    print(f"   Optimal Trade In:  {ev['best']['in_sol']:.4f} SOL")
    print(f"   Est Gross Profit:  +{ev['best']['gross_profit_sol']:.6f} SOL")
    print(f"   Est Net Profit:    +{ev['best']['net_profit_sol']:.6f} SOL (After All Fees!)")
    print("🔥" * 40 + "\n")

async def ws_worker(worker_id, vault_subset):
    print(f"[Worker #{worker_id}] Starting listener for {len(vault_subset)} vaults...")
    while shared_state['running']:
        try:
            async with websockets.connect(HELIUS_WSS, ping_interval=20, ping_timeout=20) as ws:
                sub_id_to_vault = {}
                req_id_to_vault = {}
                # Non-blocking pipelined subscription dispatch with 8-byte dataSlice
                for i, v_k in enumerate(vault_subset, start=1):
                    req_id_to_vault[i] = v_k
                    req = {
                        "jsonrpc": "2.0",
                        "id": i,
                        "method": "accountSubscribe",
                        "params": [
                            v_k,
                            {
                                "encoding": "base64",
                                "commitment": "processed",
                                "dataSlice": {"offset": 64, "length": 8}
                            }
                        ]
                    }
                    await ws.send(json.dumps(req))
                    
                print(f"[Worker #{worker_id}] Pipelined {len(vault_subset)} subscription requests (8-byte dataSlice). Active ✅")
                
                while shared_state['running']:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        data = json.loads(msg_raw)
                        
                        # Handle subscription response
                        if "id" in data and data["id"] in req_id_to_vault:
                            sub_id = data.get("result")
                            if sub_id is not None:
                                sub_id_to_vault[sub_id] = req_id_to_vault[data["id"]]
                                
                        # Handle live account notification
                        elif "method" in data and data["method"] == "accountNotification":
                            shared_state['total_ws_events'] += 1
                            res_val = data["params"]["result"]
                            slot = res_val.get("context", {}).get("slot", 0)
                            sub_id = data["params"]["subscription"]
                            v_key = sub_id_to_vault.get(sub_id)
                            if not v_key:
                                continue
                            
                            c_idx, p_kind, val_kind = shared_state['vault_map'][v_key]
                            acc_info = res_val["value"]
                            raw_bytes = base64.b64decode(acc_info["data"][0])
                            
                            # Fast 8-byte parse from dataSlice or fallback to full 72-byte
                            if len(raw_bytes) == 8:
                                amt = int.from_bytes(raw_bytes, "little")
                            elif len(raw_bytes) >= 72:
                                amt = struct.unpack_from("<Q", raw_bytes, 64)[0]
                            else:
                                continue
                                
                            pool_obj = shared_state['coins'][c_idx][p_kind]
                            pool_obj[val_kind] = amt
                            pool_obj[f'slot_{val_kind}'] = slot

                            # SLOT-CONSISTENCY FILTER:
                            # Both vaults of the updated pool MUST be in the exact same slot!
                            # If they differ, the pool is mid-update across two separate WebSocket messages.
                            if pool_obj.get('slot_sol', 0) != pool_obj.get('slot_tok', 0):
                                continue  # Wait for counterpart vault in the same slot!
                                
                            # Microsecond recalculation on fully slot-consistent reserves
                            shared_state['total_recalcs'] += 1
                            ev = evaluate_pair(shared_state['coins'][c_idx])
                            if ev:
                                tok = ev['token']
                                if ev['spread_pct'] > shared_state['max_spread_seen'].get(tok, 0):
                                    shared_state['max_spread_seen'][tok] = ev['spread_pct']
                                    
                                if ev['best'] and ev['best']['is_viable']:
                                    log_viable_opportunity(ev)
                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
            if shared_state['running']:
                print(f"[Worker #{worker_id}] Connection lost ({e}). Reconnecting in 3s...")
                await asyncio.sleep(3)

async def periodic_sync_and_heartbeat():
    async with aiohttp.ClientSession() as session:
        last_heartbeat = time.time()
        all_keys = list(shared_state['vault_map'].keys())
        
        while shared_state['running']:
            await asyncio.sleep(20)
            now = time.time()
            
            # Periodic Sync
            try:
                res = await fetch_all_reserves(session, all_keys)
                sync_slot = int(time.time())
                for v_k, (c_idx, p_kind, val_kind) in shared_state['vault_map'].items():
                    if v_k in res:
                        shared_state['coins'][c_idx][p_kind][val_kind] = res[v_k]
                        shared_state['coins'][c_idx][p_kind][f'slot_{val_kind}'] = sync_slot
                        
                for coin in shared_state['coins']:
                    shared_state['total_recalcs'] += 1
                    ev = evaluate_pair(coin)
                    if ev:
                        tok = ev['token']
                        if ev['spread_pct'] > shared_state['max_spread_seen'].get(tok, 0):
                            shared_state['max_spread_seen'][tok] = ev['spread_pct']
                        if ev['best'] and ev['best']['is_viable']:
                            log_viable_opportunity(ev)
            except Exception:
                pass
                
            # 30-Second Heartbeat & State Dump
            if now - last_heartbeat >= 30:
                last_heartbeat = now
                viable_count = len(shared_state['viable_events'])
                print(f"[HEARTBEAT] WS Events: {shared_state['total_ws_events']} | Recalcs: {shared_state['total_recalcs']} | Opportunities: {viable_count} | Monitored: {len(shared_state['coins'])} Pools")
                
                # Write status file
                status_dump = {
                    'timestamp': datetime.now().isoformat(),
                    'monitored_pools': len(shared_state['coins']),
                    'total_vaults': len(all_keys),
                    'total_ws_events': shared_state['total_ws_events'],
                    'total_recalcs': shared_state['total_recalcs'],
                    'viable_count': viable_count,
                    'top_spreads': sorted(
                        [{'token': k, 'spread': v} for k, v in shared_state['max_spread_seen'].items()],
                        key=lambda x: x['spread'], reverse=True
                    )[:10]
                }
                with open("engine_status.json", "w", encoding="utf-8") as sf:
                    json.dump(status_dump, sf, indent=2)

# Cloud Health Check Server (Render Keep-Alive)
async def health_check(request):
    return web.json_response({
        "status": "healthy",
        "service": "solana-state-arb",
        "timestamp": datetime.now().isoformat(),
        "monitored_pools": len(shared_state['coins']),
        "ws_events": shared_state['total_ws_events'],
        "recalcs": shared_state['total_recalcs'],
        "viable_opportunities": len(shared_state['viable_events']),
        "active_workers": NUM_WORKERS
    })

async def run_http_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Cloud Health Check Server active on 0.0.0.0:{PORT} (/health) ✅")

async def main(run_seconds=None):
    print("=" * 90)
    print("🚀 INITIALIZING 55-POOL REAL-TIME STATE ARBITRAGE SERVICE")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔗 RPC HTTP:  {HTTP_URL[:38]}...")
    print(f"⚡ RPC WSS:   {HELIUS_WSS[:38]}...")
    print("=" * 90)
    
    pools_path = os.path.join(os.path.dirname(__file__), "pools.json")
    with open(pools_path, "r", encoding="utf-8") as f:
        all_coins = json.load(f)
        
    print(f"📦 Loaded {len(all_coins)} verified liquid dual pools.")
    
    coins_state = []
    vault_map = {}
    
    for i, c in enumerate(all_coins):
        v4_fee = c['v4'].get('fee_rate', 0.0025)
        cpmm_fee = c['cpmm'].get('fee_rate', 0.0025)
        st = {
            'token': c['token'],
            'v4': {
                'sol': int(c['v4']['sol_depth'] * LAMPORTS),
                'tok': 0, 'fee': v4_fee,
                'sol_vault': c['v4']['sol_vault'],
                'tok_vault': c['v4']['tok_vault'],
                'slot_sol': 1, 'slot_tok': 1
            },
            'cpmm': {
                'sol': int(c['cpmm']['sol_depth'] * LAMPORTS),
                'tok': 0, 'fee': cpmm_fee,
                'sol_vault': c['cpmm']['sol_vault'],
                'tok_vault': c['cpmm']['tok_vault'],
                'slot_sol': 1, 'slot_tok': 1
            }
        }
        coins_state.append(st)
        vault_map[c['v4']['sol_vault']] = (i, 'v4', 'sol')
        vault_map[c['v4']['tok_vault']] = (i, 'v4', 'tok')
        vault_map[c['cpmm']['sol_vault']] = (i, 'cpmm', 'sol')
        vault_map[c['cpmm']['tok_vault']] = (i, 'cpmm', 'tok')
        
    shared_state['coins'] = coins_state
    shared_state['vault_map'] = vault_map
    all_vault_keys = list(vault_map.keys())
    print(f"🔑 Total Unique Vault Accounts: {len(all_vault_keys)}")
    
    # Initial Batch Load
    async with aiohttp.ClientSession() as session:
        print("\n📥 Fetching Real Initial Reserves for all 220 Vaults via batch RPC...")
        res = await fetch_all_reserves(session, all_vault_keys)
        for v_k, (c_idx, p_kind, val_kind) in vault_map.items():
            if v_k in res:
                coins_state[c_idx][p_kind][val_kind] = res[v_k]
                
    # Baseline Spread Evaluation
    above_req = 0
    print("\n[BASELINE SPREAD SNAPSHOT - TOP SPREAD CANDIDATES]")
    for st in coins_state:
        ev = evaluate_pair(st)
        if ev:
            shared_state['max_spread_seen'][ev['token']] = ev['spread_pct']
            if ev['spread_pct'] >= ev['min_gross_spread']:
                above_req += 1
                print(f"  🔥 CANDIDATE: {ev['token'][:10]}.. | L: {ev['harmonic_depth']:>5.2f} SOL | Spread: {ev['spread_pct']:>6.3f}% | Req: {ev['min_gross_spread']:>5.2f}%")
                
    print(f"\n✅ Initial Snapshot Complete. Active Candidates near/above threshold: {above_req} / {len(coins_state)}")
    
    # Split vaults across 4 parallel workers
    chunk_size = math.ceil(len(all_vault_keys) / NUM_WORKERS)
    worker_subsets = [all_vault_keys[i:i + chunk_size] for i in range(0, len(all_vault_keys), chunk_size)]
    
    print("\n" + "=" * 90)
    print(f"⚡ LAUNCHING {NUM_WORKERS} PARALLEL WEBSOCKET WORKERS + CLOUD HEALTH SERVER")
    print("=" * 90)
    
    tasks = []
    for w_id, subset in enumerate(worker_subsets):
        tasks.append(asyncio.create_task(ws_worker(w_id, subset)))
        
    tasks.append(asyncio.create_task(periodic_sync_and_heartbeat()))
    tasks.append(asyncio.create_task(run_http_server()))
    
    if run_seconds:
        print(f"⏱️ Running for {run_seconds} seconds before graceful exit...")
        await asyncio.sleep(run_seconds)
        shared_state['running'] = False
        print("\n🛑 Shutting down workers...")
        for t in tasks:
            t.cancel()
    else:
        # Run indefinitely (Production 24/7 Mode)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else None
    asyncio.run(main(duration))
