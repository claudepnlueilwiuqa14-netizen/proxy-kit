#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""proxykit —— 免费代理池：多源采集 + 并发验活 + 分类取用
源: proxifly/free-proxy-list (jsdelivr CDN) + TheSpeedX/PROXY-List (raw)
用法:
  python proxykit.py refresh          # 拉全量 + 验活 + 存 sqlite
  python proxykit.py pick [http|socks4|socks5] [国家码]  # 取一个可用代理
  python proxykit.py stats            # 池子统计
  python proxykit.py export [n]       # 导出可用列表
"""
import concurrent.futures, json, os, sqlite3, sys, time, urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "proxykit.db")
PROXY = "http://127.0.0.1:7897"   # 采集走 Clash；验活不走代理（直连目标）

SOURCES = {
    "proxifly": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json",
    "speedx_http": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "speedx_socks4": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "speedx_socks5": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
}

def _opener(proxy=None):
    if proxy:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))

def _get(url, proxy=PROXY, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return _opener(proxy).open(req, timeout=timeout).read()

def init_db():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS proxies(
        addr TEXT PRIMARY KEY, proto TEXT, country TEXT, anonymity TEXT,
        google INT, https INT, latency_ms INT, checked_at INT)""")
    c.commit()
    return c

def collect():
    """多源拉取，返回 [{addr, proto, country, google, https}]"""
    rows = {}
    # proxifly JSON
    try:
        d = json.loads(_get(SOURCES["proxifly"]))
        for p in d.get("data", []):
            key = "%s://%s:%s" % (p["protocol"], p["ip"], p["port"])
            rows[key] = {"addr": key, "proto": p["protocol"],
                         "country": (p.get("geolocation") or {}).get("country"),
                         "google": 0, "https": 0}
        print("[i] proxifly:", len(d.get("data", [])))
    except Exception as e:
        print("[warn] proxifly:", e)
    # TheSpeedX txt
    for name, proto in (("speedx_http", "http"), ("speedx_socks4", "socks4"),
                        ("speedx_socks5", "socks5")):
        try:
            txt = _get(SOURCES[name]).decode("utf-8", "ignore")
            n = 0
            for line in txt.splitlines():
                line = line.strip()
                if not line or ":" not in line:
                    continue
                key = "%s://%s" % (proto, line)
                if key not in rows:
                    rows[key] = {"addr": key, "proto": proto,
                                 "country": None, "google": 0, "https": 0}
                    n += 1
            print("[i]", name, n)
        except Exception as e:
            print("[warn]", name, e)
    return list(rows.values())

def check_one(addr, timeout=8):
    """验活：用该代理访问国内可达目标（httpbin 国内直连不通，改用 baidu echo）
    只验证代理能否转发 HTTP，不要求出口位置"""
    t0 = time.time()
    try:
        op = _opener(addr)
        req = urllib.request.Request("http://www.baidu.com/",
                                     headers={"User-Agent": "Mozilla/5.0"})
        with op.open(req, timeout=timeout) as r:
            body = r.read(512)
        if b"baidu" not in body.lower() and len(body) < 100:
            return None
        return int((time.time() - t0) * 1000)
    except Exception:
        return None

def refresh(check_limit=400, workers=60):
    rows = collect()
    print("[i] 合计 %d 个候选，验活前 %d 个（%d 线程）" %
          (len(rows), min(check_limit, len(rows)), workers))
    sample = rows[:check_limit]
    alive = []
    with concurrent.futures.ThreadPoolExecutor(workers) as ex:
        futs = {ex.submit(check_one, r["addr"]): r for r in sample}
        done = 0
        for f in concurrent.futures.as_completed(futs):
            r = futs[f]
            lat = f.result()
            done += 1
            if done % 50 == 0:
                print("  ... %d/%d 已验，存活 %d" % (done, len(sample), len(alive)))
            if lat is not None:
                r2 = dict(r)
                r2["latency_ms"] = lat
                alive.append(r2)
    c = init_db()
    c.execute("DELETE FROM proxies")
    for r in alive:
        c.execute("INSERT OR REPLACE INTO proxies VALUES(?,?,?,?,?,?,?,?)",
                  (r["addr"], r["proto"], r["country"], None, 0, 0,
                   r["latency_ms"], int(time.time())))
    c.commit(); c.close()
    print("[✓] 存活 %d / 验了 %d，已入库" % (len(alive), len(sample)))
    return alive

def pick(proto=None, country=None):
    c = init_db()
    q, a = "SELECT addr, proto, country, latency_ms FROM proxies WHERE 1=1", []
    if proto:
        q += " AND proto=?"; a.append(proto)
    if country:
        q += " AND country LIKE ?"; a.append("%" + country + "%")
    q += " ORDER BY latency_ms ASC LIMIT 1"
    row = c.execute(q, a).fetchone()
    c.close()
    return {"addr": row[0], "proto": row[1], "country": row[2],
            "latency_ms": row[3]} if row else None

def stats():
    c = init_db()
    total = c.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
    by_proto = c.execute("SELECT proto, COUNT(*) FROM proxies GROUP BY proto").fetchall()
    by_ctry = c.execute("SELECT country, COUNT(*) FROM proxies GROUP BY country ORDER BY 2 DESC LIMIT 10").fetchall()
    c.close()
    return {"total": total, "by_proto": by_proto, "top_countries": by_ctry}

if __name__ == "__main__":
    a = sys.argv[1:]
    cmd = a[0] if a else "stats"
    if cmd == "refresh":
        limit = int(a[1]) if len(a) > 1 else 400
        refresh(limit)
    elif cmd == "pick":
        print(json.dumps(pick(a[1] if len(a) > 1 else None,
                              a[2] if len(a) > 2 else None), ensure_ascii=False))
    elif cmd == "stats":
        print(json.dumps(stats(), ensure_ascii=False, indent=1))
    else:
        print(__doc__)
