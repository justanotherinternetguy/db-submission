#!/usr/bin/env python3
"""Slow, independent reference for rules.md, for cross-checking on other seeds.

Usage: reference.py DATA_DIR OUT_DIR   (writes OUT_DIR/user_daily.csv and campaign_daily.csv)
"""
import calendar
import csv as _csv
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
DIGITS = set(b"0123456789")


def split(line, n):
    fields = line.split(b",")
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def lines(path):
    with open(path, "rb") as fh:
        data = fh.read()
    parts = data.split(b"\n")
    if data.endswith(b"\n"):
        parts.pop()
    return parts[1:]


def isdig(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def parse_ts(s):
    if isdig(s):
        v = int(s)
        return v if v < 2**63 else None
    if len(s) not in (20, 25):
        return None
    try:
        t = s.decode("ascii")
    except UnicodeDecodeError:
        return None
    if t[4] != "-" or t[7] != "-" or t[10] != "T" or t[13] != ":" or t[16] != ":":
        return None
    parts = [t[0:4], t[5:7], t[8:10], t[11:13], t[14:16], t[17:19]]
    if not all(isdig(p.encode()) for p in parts):
        return None
    y, mo, d, h, mi, se = map(int, parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]) if y > 0 else d < 1:
        return None
    if len(t) == 20:
        if t[19] != "Z":
            return None
        off = 0
    else:
        if t[19] not in "+-" or t[22] != ":" or not isdig(t[20:22].encode()) or not isdig(t[23:25].encode()):
            return None
        oh, om = int(t[20:22]), int(t[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * (1 if t[19] == "+" else -1)
    days = (calendar.timegm((y, mo, d, 0, 0, 0)) // 86400)
    return ((days * 86400 + h * 3600 + mi * 60 + se) - off * 60) * 1000


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())

    cat_of = {}
    for line in lines(os.path.join(data, "products.csv")):
        f = split(line, 2)
        if f:
            cat_of[f[0]] = f[1]
    channel = {}
    for line in lines(os.path.join(data, "campaigns.csv")):
        f = split(line, 2)
        if f:
            channel[f[0]] = f[1]

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for p in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in lines(p):
            f = split(line, 2)
            if not f or not f[0] or not f[1]:
                continue
            a, b = find(f[0]), find(f[1])
            if a != b:
                parent[a] = b
    name = {}
    for x in list(parent):
        r = find(x)
        if r not in name or x < name[r]:
            name[r] = x

    def person(u):
        return name[find(u)] if u in parent else u

    best = {}
    for p in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in lines(p):
            f = split(line, 7)
            if not f or not f[0]:
                continue
            ing = parse_ts(f[3])
            cur = best.get(f[0])
            if cur is None or ing < cur[3]:
                best[f[0]] = (f[1], f[2], f[0], ing, f[4], f[5], f[6])

    by_person = defaultdict(list)
    for uid, tsr, _, ing, typ, prod, price in best.values():
        if not uid:
            continue
        t = parse_ts(tsr)
        if t is None or not (ws <= t < we):
            continue
        if typ not in (b"view", b"cart", b"purchase") or not prod:
            continue
        pr = 0
        if typ == b"purchase":
            if not isdig(price) or not (1 <= int(price) <= 10_000_000):
                continue
            pr = int(price)
        if ing - t > DAY:
            continue
        by_person[person(uid)].append((t, pr, cat_of.get(prod, b"unknown")))

    clicks_seen = set()
    clicks = defaultdict(list)
    for p in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in lines(p):
            f = split(line, 4)
            if not f or not f[0] or f[0] in clicks_seen:
                continue
            clicks_seen.add(f[0])
            cid, uid, camp, tsr = f
            if not uid or camp not in channel:
                continue
            t = parse_ts(tsr)
            if t is None or not (ws - WEEK <= t < we):
                continue
            clicks[person(uid)].append((t, cid, camp))

    user_rows = {}
    camp_rows = defaultdict(lambda: [0, 0, 0, 0])
    for pid, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = clicks.get(pid, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            rev = sum(e[1] for e in s)
            purch = any(e[1] > 0 for e in s)
            row = user_rows.setdefault((pid, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(s)
            row[2] += rev
            for e in s:
                if e[1] > 0:
                    row[3][e[2]] += e[1]
            cand = [c for c in cl if start - WEEK <= c[0] <= start]
            if cand:
                win = min(cand, key=lambda c: (-c[0], c[1]))
                camp = win[2]
                for cp in {c[2] for c in cand}:
                    camp_rows[(cp, day)][3] += 1
            else:
                camp = b"organic"
            r = camp_rows[(camp, day)]
            r[0] += 1
            r[1] += purch
            r[2] += rev

    import datetime

    def dstr(day):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="", encoding="utf-8", errors="surrogateescape") as fh:
        w = _csv.writer(fh, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (pid, day), (s, e, r, cats) in user_rows.items():
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            w.writerow([pid.decode(), dstr(day), s, e, r, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="", encoding="utf-8") as fh:
        w = _csv.writer(fh, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (camp, day), (s, ps, r, a) in camp_rows.items():
            ch = b"organic" if camp == b"organic" else channel[camp]
            w.writerow([camp.decode(), ch.decode(), dstr(day), s, ps, r, a])


if __name__ == "__main__":
    main()
