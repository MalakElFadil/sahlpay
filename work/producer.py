#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
producer.py - Générateur de transactions SahlPay (projet Écosystème Big Data 2)

Simule les événements d'un portefeuille mobile : un événement JSON par changement
d'état d'une transaction (INITIATED, AUTHORIZED, COMPLETED, REJECTED, REVERSED).

Caractéristiques simulées volontairement (comme décrit dans le sujet) :
  - ~5 % d'événements en retard (jusqu'à 15 minutes, téléphone hors réseau) :
    event_time ancien par rapport au moment où l'événement arrive ;
  - ~2 % de doublons (même tx_id et même status) ;
  - scénarios de fraude injectés : un appareil partagé par de nombreux comptes,
    des transferts circulaires A -> B -> C -> A, des comptes collecteurs qui
    reçoivent de nombreux petits virements.

Modes :
  --mode merchants   écrit le référentiel merchants.json (300 commerçants, JSON Lines)
  --mode file        écrit un historique de ~N événements dans un fichier .jsonl
  --mode kafka       publie en continu dans le topic Kafka (--rate événements/s)
  --mode stdout      comme kafka mais affiche les événements (pour tester sans Kafka)

Exemples :
  python3 producer.py --mode merchants --out merchants.json
  python3 producer.py --mode file --n 50000 --out transactions_history.jsonl
  python3 producer.py --mode kafka --rate 20 --bootstrap kafka:9092

Les données sont reproductibles : même --seed => mêmes données.
"""
import argparse
import heapq
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

# ----------------------------------------------------------------------------
# Paramètres du modèle
# ----------------------------------------------------------------------------
LATE_P = 0.05          # proportion d'événements en retard
LATE_MAX_S = 900       # retard maximal : 15 minutes
DUP_P = 0.02           # proportion de doublons
AVG_EV_PER_TX = 2.8    # nombre moyen d'événements par transaction (pour le débit)
P2M_SHARE = 0.75       # part des paiements chez un commerçant (le reste : P2P)
N_WALLETS = 2000       # nombre de comptes "normaux"
REJECT_P = 0.07        # transactions rejetées
REVERSE_P = 0.01       # transactions annulées après succès

# ville : (latitude, longitude, poids)
CITIES = {
    "Casablanca": (33.5731, -7.5898, 25),
    "Rabat": (34.0209, -6.8416, 15),
    "Marrakech": (31.6295, -7.9811, 12),
    "Tanger": (35.7595, -5.8340, 10),
    "Fes": (34.0181, -5.0078, 9),
    "Agadir": (30.4278, -9.5981, 8),
    "Meknes": (33.8935, -5.5547, 6),
    "Oujda": (34.6867, -1.9114, 5),
    "Kenitra": (34.2610, -6.5802, 5),
    "Tetouan": (35.5785, -5.3684, 5),
}
CATEGORIES = {  # catégorie : poids
    "epicerie": 30, "restauration": 22, "e-commerce": 16,
    "pharmacie": 12, "carburant": 10, "telecom": 10,
}
NAMES = {
    "epicerie": ["Epicerie Al Baraka", "Superette Atlas", "Hanout Salam", "Marche Bab Doukkala",
                 "Superette Nour", "Epicerie Al Amal", "Mini Market Zahra"],
    "restauration": ["Cafe Andalous", "Restaurant Dar Salam", "Snack Sahara", "Pizzeria Roma",
                     "Tacos House", "Boulangerie Amal", "Cafe Panorama", "Restaurant Al Medina"],
    "e-commerce": ["Shop Maroc Online", "MarocMarket.ma", "TechStore.ma", "ModeShop.ma",
                   "LivrExpress.ma", "DealsMaroc.ma"],
    "pharmacie": ["Pharmacie Ibn Sina", "Pharmacie Al Amal", "Pharmacie Centrale",
                  "Pharmacie du Marche", "Pharmacie Les Oliviers"],
    "carburant": ["Station Atlas", "Station Oasis", "Station Sahara", "Station Horizon"],
    "telecom": ["Recharge Express", "Boutique Telecom Orbit", "Point Mobile Atlas",
                "Telecom Services Plus"],
}

# Scénarios de fraude (identifiants fixes pour pouvoir les retrouver en Partie 6)
SHARED_DEVICE = "D-ff0001"
SHARED_WALLETS = ["W-%05d" % (90001 + i) for i in range(12)]
CYCLES = [["W-91001", "W-91002", "W-91003"], ["W-91011", "W-91012", "W-91013"]]
COLLECTORS = {"W-92001": 40, "W-92002": 25}   # compte collecteur : nombre d'expéditeurs


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ----------------------------------------------------------------------------
# Référentiel commerçants
# ----------------------------------------------------------------------------
def gen_merchants(rng, n=300):
    cities = list(CITIES)
    cweights = [CITIES[c][2] for c in cities]
    cats = list(CATEGORIES)
    catweights = [CATEGORIES[c] for c in cats]
    seen = {}
    out = []
    for i in range(1, n + 1):
        city = rng.choices(cities, cweights)[0]
        cat = rng.choices(cats, catweights)[0]
        base = rng.choice(NAMES[cat])
        key = (base, city)
        seen[key] = seen.get(key, 0) + 1
        name = "%s %s" % (base, city) if seen[key] == 1 else "%s %s %d" % (base, city, seen[key])
        lat0, lon0, _ = CITIES[city]
        out.append({
            "merchant_id": "M-%03d" % i,
            "name": name,
            "city": city,
            "category": cat,
            "latitude": round(lat0 + rng.uniform(-0.05, 0.05), 5),
            "longitude": round(lon0 + rng.uniform(-0.05, 0.05), 5),
        })
    return out


def load_merchants(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ----------------------------------------------------------------------------
# Monde simulé (comptes, appareils, commerçants)
# ----------------------------------------------------------------------------
class World:
    def __init__(self, rng, merchants):
        self.rng = rng
        self.merchants = merchants
        # popularité des commerçants (quelques-uns très fréquentés)
        order = list(range(len(merchants)))
        rng.shuffle(order)
        self.weight = {}
        for rank, idx in enumerate(order):
            self.weight[merchants[idx]["merchant_id"]] = 1.0 / ((rank + 1) ** 0.6)
        self.by_city = {}
        for m in merchants:
            self.by_city.setdefault(m["city"], []).append(m)
        cities = list(CITIES)
        cweights = [CITIES[c][2] for c in cities]
        self.wallets = []
        for i in range(N_WALLETS):
            city = rng.choices(cities, cweights)[0]
            devices = ["D-%06x" % rng.getrandbits(24)]
            if rng.random() < 0.05:
                devices.append("D-%06x" % rng.getrandbits(24))
            self.wallets.append({"id": "W-%05d" % (10000 + i), "city": city, "devices": devices})
        self.fraud_devices = {}

    def pick_merchant(self, city):
        rng = self.rng
        pool = self.by_city.get(city) if rng.random() < 0.85 else None
        if not pool:
            pool = self.merchants
        return rng.choices(pool, [self.weight[m["merchant_id"]] for m in pool])[0]

    def fraud_device(self, wallet_id):
        if wallet_id not in self.fraud_devices:
            self.fraud_devices[wallet_id] = "D-%06x" % self.rng.getrandbits(24)
        return self.fraud_devices[wallet_id]


def amount_p2m(rng):
    return round(min(max(rng.lognormvariate(4.8, 1.2), 5.0), 9000.0), 2)


def amount_p2p(rng):
    return round(min(max(rng.lognormvariate(5.2, 1.0), 10.0), 6000.0), 2)


def normal_spec(w):
    rng = w.rng
    wal = rng.choice(w.wallets)
    device = rng.choice(wal["devices"])
    if rng.random() < P2M_SHARE:
        m = w.pick_merchant(wal["city"])
        channel = rng.choices(["QR", "APP", "USSD"], [70, 25, 5])[0]
        return {"type": "P2M", "payer": wal["id"], "payee": m["merchant_id"], "device": device,
                "city": m["city"], "channel": channel, "amount": amount_p2m(rng)}
    other = rng.choice(w.wallets)
    while other["id"] == wal["id"]:
        other = rng.choice(w.wallets)
    channel = rng.choices(["APP", "USSD", "QR"], [70, 20, 10])[0]
    return {"type": "P2P", "payer": wal["id"], "payee": other["id"], "device": device,
            "city": wal["city"], "channel": channel, "amount": amount_p2p(rng)}


# ----------------------------------------------------------------------------
# Scénarios de fraude : renvoie une liste de (t0, spec) et la vérité terrain
# ----------------------------------------------------------------------------
def fraud_plans(w, t_a, t_b):
    rng = w.rng
    span = max((t_b - t_a).total_seconds(), 60.0)
    plans = []

    def rt(frac_max=1.0):
        return t_a + timedelta(seconds=rng.uniform(0, span * frac_max))

    # 1) un appareil partagé par de nombreux comptes (paiements chez des commerçants)
    for wid in SHARED_WALLETS:
        for _ in range(rng.randint(3, 5)):
            city = rng.choice(list(CITIES))
            m = w.pick_merchant(city)
            plans.append((rt(), {"type": "P2M", "payer": wid, "payee": m["merchant_id"],
                                 "device": SHARED_DEVICE, "city": m["city"],
                                 "channel": rng.choice(["APP", "QR"]), "amount": amount_p2m(rng),
                                 "force_ok": True}))
    # 2) transferts circulaires A -> B -> C -> A (plusieurs tours)
    for ci, cyc in enumerate(CYCLES):
        base = 1500.0 if ci == 0 else 850.0
        start = rt(0.6)
        for rnd in range(4):
            t = start + timedelta(minutes=rnd * rng.uniform(20, 60))
            for k in range(len(cyc)):
                a, b = cyc[k], cyc[(k + 1) % len(cyc)]
                t = t + timedelta(seconds=rng.uniform(30, 120))
                plans.append((t, {"type": "P2P", "payer": a, "payee": b, "device": w.fraud_device(a),
                                  "city": rng.choice(list(CITIES)), "channel": "APP",
                                  "amount": round(base * rng.uniform(0.93, 1.07), 2),
                                  "force_ok": True}))
    # 3) comptes collecteurs : beaucoup de petits virements reçus
    for coll, n_senders in COLLECTORS.items():
        senders = rng.sample(w.wallets, n_senders)
        for s in senders:
            for _ in range(rng.randint(1, 2)):
                plans.append((rt(), {"type": "P2P", "payer": s["id"], "payee": coll,
                                     "device": rng.choice(s["devices"]), "city": s["city"],
                                     "channel": rng.choice(["APP", "USSD"]),
                                     "amount": round(rng.uniform(20, 100), 2), "force_ok": True}))
    return plans


def ground_truth():
    return {
        "description": "Scénarios de fraude injectés par producer.py (vérité terrain pour la Partie 6)",
        "shared_device": {"device_id": SHARED_DEVICE, "wallets": SHARED_WALLETS},
        "cycles": CYCLES,
        "collectors": [{"wallet": k, "n_senders_min": v} for k, v in COLLECTORS.items()],
    }


# ----------------------------------------------------------------------------
# Construction des événements d'une transaction
# ----------------------------------------------------------------------------
def build_events(rng, tx_id, t0, spec):
    fraud = bool(spec.get("force_ok"))
    r = rng.random()
    if fraud or r >= REJECT_P + REVERSE_P:
        path = [("INITIATED", 0.0)]
        t = rng.uniform(0.3, 2.0)
        path.append(("AUTHORIZED", t))
        t += rng.uniform(0.3, 3.0)
        path.append(("COMPLETED", t))
    elif r < REJECT_P:
        if rng.random() < 0.6:
            path = [("INITIATED", 0.0), ("REJECTED", rng.uniform(0.3, 2.0))]
        else:
            t = rng.uniform(0.3, 2.0)
            path = [("INITIATED", 0.0), ("AUTHORIZED", t), ("REJECTED", t + rng.uniform(0.3, 3.0))]
    else:
        t = rng.uniform(0.3, 2.0)
        t2 = t + rng.uniform(0.3, 3.0)
        path = [("INITIATED", 0.0), ("AUTHORIZED", t), ("COMPLETED", t2),
                ("REVERSED", t2 + rng.uniform(60, 3600))]
    out = []
    for status, off in path:
        t_ev = t0 + timedelta(seconds=off)
        ev = {
            "tx_id": tx_id, "type": spec["type"], "payer_wallet": spec["payer"],
            "payee_id": spec["payee"], "device_id": spec["device"], "city": spec["city"],
            "channel": spec["channel"], "status": status, "amount_mad": spec["amount"],
            "event_time": iso(t_ev),
        }
        if rng.random() < LATE_P:
            arrival = t_ev + timedelta(seconds=rng.uniform(60, LATE_MAX_S))   # téléphone hors réseau
        else:
            arrival = t_ev + timedelta(seconds=rng.uniform(0.05, 0.4))
        out.append((arrival, ev, fraud))
        if rng.random() < DUP_P:
            out.append((arrival + timedelta(seconds=rng.uniform(0.1, 5.0)), dict(ev), fraud))
    return out


# ----------------------------------------------------------------------------
# Mode file : historique
# ----------------------------------------------------------------------------
def run_file(args, rng, w):
    start = parse_iso(args.start)
    gap_ev = 0.5                       # un événement toutes les 0,5 s en moyenne
    tx_gap = gap_ev * AVG_EV_PER_TX
    plans = []
    t = start
    n_tx = int(args.n / AVG_EV_PER_TX * 1.03) + 1
    for _ in range(n_tx):
        t = t + timedelta(seconds=rng.expovariate(1.0 / tx_gap))
        plans.append((t, normal_spec(w), False))
    if not args.no_fraud:
        for t_f, spec in fraud_plans(w, start, t):
            plans.append((t_f, spec, True))
    plans.sort(key=lambda p: p[0])
    events = []
    for i, (t0, spec, _) in enumerate(plans):
        events.extend(build_events(rng, "TX-%08d" % (args.tx_start + i), t0, spec))
    events.sort(key=lambda e: e[0])
    # on ramène le total à N en retirant des événements normaux de fin de fichier
    excess = len(events) - args.n
    if excess > 0:
        keep = []
        for item in reversed(events):
            if excess > 0 and not item[2]:
                excess -= 1
                continue
            keep.append(item)
        events = list(reversed(keep))
    with open(args.out, "w", encoding="utf-8") as f:
        for _, ev, _ in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    print("Fichier écrit : %s (%d événements)" % (args.out, len(events)))
    if not args.no_fraud:
        gt_path = os.path.join(os.path.dirname(os.path.abspath(args.out)), "fraud_ground_truth.json")
        with open(gt_path, "w", encoding="utf-8") as f:
            json.dump(ground_truth(), f, indent=2, ensure_ascii=False)
        print("Vérité terrain des scénarios de fraude : %s" % gt_path)


# ----------------------------------------------------------------------------
# Mode kafka / stdout : flux continu
# ----------------------------------------------------------------------------
def run_stream(args, rng, w):
    sink = None
    if args.mode == "kafka":
        try:
            from kafka import KafkaProducer
        except ImportError:
            sys.exit("Le module kafka-python est absent : pip3 install kafka-python")
        sink = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
        )
        print("Connecté à Kafka (%s), topic '%s', débit cible : %.1f événements/s"
              % (args.bootstrap, args.topic, args.rate), flush=True)
    tx_gap = AVG_EV_PER_TX / args.rate
    heap = []
    seq = 0
    counter = args.tx_start
    cursor = datetime.now(timezone.utc)
    chunk_idx = 0
    sent = 0
    t_begin = time.time()
    t_report = t_begin

    def new_chunk():
        nonlocal cursor, seq, counter, chunk_idx
        chunk_start = cursor
        plans = []
        for _ in range(400):
            cursor = cursor + timedelta(seconds=rng.expovariate(1.0 / tx_gap))
            plans.append((cursor, normal_spec(w)))
        if not args.no_fraud and chunk_idx % 10 == 0:
            plans.extend(fraud_plans(w, chunk_start, cursor))
        plans.sort(key=lambda p: p[0])
        for t0, spec in plans:
            tx_id = "TX-%08d" % counter
            counter += 1
            for arrival, ev, _ in build_events(rng, tx_id, t0, spec):
                seq += 1
                heapq.heappush(heap, (arrival.timestamp(), seq, ev))
        chunk_idx += 1

    try:
        while True:
            now = time.time()
            if args.duration and now - t_begin >= args.duration:
                break
            while cursor.timestamp() < now + 30:
                new_chunk()
            while heap and heap[0][0] <= now:
                _, _, ev = heapq.heappop(heap)
                if sink is not None:
                    sink.send(args.topic, key=ev["tx_id"], value=ev)
                else:
                    print(json.dumps(ev, ensure_ascii=False), flush=True)
                sent += 1
            if now - t_report >= 10:
                print("[%s] %d événements envoyés (%.1f/s)"
                      % (datetime.now().strftime("%H:%M:%S"), sent, sent / (now - t_begin)),
                      file=sys.stderr, flush=True)
                t_report = now
            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        if sink is not None:
            sink.flush()
            sink.close()
        print("Arrêt : %d événements envoyés." % sent, file=sys.stderr, flush=True)


# ----------------------------------------------------------------------------
def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Générateur de transactions SahlPay")
    ap.add_argument("--mode", choices=["merchants", "file", "kafka", "stdout"], required=True)
    ap.add_argument("--n", type=int, default=50000, help="nombre d'événements (mode file)")
    ap.add_argument("--rate", type=float, default=20.0, help="événements par seconde (kafka/stdout)")
    ap.add_argument("--out", default=None, help="fichier de sortie (merchants / file)")
    ap.add_argument("--merchants", default=os.path.join(here, "merchants.json"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default="2026-11-14T08:00:00Z", help="début de l'historique (mode file)")
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--duration", type=float, default=0, help="durée en secondes (0 = infini)")
    ap.add_argument("--tx-start", type=int, default=None, help="premier numéro de transaction")
    ap.add_argument("--no-fraud", action="store_true", help="ne pas injecter les scénarios de fraude")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    if args.mode == "merchants":
        out = args.out or "merchants.json"
        merchants = gen_merchants(rng)
        with open(out, "w", encoding="utf-8") as f:
            for m in merchants:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        print("Référentiel écrit : %s (%d commerçants)" % (out, len(merchants)))
        return

    if os.path.exists(args.merchants):
        merchants = load_merchants(args.merchants)
    else:
        print("merchants.json introuvable, génération à la volée.", file=sys.stderr)
        merchants = gen_merchants(random.Random(args.seed))
    if args.tx_start is None:
        args.tx_start = 1 if args.mode == "file" else 1000001
    w = World(rng, merchants)
    if args.mode == "file":
        args.out = args.out or "transactions_history.jsonl"
        run_file(args, rng, w)
    else:
        run_stream(args, rng, w)


if __name__ == "__main__":
    main()
