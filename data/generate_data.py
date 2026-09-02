"""Script to generate 1000+ rows for customers.csv and orders.csv"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Chemins resolus depuis l'emplacement du script : le depot doit pouvoir
# etre clone n'importe ou, pas seulement dans C:\SF_DEV_EXP.
SOURCES_DIR = Path(__file__).resolve().parent / "sources_files"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)

# --- Customers ---
cities = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux", "Strasbourg",
    "Nantes", "Montpellier", "Rennes", "Lille", "Grenoble", "Dijon", "Angers",
    "Toulon", "Reims", "Clermont-Ferrand", "Le Havre", "Brest", "Rouen",
    "Metz", "Orleans", "Amiens", "Tours", "Limoges", "Perpignan", "Poitiers",
    "Pau", "Caen", "Nancy"
]

first_names = [
    "Jean", "Marie", "Pierre", "Sophie", "Lucas", "Emma", "Hugo", "Lea",
    "Nathan", "Chloe", "Tom", "Alice", "David", "Eva", "Charlie", "Julie",
    "Louis", "Camille", "Antoine", "Clara", "Maxime", "Ines", "Theo", "Manon",
    "Paul", "Sarah", "Arthur", "Laura", "Raphael", "Jade", "Etienne", "Elise",
    "Gabriel", "Lola", "Victor", "Margaux", "Alex", "Pauline", "Adrien", "Charlotte",
    "Bastien", "Oceane", "Romain", "Amandine", "Clement", "Aurelie", "Julien", "Marine",
    "Damien", "Justine"
]

last_names = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit",
    "Durand", "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel",
    "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier", "Morel",
    "Girard", "Andre", "Mercier", "Dupont", "Lambert", "Bonnet", "Francois",
    "Martinez", "Legrand", "Garnier", "Faure", "Rousseau", "Blanc", "Guerin",
    "Muller", "Henry", "Roussel", "Nicolas", "Perrin", "Morin", "Mathieu",
    "Clement", "Gauthier", "Dumont", "Lopez", "Fontaine", "Chevalier", "Robin",
    "Masson"
]

base_date = datetime(2026, 1, 1, 8, 0, 0)

# Keep existing rows, add new customers starting from ID 116
with open(SOURCES_DIR / "customers.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "name", "email", "city", "updated_at"])

    # Original rows (with duplicates for dedup testing)
    existing = [
        [101, "John Doe", "john@mail.com", "Paris", "2026-01-01 09:00:00"],
        [101, "John Doe", "John@mail.com", "Paris", "2026-01-01 10:00:00"],
        [102, "Jane Smith", "jane@mail.com", "Lyon", "2026-01-01 11:00:00"],
        [103, "Bob Lee", "bob@mail.com", "Marseille", "2026-01-01 12:00:00"],
        [104, "Alice Martin", "Alice@Mail.com", "Toulouse", "2026-01-02 08:00:00"],
        [104, "Alice Martin", "alice@mail.com", "Toulouse", "2026-01-02 09:30:00"],
        [105, "Charlie Dupont", "charlie@mail.com", "Nice", "2026-01-02 10:00:00"],
        [106, "Eva Bernard", "EVA@mail.com", "Bordeaux", "2026-01-03 08:00:00"],
        [106, "Eva Bernard", "eva@mail.com", "Bordeaux", "2026-01-03 14:00:00"],
        [107, "David Moreau", "david@mail.com", "Strasbourg", "2026-01-03 09:00:00"],
        [108, "Sophie Leroy", "sophie@mail.com", "Nantes", "2026-01-04 10:00:00"],
        [109, "Lucas Petit", "lucas@mail.com", "Montpellier", "2026-01-04 11:00:00"],
        [110, "Emma Duval", "emma@mail.com", "Rennes", "2026-01-05 08:00:00"],
        [111, "Hugo Garnier", "hugo@mail.com", "Lille", "2026-01-05 09:00:00"],
        [112, "Lea Faure", "lea@mail.com", "Grenoble", "2026-01-06 10:00:00"],
        [113, "Nathan Bonnet", "nathan@mail.com", "Dijon", "2026-01-06 11:00:00"],
        [114, "Chloe Mercier", "chloe@mail.com", "Angers", "2026-01-07 08:00:00"],
        [115, "Tom Lambert", "tom@mail.com", "Toulon", "2026-01-07 09:00:00"],
    ]
    for row in existing:
        writer.writerow(row)

    # Generate 1000 new unique customers (116 -> 1115)
    new_id = 116
    for i in range(1000):
        cid = new_id + i
        fname = random.choice(first_names)
        lname = random.choice(last_names)
        name = f"{fname} {lname}"
        city = random.choice(cities)
        ts = base_date + timedelta(hours=random.randint(0, 2000))
        email_base = f"{fname.lower()}.{lname.lower()}{cid}@mail.com"

        # ~15% chance of duplicate row (email case variation + later timestamp)
        if random.random() < 0.15:
            writer.writerow([cid, name, email_base.upper(), city, ts.strftime("%Y-%m-%d %H:%M:%S")])
            ts2 = ts + timedelta(hours=random.randint(1, 48))
            writer.writerow([cid, name, email_base, city, ts2.strftime("%Y-%m-%d %H:%M:%S")])
        else:
            writer.writerow([cid, name, email_base, city, ts.strftime("%Y-%m-%d %H:%M:%S")])

print("customers.csv generated!")

# --- Orders ---
statuses = ["pending", "shipped", "completed", "cancelled"]
product_ids = list(range(2001, 2051))  # 50 products

with open(SOURCES_DIR / "orders.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "product_id", "timestamp", "amount", "status", "updated_at"])

    # Original rows
    existing_orders = [
        [1, 101, 2001, "2026-01-01 10:00:00", 100, "pending", "2026-01-01 10:01:00"],
        [1, 101, 2001, "2026-01-01 10:00:00", 100, "shipped", "2026-01-01 10:05:00"],
        [2, 102, 2002, "2026-01-01 11:00:00", 200, "pending", "2026-01-01 11:01:00"],
        [3, 103, 2003, "2026-01-01 12:00:00", 150, "cancelled", "2026-01-01 12:02:00"],
        [3, 103, 2003, "2026-01-01 12:00:00", 150, "completed", "2026-01-01 12:10:00"],
        [4, 101, 2004, "2026-01-02 09:00:00", 75, "pending", "2026-01-02 09:01:00"],
        [4, 101, 2004, "2026-01-02 09:00:00", 75, "completed", "2026-01-02 09:30:00"],
        [5, 104, 2001, "2026-01-02 10:00:00", 120, "completed", "2026-01-02 10:15:00"],
        [6, 105, 2005, "2026-01-02 14:00:00", 300, "pending", "2026-01-02 14:01:00"],
        [6, 105, 2005, "2026-01-02 14:00:00", 300, "shipped", "2026-01-02 15:00:00"],
        [7, 102, 2003, "2026-01-03 08:30:00", 180, "completed", "2026-01-03 09:00:00"],
        [8, 106, 2002, "2026-01-03 11:00:00", 250, "pending", "2026-01-03 11:01:00"],
        [8, 106, 2002, "2026-01-03 11:00:00", 250, "completed", "2026-01-03 12:00:00"],
        [9, 107, 2006, "2026-01-03 15:00:00", 90, "cancelled", "2026-01-03 15:05:00"],
        [10, 108, 2001, "2026-01-04 09:00:00", 100, "completed", "2026-01-04 09:20:00"],
        [11, 109, 2004, "2026-01-04 10:30:00", 175, "shipped", "2026-01-04 11:00:00"],
        [12, 104, 2007, "2026-01-05 08:00:00", 420, "pending", "2026-01-05 08:01:00"],
        [12, 104, 2007, "2026-01-05 08:00:00", 420, "completed", "2026-01-05 10:00:00"],
        [13, 110, 2002, "2026-01-05 11:00:00", 200, "completed", "2026-01-05 11:30:00"],
        [14, 111, 2008, "2026-01-05 14:00:00", 350, "shipped", "2026-01-05 14:30:00"],
        [15, 101, 2003, "2026-01-06 09:00:00", 150, "completed", "2026-01-06 09:45:00"],
        [16, 112, 2005, "2026-01-06 10:00:00", 300, "cancelled", "2026-01-06 10:10:00"],
        [17, 113, 2009, "2026-01-06 13:00:00", 85, "completed", "2026-01-06 13:20:00"],
        [18, 105, 2001, "2026-01-07 08:00:00", 100, "completed", "2026-01-07 08:30:00"],
        [19, 114, 2006, "2026-01-07 10:00:00", 190, "pending", "2026-01-07 10:01:00"],
        [19, 114, 2006, "2026-01-07 10:00:00", 190, "shipped", "2026-01-07 11:00:00"],
        [20, 115, 2002, "2026-01-07 14:00:00", 200, "completed", "2026-01-07 14:30:00"],
        [21, 106, 2010, "2026-01-08 09:00:00", 500, "pending", "2026-01-08 09:01:00"],
        [21, 106, 2010, "2026-01-08 09:00:00", 500, "completed", "2026-01-08 11:00:00"],
        [22, 103, 2004, "2026-01-08 10:00:00", 75, "completed", "2026-01-08 10:30:00"],
        [23, 108, 2008, "2026-01-08 13:00:00", 350, "cancelled", "2026-01-08 13:05:00"],
        [24, 109, 2003, "2026-01-09 08:00:00", 150, "completed", "2026-01-09 08:45:00"],
        [25, 102, 2010, "2026-01-09 11:00:00", 500, "completed", "2026-01-09 12:00:00"],
    ]
    for row in existing_orders:
        writer.writerow(row)

    # Generate 1000 new orders (26 -> 1025)
    all_customer_ids = list(range(101, 1116))  # all customers
    order_id = 26
    for i in range(1000):
        oid = order_id + i
        cid = random.choice(all_customer_ids)
        pid = random.choice(product_ids)
        ts = base_date + timedelta(hours=random.randint(0, 2000))
        amount = random.choice([50, 75, 85, 90, 100, 120, 150, 175, 190, 200, 250, 300, 350, 420, 500, 750, 999])
        final_status = random.choice(statuses)
        updated = ts + timedelta(minutes=random.randint(1, 120))

        # ~20% chance of duplicate (status update: pending -> final)
        if random.random() < 0.20:
            writer.writerow([oid, cid, pid, ts.strftime("%Y-%m-%d %H:%M:%S"), amount, "pending", ts.strftime("%Y-%m-%d %H:%M:%S")])
            writer.writerow([oid, cid, pid, ts.strftime("%Y-%m-%d %H:%M:%S"), amount, final_status, updated.strftime("%Y-%m-%d %H:%M:%S")])
        else:
            writer.writerow([oid, cid, pid, ts.strftime("%Y-%m-%d %H:%M:%S"), amount, final_status, updated.strftime("%Y-%m-%d %H:%M:%S")])

print("orders.csv generated!")

