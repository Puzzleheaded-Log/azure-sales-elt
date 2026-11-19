import pandas as pd
import numpy as np
import random

random.seed(42)
np.random.seed(42)

customers_raw = pd.read_csv("customers.csv", encoding="utf-8",
                            dtype={"customer_name": str, "customer_email": str, "city": str})
products = pd.read_csv("products.csv", encoding="utf-8",
                       dtype={"product_name": str, "category": str})

category_map = {
    1: "Food", 2: "Accessories", 3: "Food", 4: "Food", 5: "Accessories",
    6: "Beverages", 7: "Home", 8: "Food", 9: "Beverages", 10: "Accessories",
    11: "Food", 12: "Food", 13: "Food", 14: "Food", 15: "Food",
    16: "Food", 17: "Food", 18: "Home", 19: "Food", 20: "Home",
    21: "Home", 22: "Food", 23: "Food", 24: "Accessories", 25: "Home",
}
products["category"] = products["product_id"].map(category_map)

country_map = {
    "Cluj-Napoca": "Romania", "Bucuresti": "Romania", "Timisoara": "Romania",
    "Iasi": "Romania", "Brasov": "Romania", "Budapest": "Hungary",
    "Vienna": "Austria", "Berlin": "Germany",
}

COLS = ["order_id", "order_date", "customer_id", "customer_name", "customer_email",
        "city", "country", "product_id", "product_name", "category",
        "unit_price", "quantity", "discount"]

# Clienti fara email in sistem (consistent, pe toate randurile lor)
NO_EMAIL_ALWAYS = [3, 9, 17, 30]
NO_EMAIL_DAY1_ONLY = [7]          # isi completeaza emailul in ziua 2 (schimbare SCD2)


def build_sales(date, start_id, cust, prod, cust_ids, n_rows):
    cust = cust[cust["customer_id"].isin(cust_ids)].copy()
    cust["country"] = cust["city"].map(country_map)
    sales = pd.DataFrame({
        "order_id": range(start_id, start_id + n_rows),
        "order_date": date,
        "customer_id": np.random.choice(cust["customer_id"], n_rows),
        "product_id": np.random.choice(prod["product_id"], n_rows),
        "quantity": np.random.randint(1, 11, n_rows),
        "discount": np.random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2, 0.3], n_rows),
    })
    df = sales.merge(cust, on="customer_id").merge(prod, on="product_id")
    return df.sort_values("order_id")[COLS].reset_index(drop=True)


def inject_problems(df):
    """Strica intentionat date la nivel de rand; fiecare problema pe randuri diferite."""
    df = df.copy()
    idx = list(np.random.permutation(df.index))
    take = lambda n: [idx.pop() for _ in range(n)]

    df.loc[take(40), "customer_name"] = df["customer_name"].map(lambda s: f"  {s}  ")   # spatii in plus
    df.loc[take(40), "city"] = df["city"].str.upper()                                    # CLUJ-NAPOCA
    df.loc[take(40), "city"] = df["city"].str.lower()                                    # cluj-napoca
    df.loc[take(20), "discount"] = np.nan                                                # discount lipsa
    neg = take(10)
    df.loc[neg, "quantity"] = -df.loc[neg, "quantity"]                                   # cantitate negativa
    neg_p = take(5)
    df.loc[neg_p, "unit_price"] = -df.loc[neg_p, "unit_price"]                           # pret negativ

    df["order_date"] = df["order_date"].astype(str)
    bad = take(15)
    df.loc[bad, "order_date"] = pd.to_datetime(df.loc[bad, "order_date"]).dt.strftime("%d/%m/%Y")  # format gresit

    dups = df.sample(20)                                                                 # 20 duplicate exacte
    return pd.concat([df, dups]).reset_index(drop=True)


# ---------- ZIUA 1 ----------
customers1 = customers_raw.copy()
customers1.loc[customers1.customer_id.isin(NO_EMAIL_ALWAYS + NO_EMAIL_DAY1_ONLY), "customer_email"] = np.nan

# clientii 1-35 (36-40 apar abia a doua zi)
day1 = build_sales("2026-11-01", 1, customers1, products, range(1, 36), 780)
inject_problems(day1).to_csv("sales_2026-11-01.csv", index=False, encoding="utf-8")

# ---------- ZIUA 2: schimbari pentru SCD2 ----------
customers2 = customers_raw.copy()
customers2.loc[customers2.customer_id.isin(NO_EMAIL_ALWAYS), "customer_email"] = np.nan   # clientul 7 are email acum

for cid in [5, 12, 23]:                      # 3 clienti se muta in alt oras
    old = customers2.loc[customers2.customer_id == cid, "city"].iloc[0]
    customers2.loc[customers2.customer_id == cid, "city"] = random.choice(
        [c for c in country_map if c != old])

products2 = products.copy()
p = products2.product_id
products2.loc[p == 3, "unit_price"] = (products2.loc[p == 3, "unit_price"] * 1.15).round(2)   # scumpire
products2.loc[p == 8, "unit_price"] = (products2.loc[p == 8, "unit_price"] * 0.90).round(2)   # ieftinire
products2.loc[p == 14, "category"] = "Beverages"                                              # schimbare categorie

day2 = build_sales("2026-11-02", 801, customers2, products2, range(1, 41), 780)
inject_problems(day2).to_csv("sales_2026-11-02.csv", index=False, encoding="utf-8")

print("Gata: sales_2026-11-01.csv si sales_2026-11-02.csv")

#lllllll