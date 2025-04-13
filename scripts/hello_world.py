import sqlite3
import pandas as pd
import os

# Connecter à la base de données
conn = sqlite3.connect('/app/data/sales.db')
cursor = conn.cursor()
print("Connexion à sales.db établie")

# Créer les tables avec le schéma mis à jour
cursor.execute('''CREATE TABLE IF NOT EXISTS stores (
    store_id INTEGER PRIMARY KEY,
    city TEXT,
    employee_count INTEGER
)''')
print("Tableau stores créé")

cursor.execute('''CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    unit_price REAL,
    stock INTEGER
)''')
print("Tableau products créé")

cursor.execute('''CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id INTEGER,
    product_id TEXT,
    quantity INTEGER,
    sale_date TEXT,
    total_amount REAL,
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)''')
print("Tableau sales créé")

cursor.execute('''CREATE TABLE IF NOT EXISTS analysis_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT,
    value REAL,
    details TEXT
)''')
print("Tableau analysis_results créé")

# Importer magasins.csv
try:
    stores_df = pd.read_csv('/app/data/magasins.csv')
    print(f"magasins.csv lu, {len(stores_df)} filas")
    for _, row in stores_df.iterrows():
        cursor.execute('INSERT OR IGNORE INTO stores (store_id, city, employee_count) VALUES (?, ?, ?)',
                       (row['ID Magasin'], row['Ville'], row['Nombre de salariés']))
    print("Données de magasins.csv importées")
except Exception as e:
    print(f"Error al importar magasins.csv: {e}")

# Importer produits.csv
try:
    products_df = pd.read_csv('/app/data/produits.csv')
    print(f"produits.csv leído, {len(products_df)} filas")
    for _, row in products_df.iterrows():
        cursor.execute('INSERT OR IGNORE INTO products (product_id, product_name, unit_price, stock) VALUES (?, ?, ?, ?)',
                       (row['ID Référence produit'], row['Nom'], row['Prix'], row['Stock']))
    print("Datos de produits.csv importados")
except Exception as e:
    print(f"Erreur lors de l'importation de produits.csv: {e}")

# Importer ventes.csv y calculer total_amount
try:
    sales_df = pd.read_csv('/app/data/ventes.csv')
    print(f"ventes.csv lu, {len(sales_df)} filas")
    for _, row in sales_df.iterrows():
        cursor.execute('SELECT unit_price FROM products WHERE product_id = ?', (row['ID Référence produit'],))
        unit_price_result = cursor.fetchone()
        if unit_price_result is None:
            print(f"Produit non trouvé: {row['ID Référence produit']}")
            continue
        unit_price = unit_price_result[0]
        total_amount = row['Quantité'] * unit_price
        cursor.execute('INSERT INTO sales (store_id, product_id, quantity, sale_date, total_amount) VALUES (?, ?, ?, ?, ?)',
                       (row['ID Magasin'], row['ID Référence produit'], row['Quantité'], row['Date'], total_amount))
    print("Datos de ventes.csv importes")
except Exception as e:
    print(f"Error al importar ventes.csv: {e}")

# Requêtes SQL pour l'analyse
try:
    # Ventes totales
    cursor.execute('SELECT SUM(total_amount) FROM sales')
    total_sales = cursor.fetchone()[0]
    cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                   ('total_sales', total_sales, 'Suma total de todas las ventas'))
    print("Ventes totales calculées")

    # Ventes por produuit
    cursor.execute('SELECT product_id, SUM(total_amount) FROM sales GROUP BY product_id')
    sales_by_product = cursor.fetchall()
    for product_id, total in sales_by_product:
        cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                       ('sales_by_product', total, f'Ventas para producto {product_id}'))
    print("Ventes par produit calculées")

    # Ventes por región
    cursor.execute('SELECT s.city, SUM(sa.total_amount) FROM sales sa JOIN stores s ON sa.store_id = s.store_id GROUP BY s.city')
    sales_by_region = cursor.fetchall()
    for city, total in sales_by_region:
        cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                       ('sales_by_region', total, f'Ventas en {city}'))
    print("Ventes par région calculées")
except Exception as e:
    print(f"Error en análisis SQL: {e}")

# Confirmer les changements
conn.commit()
conn.close()

print("Données importées et analyses réalisées dans sales.db")
