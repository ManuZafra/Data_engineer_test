import sqlite3
import pandas as pd
import os

# Conectar a la base de datos
conn = sqlite3.connect('/app/data/sales.db')
cursor = conn.cursor()
print("Conexión a sales.db establecida")

# Crear tablas con el esquema actualizado
cursor.execute('''CREATE TABLE IF NOT EXISTS stores (
    store_id INTEGER PRIMARY KEY,
    city TEXT,
    employee_count INTEGER
)''')
print("Tabla stores creada")

cursor.execute('''CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    unit_price REAL,
    stock INTEGER
)''')
print("Tabla products creada")

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
print("Tabla sales creada")

cursor.execute('''CREATE TABLE IF NOT EXISTS analysis_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT,
    value REAL,
    details TEXT
)''')
print("Tabla analysis_results creada")

# Importar magasins.csv
try:
    stores_df = pd.read_csv('/app/data/magasins.csv')
    print(f"magasins.csv leído, {len(stores_df)} filas")
    for _, row in stores_df.iterrows():
        cursor.execute('INSERT OR IGNORE INTO stores (store_id, city, employee_count) VALUES (?, ?, ?)',
                       (row['ID Magasin'], row['Ville'], row['Nombre de salariés']))
    print("Datos de magasins.csv importados")
except Exception as e:
    print(f"Error al importar magasins.csv: {e}")

# Importar produits.csv
try:
    products_df = pd.read_csv('/app/data/produits.csv')
    print(f"produits.csv leído, {len(products_df)} filas")
    for _, row in products_df.iterrows():
        cursor.execute('INSERT OR IGNORE INTO products (product_id, product_name, unit_price, stock) VALUES (?, ?, ?, ?)',
                       (row['ID Référence produit'], row['Nom'], row['Prix'], row['Stock']))
    print("Datos de produits.csv importados")
except Exception as e:
    print(f"Error al importar produits.csv: {e}")

# Importar ventes.csv y calcular total_amount
try:
    sales_df = pd.read_csv('/app/data/ventes.csv')
    print(f"ventes.csv leído, {len(sales_df)} filas")
    for _, row in sales_df.iterrows():
        cursor.execute('SELECT unit_price FROM products WHERE product_id = ?', (row['ID Référence produit'],))
        unit_price_result = cursor.fetchone()
        if unit_price_result is None:
            print(f"Producto no encontrado: {row['ID Référence produit']}")
            continue
        unit_price = unit_price_result[0]
        total_amount = row['Quantité'] * unit_price
        cursor.execute('INSERT INTO sales (store_id, product_id, quantity, sale_date, total_amount) VALUES (?, ?, ?, ?, ?)',
                       (row['ID Magasin'], row['ID Référence produit'], row['Quantité'], row['Date'], total_amount))
    print("Datos de ventes.csv importados")
except Exception as e:
    print(f"Error al importar ventes.csv: {e}")

# Consultas SQL para análisis
try:
    # Ventas totales
    cursor.execute('SELECT SUM(total_amount) FROM sales')
    total_sales = cursor.fetchone()[0]
    cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                   ('total_sales', total_sales, 'Suma total de todas las ventas'))
    print("Ventas totales calculadas")

    # Ventas por producto
    cursor.execute('SELECT product_id, SUM(total_amount) FROM sales GROUP BY product_id')
    sales_by_product = cursor.fetchall()
    for product_id, total in sales_by_product:
        cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                       ('sales_by_product', total, f'Ventas para producto {product_id}'))
    print("Ventas por producto calculadas")

    # Ventas por región
    cursor.execute('SELECT s.city, SUM(sa.total_amount) FROM sales sa JOIN stores s ON sa.store_id = s.store_id GROUP BY s.city')
    sales_by_region = cursor.fetchall()
    for city, total in sales_by_region:
        cursor.execute('INSERT INTO analysis_results (metric, value, details) VALUES (?, ?, ?)',
                       ('sales_by_region', total, f'Ventas en {city}'))
    print("Ventas por región calculadas")
except Exception as e:
    print(f"Error en análisis SQL: {e}")

# Confirmar cambios
conn.commit()
conn.close()

print("Datos importados y análisis realizados en sales.db")
