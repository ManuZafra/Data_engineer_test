import sqlite3

# Conectar a la base de datos (montada desde el volumen)
conn = sqlite3.connect('/app/data/sales.db')
cursor = conn.cursor()

# Crear tablas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stores (
        store_id INTEGER PRIMARY KEY,
        store_name TEXT,
        city TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        unit_price REAL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INTEGER PRIMARY KEY,
        store_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        sale_date TEXT,
        total_amount REAL,
        FOREIGN KEY (store_id) REFERENCES stores(store_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS analysis_results (
        result_id INTEGER PRIMARY KEY,
        metric TEXT,
        value REAL,
        details TEXT
    )
''')

# Confirmar cambios y cerrar
conn.commit()
conn.close()

print("Tablas creadas en sales.db")
