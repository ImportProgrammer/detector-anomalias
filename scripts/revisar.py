import pandas as pd

file_path = "data/Inventario General Disp ATM_Centro de Efectivo.xlsx"

# Leer Excel sin asumir headers
df = pd.read_excel(file_path, header=None, nrows=10)

print("📊 PRIMERAS 10 FILAS DEL EXCEL:")
print("="*80)
for i, row in df.iterrows():
    print(f"Línea {i}: {list(row[:5])}")  # Mostrar primeras 5 columnas
print("="*80)