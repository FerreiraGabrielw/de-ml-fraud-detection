import pandas as pd

INPUT_FILE = "data/raw/fraudTrain.csv"
OUTPUT_FILE = "data/raw/fraudTrain_clean.csv"

print("Lendo dataset original...")

df = pd.read_csv(INPUT_FILE)

print(f"Linhas originais: {len(df)}")
print(f"Colunas originais: {len(df.columns)}")

# Remove coluna desnecessária
if "Unnamed: 0" in df.columns:
    print("Removendo coluna 'Unnamed: 0'...")
    df = df.drop(columns=["Unnamed: 0"])

# Padroniza nomes das colunas
df.columns = [
    col.strip().lower().replace(" ", "_")
    for col in df.columns
]

print("Salvando dataset limpo...")

df.to_csv(OUTPUT_FILE, index=False)

print("Dataset limpo criado com sucesso!")
print(f"Arquivo salvo em: {OUTPUT_FILE}")