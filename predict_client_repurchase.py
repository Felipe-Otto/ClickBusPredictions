"""
Este script processa o histórico de compras de clientes para prever a probabilidade
de recompra em até 30 dias utilizando o modelo de Cox Proportional Hazards.
Ele lê os dados em chunks para economizar memória, calcula features temporais
e históricas, treina o modelo de sobrevivência e exporta as previsões por cliente.
"""

# Bibliotecas para manipulação de dados
import pandas as pd  # DataFrame e manipulação de tabelas
import gc            # Coleta manual de lixo para liberar memória
from lifelines import CoxPHFitter  # Modelo de sobrevivência (Cox Proportional Hazards)
import warnings      # Controle de avisos do Python

# Ignorar warnings
warnings.filterwarnings("ignore")

# Caminho do arquivo CSV com histórico de compras
PATH = "resources/travel_data_export.csv"  

# Porcentagem de clientes a serem amostrados (100%)
SAMPLE_FRAC = 1                          

# Número de linhas lidas por chunk (para economizar memória)
CHUNKSIZE = 200_000                       

# Semente para reprodutibilidade da amostra
RANDOM_STATE = 42                            

# Colunas relevantes do CSV
USECOLS = [
    "EMAIL_CLIENTE",
    "DATA_COMPRA",
    "HORA_COMPRA",
    "VALOR_TOTAL_PASSAGEM",
    "QUANTIDADE_PASSAGENS",
    "GENERO",
    "NOME_CATEGORIA",
    "REGIAO_IDA_ORIGEM",
    "REGIAO_IDA_DESTINO",
]

# ----------------- 1) Amostragem de clientes -----------------
print("Reading and sampling clients...")

# Inicializa série vazia para armazenar emails
email_series = pd.Series(dtype="object")  

# Lê o CSV em chunks para evitar estouro de memória
for chunk in pd.read_csv(PATH, usecols=["EMAIL_CLIENTE"], chunksize=CHUNKSIZE):
    # Concatena os emails do chunk à série principal
    email_series = pd.concat([email_series, chunk["EMAIL_CLIENTE"].astype(str)], ignore_index=True)

# Remove duplicados e valores nulos, obtendo clientes únicos
unique_emails = email_series.dropna().drop_duplicates()
n_clients = len(unique_emails)  # Conta o número total de clientes únicos

# Calcula número de clientes amostrados (no mínimo 500)
sample_n = max(500, int(SAMPLE_FRAC * n_clients))
# Amostra clientes de forma aleatória
sample_clients = unique_emails.sample(n=sample_n, random_state=RANDOM_STATE)
# Converte para set para filtro rápido
sample_set = set(sample_clients.values)  

print(f"Unique clients: {n_clients:,} | Sampled clients: {len(sample_set):,}")

# Libera memória usada pela série e lista de emails
del email_series, unique_emails
gc.collect()

# ----------------- 2) Construção do dataset -----------------
print("Building dataset...")

# Lista para armazenar os chunks processados
data_parts = []  

# Lê novamente o CSV em chunks, filtrando apenas clientes amostrados
for chunk in pd.read_csv(PATH, usecols=USECOLS, chunksize=CHUNKSIZE, dtype=str):
    chunk = chunk[chunk["EMAIL_CLIENTE"].isin(sample_set)].copy()  # Filtra clientes
    if chunk.empty:  # Pula chunk vazio
        continue

    # Converte colunas numéricas para float
    for col in ["VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS"]:
        chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

    # Combina DATA_COMPRA e HORA_COMPRA em datetime
    chunk["DATA_HORA_COMPRA"] = pd.to_datetime(
        chunk["DATA_COMPRA"] + " " + chunk.get("HORA_COMPRA", ""),
        errors="coerce"
    )

    # Mantém apenas colunas relevantes
    keep_cols = [
        "EMAIL_CLIENTE",
        "DATA_HORA_COMPRA",
        "VALOR_TOTAL_PASSAGEM",
        "QUANTIDADE_PASSAGENS",
        "GENERO",
        "NOME_CATEGORIA",
        "REGIAO_IDA_ORIGEM",
        "REGIAO_IDA_DESTINO"
    ]
    data_parts.append(chunk[keep_cols])

# Concatena todos os chunks em um DataFrame único
df = pd.concat(data_parts, ignore_index=True)
del data_parts
gc.collect()

# Ordena por cliente e datetime
df = df.sort_values(["EMAIL_CLIENTE", "DATA_HORA_COMPRA"])

# ----------------- 3) Cálculo do tempo até a próxima compra -----------------
# Tempo até a próxima compra do mesmo cliente
df["tempo_prox"] = (df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(-1) - df["DATA_HORA_COMPRA"]).dt.days
# Evento = 1 se houver próxima compra, 0 caso contrário
df["evento"] = (~df["tempo_prox"].isna()).astype(int)
# Preenche tempos censurados (clientes sem próxima compra)
df["tempo_prox"] = df["tempo_prox"].fillna(9999)  

# ----------------- 4) Feature engineering -----------------
# Dias desde a última compra
df["dias_desde_ultima"] = (df["DATA_HORA_COMPRA"] - df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(1)).dt.days

# Número cumulativo de compras até o momento
df["compras_ate_agora"] = df.groupby("EMAIL_CLIENTE").cumcount()

# Valor médio histórico de compras (excluindo a compra atual)
df["valor_medio_historico"] = df.groupby("EMAIL_CLIENTE")["VALOR_TOTAL_PASSAGEM"].apply(
    lambda s: s.shift(1).expanding().mean()
).reset_index(level=0, drop=True)

# Extração de features de data
dt = df["DATA_HORA_COMPRA"]
df["compra_ano"] = dt.dt.year
df["compra_mes"] = dt.dt.month
df["compra_dia"] = dt.dt.day
df["compra_dow"] = dt.dt.dayofweek
df["compra_fds"] = dt.dt.dayofweek.isin([5,6]).astype(int)
df["compra_semana_ano"] = dt.dt.isocalendar().week.astype(int)

# ----------------- 5) Preparação das features para o modelo -----------------
feature_cols = [
    "VALOR_TOTAL_PASSAGEM",
    "QUANTIDADE_PASSAGENS",
    "dias_desde_ultima",
    "compras_ate_agora",
    "valor_medio_historico",
    "compra_ano",
    "compra_mes",
    "compra_dia",
    "compra_dow",
    "compra_fds",
    "compra_semana_ano"
]

# One-hot encoding para variáveis categóricas
categorical = ["GENERO", "NOME_CATEGORIA", "REGIAO_IDA_ORIGEM", "REGIAO_IDA_DESTINO"]
df = pd.get_dummies(df, columns=categorical, drop_first=True)

# Adiciona colunas de dummy à lista de features
feature_cols += [c for c in df.columns if any(cat in c for cat in categorical)]

# Dataset final para modelagem
df_model = df[feature_cols + ["tempo_prox", "evento"]].dropna()

# ----------------- 6) Treinamento do modelo Cox Proportional Hazards -----------------
cph = CoxPHFitter()
cph.fit(df_model, duration_col="tempo_prox", event_col="evento", show_progress=True)
print("\nModel trained!")

# ----------------- 7) Predições -----------------
# Probabilidade de recompra em até 30 dias
df_model["prob_30dias"] = 1 - cph.predict_survival_function(df_model, times=[30]).T[30]

# Mostra os 10 clientes com maior probabilidade
print("\nTop clients with highest probability to repurchase in 30 days:")
print(df_model[["prob_30dias"]].sort_values("prob_30dias", ascending=False).head(10))

# ----------------- 8) Exportação das predições -----------------
# Copia df_model para preservar original
df_model_export = df_model.copy()
# Alinha EMAIL_CLIENTE com índice original
df_model_export["EMAIL_CLIENTE"] = df.loc[df_model.index, "EMAIL_CLIENTE"]

# Mantém apenas colunas relevantes
df_export = df_model_export[["EMAIL_CLIENTE", "prob_30dias"]]

# ----------------- 8.1) Agrupa duplicatas e calcula média -----------------
df_export = df_export.groupby("EMAIL_CLIENTE", as_index=False)["prob_30dias"].mean()

# Salva CSV final para Power BI
output_path = "predictions/repurchase_predictions_30days.csv"
df_export.to_csv(output_path, index=False)
print(f"Predictions saved to {output_path}")
