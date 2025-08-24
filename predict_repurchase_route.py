"""
Este script processa o histórico de compras de clientes para prever a probabilidade
de recompra em rotas específicas (origem → destino) nos próximos 30 dias.
Ele lê os dados em chunks, calcula features históricas e temporais,
trata categorias de alta cardinalidade, treina o modelo Cox Proportional Hazards
e exporta a rota com maior probabilidade por cliente.
"""

# ----------------- Imports -----------------
import pandas as pd  # Manipulação de DataFrames
import numpy as np   # Operações numéricas e tratamento de arrays
import gc            # Coleta manual de memória
from lifelines import CoxPHFitter  # Modelo de sobrevivência Cox Proportional Hazards
import warnings      # Controle de avisos do Python

warnings.filterwarnings("ignore")  # Ignorar avisos não críticos

# ----------------- Configurações -----------------
PATH = "resources/travel_data_export.csv"        # Caminho do CSV de entrada
CHUNKSIZE = 200_000                              # Número de linhas por chunk ao ler
SAMPLE_FRAC = 1                                  # Fração de clientes a amostrar
RANDOM_STATE = 42                                # Semente para amostragem reprodutível
OUTPUT_CSV = "repurchase_route_30days.csv"      # Caminho de saída do CSV de predições

USECOLS = [  # Colunas necessárias do CSV
    "EMAIL_CLIENTE", "DATA_COMPRA", "HORA_COMPRA",
    "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
    "GENERO", "NOME_CATEGORIA",
    "CIDADE_IDA_ORIGEM", "CIDADE_IDA_DESTINO"
]

TOP_CATS = 10  # Número máximo de categorias a manter para GENERO/NOME_CATEGORIA

# ----------------- 1) Amostragem de clientes -----------------
print("Sampling clients...")  # Log de progresso

email_series = pd.Series(dtype="object")  # Inicializa série vazia para emails

# Lê o CSV em chunks para economizar memória
for chunk in pd.read_csv(PATH, usecols=["EMAIL_CLIENTE"], chunksize=CHUNKSIZE):
    email_series = pd.concat([email_series, chunk["EMAIL_CLIENTE"].astype(str)], ignore_index=True)  # Acumula emails

# Remove duplicados e valores nulos, obtendo emails únicos
unique_emails = email_series.dropna().drop_duplicates()
n_clients = len(unique_emails)  # Total de clientes únicos

# Determina o tamanho da amostra (mínimo 500)
sample_n = max(500, int(SAMPLE_FRAC * n_clients))
sample_clients = unique_emails.sample(n=sample_n, random_state=RANDOM_STATE)  # Amostra aleatória
sample_set = set(sample_clients.values)  # Converte para set para filtro rápido

print(f"Unique clients: {n_clients:,} | Sampled clients: {len(sample_set):,}")  # Log estatísticas
del email_series, unique_emails, sample_clients  # Libera memória
gc.collect()  # Coleta manual de lixo

# ----------------- 2) Construção do dataset -----------------
print("Building dataset in chunks...")  # Log de progresso
parts = []  # Lista para armazenar chunks processados

# Lê CSV em chunks, filtrando apenas clientes amostrados
for chunk in pd.read_csv(PATH, usecols=USECOLS, chunksize=CHUNKSIZE, dtype=str):
    chunk = chunk[chunk["EMAIL_CLIENTE"].isin(sample_set)].copy()  # Mantém apenas clientes amostrados
    if chunk.empty:
        continue  # Pula chunks vazios

    # Converte colunas numéricas
    chunk["VALOR_TOTAL_PASSAGEM"] = pd.to_numeric(chunk["VALOR_TOTAL_PASSAGEM"], errors="coerce")
    chunk["QUANTIDADE_PASSAGENS"] = pd.to_numeric(chunk["QUANTIDADE_PASSAGENS"], errors="coerce")

    # Combina data e hora em datetime
    dt_str = (chunk["DATA_COMPRA"].fillna("") + " " + chunk["HORA_COMPRA"].fillna("")).str.strip()
    chunk["DATA_HORA_COMPRA"] = pd.to_datetime(dt_str, errors="coerce")

    # Renomeia colunas de cidade para ORIGEM/DESTINO
    chunk = chunk.rename(columns={
        "CIDADE_IDA_ORIGEM": "ORIGEM",
        "CIDADE_IDA_DESTINO": "DESTINO"
    })

    # Ordena por cliente e datetime para sequência de compras
    chunk = chunk.sort_values(["EMAIL_CLIENTE", "DATA_HORA_COMPRA"])

    # Mantém apenas colunas necessárias e adiciona à lista
    parts.append(chunk[[
        "EMAIL_CLIENTE", "DATA_HORA_COMPRA",
        "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
        "GENERO", "NOME_CATEGORIA", "ORIGEM", "DESTINO"
    ]])

# Concatena todos os chunks
df = pd.concat(parts, ignore_index=True)
del parts  # Libera lista
gc.collect()  # Coleta manual de lixo

# Remove linhas sem datetime válido
df = df.dropna(subset=["DATA_HORA_COMPRA"])

# Ordena para calcular sequência de rotas
df = df.sort_values(["EMAIL_CLIENTE", "ORIGEM", "DESTINO", "DATA_HORA_COMPRA"])

# ----------------- 3) Cálculo de tempo até a próxima compra na mesma rota -----------------
next_time = df.groupby(["EMAIL_CLIENTE", "ORIGEM", "DESTINO"])["DATA_HORA_COMPRA"].shift(-1)  # Próxima compra na rota
df["tempo_prox"] = (next_time - df["DATA_HORA_COMPRA"]).dt.days  # Dias até próxima compra
df["evento"] = (~df["tempo_prox"].isna()).astype("int8")  # 1 se evento ocorreu, 0 caso censurado
df["tempo_prox"] = df["tempo_prox"].fillna(9999).astype("float32")  # Censura para clientes sem próxima compra

# ----------------- 4) Feature engineering -----------------
prev_time = df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(1)  # Compra anterior do cliente
df["dias_desde_ultima"] = (df["DATA_HORA_COMPRA"] - prev_time).dt.days.astype("float32")  # Dias desde última compra
df["compras_ate_agora"] = df.groupby("EMAIL_CLIENTE").cumcount().astype("float32")  # Contagem cumulativa de compras

# Valor médio histórico (excluindo compra atual)
val_shift = df.groupby("EMAIL_CLIENTE")["VALOR_TOTAL_PASSAGEM"].shift(1)
cum_sum = val_shift.groupby(df["EMAIL_CLIENTE"]).cumsum()
cum_cnt = df.groupby("EMAIL_CLIENTE").cumcount()
df["valor_medio_historico"] = (cum_sum / cum_cnt.replace(0, np.nan)).astype("float32")

# Extração de features de data
dt = df["DATA_HORA_COMPRA"]
df["compra_ano"] = dt.dt.year.astype("int16")
df["compra_mes"] = dt.dt.month.astype("int8")
df["compra_dia"] = dt.dt.day.astype("int8")
df["compra_dow"] = dt.dt.dayofweek.astype("int8")
df["compra_fds"] = dt.dt.dayofweek.isin([5, 6]).astype("int8")
df["compra_semana_ano"] = dt.dt.isocalendar().week.astype("int16")

# Frequência de cidades
for col in ["ORIGEM", "DESTINO"]:
    freq = df[col].value_counts(dropna=True) / len(df)
    df[f"{col}_freq"] = df[col].map(freq).astype("float32").fillna(0.0)

# Ticket médio por cidade
city_mean_val = df.groupby("ORIGEM")["VALOR_TOTAL_PASSAGEM"].mean()
df["ORIGEM_ticket_mean"] = df["ORIGEM"].map(city_mean_val).astype("float32").fillna(0.0)
city_mean_val_dest = df.groupby("DESTINO")["VALOR_TOTAL_PASSAGEM"].mean()
df["DESTINO_ticket_mean"] = df["DESTINO"].map(city_mean_val_dest).astype("float32").fillna(0.0)

# ----------------- 5) Tratamento de categorias de alta cardinalidade -----------------
def cap_rare(series: pd.Series, top_n: int) -> pd.Series:
    top = series.value_counts().index[:top_n]
    return series.where(series.isin(top), other="OTHER")  # Substitui raros por OTHER

df["GENERO_cap"] = cap_rare(df["GENERO"].astype(str), TOP_CATS)
df["NOME_CATEGORIA_cap"] = cap_rare(df["NOME_CATEGORIA"].astype(str), TOP_CATS)

# One-hot encoding
df = pd.get_dummies(df, columns=["GENERO_cap", "NOME_CATEGORIA_cap"], drop_first=True)

# ----------------- 6) Preparação final para modelagem -----------------
num_cols = [
    "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
    "dias_desde_ultima", "compras_ate_agora", "valor_medio_historico",
    "compra_ano", "compra_mes", "compra_dia", "compra_dow", "compra_fds", "compra_semana_ano",
    "ORIGEM_freq", "DESTINO_freq", "ORIGEM_ticket_mean", "DESTINO_ticket_mean"
]
dummy_cols = [c for c in df.columns if c.startswith("GENERO_cap_") or c.startswith("NOME_CATEGORIA_cap_")]
feature_cols = num_cols + dummy_cols  # Features completas

# Colunas finais a manter
keep_cols = feature_cols + ["tempo_prox", "evento", "EMAIL_CLIENTE", "ORIGEM", "DESTINO"]
df_model = df[keep_cols].dropna(subset=feature_cols + ["tempo_prox", "evento"])  # Dataset final

# Conversão de tipos
for c in num_cols:
    df_model[c] = df_model[c].astype("float32")
for c in dummy_cols:
    df_model[c] = df_model[c].astype("int8")
df_model["tempo_prox"] = df_model["tempo_prox"].astype("float32")
df_model["evento"] = df_model["evento"].astype("int8")

# ----------------- 7) Treinamento do modelo CoxPH -----------------
print("Training CoxPH model...")
cph = CoxPHFitter(penalizer=0.3)  # Penalizador L2 para estabilidade
cph.fit(df_model[feature_cols + ["tempo_prox", "evento"]],
        duration_col="tempo_prox",
        event_col="evento",
        show_progress=True)
print("Model trained!")

# ----------------- 8) Predição de recompra em 30 dias -----------------
surv = cph.predict_survival_function(df_model[feature_cols], times=[30]).T
time_col = surv.columns[0]  # Coluna de 30 dias
df_model["prob_30dias"] = (1.0 - surv[time_col]).astype("float32")  # Probabilidade de recompra

# Seleciona rota com maior probabilidade por cliente
top_idx = df_model.groupby("EMAIL_CLIENTE")["prob_30dias"].idxmax()
top_routes = df_model.loc[top_idx, ["EMAIL_CLIENTE", "ORIGEM", "DESTINO", "prob_30dias"]]
top_routes = top_routes.sort_values("prob_30dias", ascending=False)

# ----------------- 9) Exportação -----------------
top_routes.to_csv(OUTPUT_CSV, index=False)  # Salva CSV final
print(f"Predictions saved to {OUTPUT_CSV}")
