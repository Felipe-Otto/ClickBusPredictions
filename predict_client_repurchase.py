import pandas as pd
import gc
from lifelines import CoxPHFitter
import warnings

# Ignore warnings for clean output
warnings.filterwarnings("ignore")

# ----------------- Configuration -----------------
PATH = "resources/travel_data_export.csv"  # CSV file path
SAMPLE_FRAC = 1                          # Fraction of clients to sample
CHUNKSIZE = 200_000                         # Number of rows per CSV chunk
RANDOM_STATE = 42                            # Seed for reproducibility

# Columns to read from CSV
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

# ----------------- 1) Sample clients -----------------
print("Reading and sampling clients...")

email_series = pd.Series(dtype="object")  # empty series for emails

# Read CSV in chunks to avoid memory issues
for chunk in pd.read_csv(PATH, usecols=["EMAIL_CLIENTE"], chunksize=CHUNKSIZE):
    email_series = pd.concat([email_series, chunk["EMAIL_CLIENTE"].astype(str)], ignore_index=True)

# Drop duplicates and NaNs to get unique clients
unique_emails = email_series.dropna().drop_duplicates()
n_clients = len(unique_emails)

# Sample a fraction of clients for faster processing
sample_n = max(500, int(SAMPLE_FRAC * n_clients))
sample_clients = unique_emails.sample(n=sample_n, random_state=RANDOM_STATE)
sample_set = set(sample_clients.values)  # convert to set for fast filtering

print(f"Unique clients: {n_clients:,} | Sampled clients: {len(sample_set):,}")

# Free memory
del email_series, unique_emails
gc.collect()

# ----------------- 2) Build dataset -----------------
print("Building dataset...")

data_parts = []  # list to store chunks

# Read CSV in chunks, filter sampled clients
for chunk in pd.read_csv(PATH, usecols=USECOLS, chunksize=CHUNKSIZE, dtype=str):
    chunk = chunk[chunk["EMAIL_CLIENTE"].isin(sample_set)].copy()
    if chunk.empty:
        continue

    # Convert numeric columns to float
    for col in ["VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS"]:
        chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

    # Combine date and time into datetime
    chunk["DATA_HORA_COMPRA"] = pd.to_datetime(
        chunk["DATA_COMPRA"] + " " + chunk.get("HORA_COMPRA", ""),
        errors="coerce"
    )

    # Keep only relevant columns
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

# Combine all chunks
df = pd.concat(data_parts, ignore_index=True)
del data_parts
gc.collect()

# Sort by client and datetime
df = df.sort_values(["EMAIL_CLIENTE", "DATA_HORA_COMPRA"])

# ----------------- 3) Compute time to next purchase -----------------
df["tempo_prox"] = (df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(-1) - df["DATA_HORA_COMPRA"]).dt.days
df["evento"] = (~df["tempo_prox"].isna()).astype(int)  # 1 if next purchase exists
df["tempo_prox"] = df["tempo_prox"].fillna(9999)       # fill censored times

# ----------------- 4) Feature engineering -----------------
# Days since last purchase
df["dias_desde_ultima"] = (df["DATA_HORA_COMPRA"] - df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(1)).dt.days

# Cumulative number of purchases
df["compras_ate_agora"] = df.groupby("EMAIL_CLIENTE").cumcount()

# Historical average value (excluding current purchase)
df["valor_medio_historico"] = df.groupby("EMAIL_CLIENTE")["VALOR_TOTAL_PASSAGEM"].apply(
    lambda s: s.shift(1).expanding().mean()
).reset_index(level=0, drop=True)

# Extract date features
dt = df["DATA_HORA_COMPRA"]
df["compra_ano"] = dt.dt.year
df["compra_mes"] = dt.dt.month
df["compra_dia"] = dt.dt.day
df["compra_dow"] = dt.dt.dayofweek
df["compra_fds"] = dt.dt.dayofweek.isin([5,6]).astype(int)
df["compra_semana_ano"] = dt.dt.isocalendar().week.astype(int)

# ----------------- 5) Prepare features for model -----------------
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

# One-hot encode categorical features
categorical = ["GENERO", "NOME_CATEGORIA", "REGIAO_IDA_ORIGEM", "REGIAO_IDA_DESTINO"]
df = pd.get_dummies(df, columns=categorical, drop_first=True)

# Add one-hot columns to feature list
feature_cols += [c for c in df.columns if any(cat in c for cat in categorical)]

# Final dataset for modeling
df_model = df[feature_cols + ["tempo_prox", "evento"]].dropna()

# ----------------- 6) Train Cox Proportional Hazards model -----------------
cph = CoxPHFitter()
cph.fit(df_model, duration_col="tempo_prox", event_col="evento", show_progress=True)
print("\nModel trained!")

# ----------------- 7) Predictions -----------------
df_model["prob_30dias"] = 1 - cph.predict_survival_function(df_model, times=[30]).T[30]

# Show top 10 clients with highest probability
print("\nTop clients with highest probability to repurchase in 30 days:")
print(df_model[["prob_30dias"]].sort_values("prob_30dias", ascending=False).head(10))

# ----------------- 8) Export predictions -----------------
# Copy df_model to preserve original
df_model_export = df_model.copy()

# Align EMAIL_CLIENTE with df_model index
df_model_export["EMAIL_CLIENTE"] = df.loc[df_model.index, "EMAIL_CLIENTE"]

# Keep only relevant columns
df_export = df_model_export[["EMAIL_CLIENTE", "prob_30dias"]]

# ----------------- 8.1) Group duplicate emails and take the mean -----------------
df_export = df_export.groupby("EMAIL_CLIENTE", as_index=False)["prob_30dias"].mean()

# Save CSV for Power BI
output_path = "predictions/repurchase_predictions_30days.csv"
df_export.to_csv(output_path, index=False)
print(f"Predictions saved to {output_path}")


