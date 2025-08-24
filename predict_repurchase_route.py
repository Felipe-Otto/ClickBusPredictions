import pandas as pd  # Data manipulation
import numpy as np   # Numerical helpers
import gc            # Manual garbage collection
from lifelines import CoxPHFitter  # Cox proportional hazards model
import warnings      # Warning control

warnings.filterwarnings("ignore")  # Silence non-critical warnings

PATH = "resources/travel_data_export.csv"  # Input CSV path
CHUNKSIZE = 200_000                        # Rows per chunk when reading
SAMPLE_FRAC = 1                            # Fraction of clients to sample (use 1.0 later)
RANDOM_STATE = 42                          # Seed for reproducibility
OUTPUT_CSV = "repurchase_route_30days.csv"  # Output predictions path

USECOLS = [  # Columns needed from the source CSV
    "EMAIL_CLIENTE", "DATA_COMPRA", "HORA_COMPRA",
    "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
    "GENERO", "NOME_CATEGORIA",
    "CIDADE_IDA_ORIGEM", "CIDADE_IDA_DESTINO"
]

TOP_CATS = 10  # Max categories to keep for GENERO/NOME_CATEGORIA; rest → OTHER

print("Sampling clients...")  # Log progress

email_series = pd.Series(dtype="object")  # Init container for emails
for chunk in pd.read_csv(PATH, usecols=["EMAIL_CLIENTE"], chunksize=CHUNKSIZE):  # Stream emails
    email_series = pd.concat([email_series, chunk["EMAIL_CLIENTE"].astype(str)], ignore_index=True)  # Append chunk

unique_emails = email_series.dropna().drop_duplicates()  # Unique valid emails
n_clients = len(unique_emails)  # Count total unique clients
sample_n = max(500, int(SAMPLE_FRAC * n_clients))  # Compute sample size with a floor
sample_clients = unique_emails.sample(n=sample_n, random_state=RANDOM_STATE)  # Sample clients
sample_set = set(sample_clients.values)  # Convert to set for fast membership tests

print(f"Unique clients: {n_clients:,} | Sampled clients: {len(sample_set):,}")  # Log stats
del email_series, unique_emails, sample_clients  # Free memory
gc.collect()  # Trigger garbage collection

print("Building dataset in chunks...")  # Log progress
parts = []  # List to collect processed chunks

for chunk in pd.read_csv(PATH, usecols=USECOLS, chunksize=CHUNKSIZE, dtype=str):  # Stream data
    chunk = chunk[chunk["EMAIL_CLIENTE"].isin(sample_set)].copy()  # Keep only sampled clients
    if chunk.empty:  # Skip empty chunks
        continue

    chunk["VALOR_TOTAL_PASSAGEM"] = pd.to_numeric(chunk["VALOR_TOTAL_PASSAGEM"], errors="coerce")  # To numeric
    chunk["QUANTIDADE_PASSAGENS"] = pd.to_numeric(chunk["QUANTIDADE_PASSAGENS"], errors="coerce")  # To numeric

    dt_str = (chunk["DATA_COMPRA"].fillna("") + " " + chunk["HORA_COMPRA"].fillna("")).str.strip()  # Join date/time
    chunk["DATA_HORA_COMPRA"] = pd.to_datetime(dt_str, errors="coerce")  # Parse datetime

    chunk = chunk.rename(columns={  # Rename cities to ORIGEM/DESTINO
        "CIDADE_IDA_ORIGEM": "ORIGEM",
        "CIDADE_IDA_DESTINO": "DESTINO"
    })

    chunk = chunk.sort_values(["EMAIL_CLIENTE", "DATA_HORA_COMPRA"])  # Sort per client timeline

    parts.append(chunk[[  # Keep only required columns
        "EMAIL_CLIENTE", "DATA_HORA_COMPRA",
        "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
        "GENERO", "NOME_CATEGORIA", "ORIGEM", "DESTINO"
    ]])

df = pd.concat(parts, ignore_index=True)  # Combine all processed chunks
del parts  # Free list
gc.collect()  # Collect garbage

df = df.dropna(subset=["DATA_HORA_COMPRA"])  # Remove rows with invalid datetime
df = df.sort_values(["EMAIL_CLIENTE", "ORIGEM", "DESTINO", "DATA_HORA_COMPRA"])  # Sort for route sequences

next_time = df.groupby(["EMAIL_CLIENTE", "ORIGEM", "DESTINO"])["DATA_HORA_COMPRA"].shift(-1)  # Next purchase on same route
df["tempo_prox"] = (next_time - df["DATA_HORA_COMPRA"]).dt.days  # Days until next purchase
df["evento"] = (~df["tempo_prox"].isna()).astype("int8")  # 1 if event observed, else 0
df["tempo_prox"] = df["tempo_prox"].fillna(9999).astype("float32")  # Censoring for no next purchase

prev_time = df.groupby("EMAIL_CLIENTE")["DATA_HORA_COMPRA"].shift(1)  # Previous purchase per client
df["dias_desde_ultima"] = (df["DATA_HORA_COMPRA"] - prev_time).dt.days.astype("float32")  # Recency in days
df["compras_ate_agora"] = df.groupby("EMAIL_CLIENTE").cumcount().astype("float32")  # Purchase count so far

val_shift = df.groupby("EMAIL_CLIENTE")["VALOR_TOTAL_PASSAGEM"].shift(1)  # Previous ticket value
cum_sum = val_shift.groupby(df["EMAIL_CLIENTE"]).cumsum()  # Cumulative sum of previous values
cum_cnt = df.groupby("EMAIL_CLIENTE").cumcount()  # Count of previous purchases
df["valor_medio_historico"] = (cum_sum / cum_cnt.replace(0, np.nan)).astype("float32")  # Historical mean per client

dt = df["DATA_HORA_COMPRA"]  # Shortcut to datetime series
df["compra_ano"] = dt.dt.year.astype("int16")  # Year
df["compra_mes"] = dt.dt.month.astype("int8")  # Month
df["compra_dia"] = dt.dt.day.astype("int8")  # Day
df["compra_dow"] = dt.dt.dayofweek.astype("int8")  # Day of week
df["compra_fds"] = dt.dt.dayofweek.isin([5, 6]).astype("int8")  # Weekend flag
df["compra_semana_ano"] = dt.dt.isocalendar().week.astype("int16")  # ISO week number

for col in ["ORIGEM", "DESTINO"]:  # Encode city intensity via frequency
    freq = df[col].value_counts(dropna=True)  # Count occurrences per city
    freq = (freq / len(df)).astype("float32")  # Convert to frequency
    df[f"{col}_freq"] = df[col].map(freq).astype("float32").fillna(0.0)  # Map to frequency feature

city_mean_val = df.groupby("ORIGEM")["VALOR_TOTAL_PASSAGEM"].mean().astype("float32")  # Mean ticket by origin
df["ORIGEM_ticket_mean"] = df["ORIGEM"].map(city_mean_val).astype("float32").fillna(0.0)  # Map origin mean
city_mean_val_dest = df.groupby("DESTINO")["VALOR_TOTAL_PASSAGEM"].mean().astype("float32")  # Mean ticket by destination
df["DESTINO_ticket_mean"] = df["DESTINO"].map(city_mean_val_dest).astype("float32").fillna(0.0)  # Map destination mean

def cap_rare(series: pd.Series, top_n: int) -> pd.Series:  # Limit high-cardinality categories
    top = series.value_counts().index[:top_n]  # Top N frequent categories
    return series.where(series.isin(top), other="OTHER")  # Replace rare with OTHER

df["GENERO_cap"] = cap_rare(df["GENERO"].astype(str), TOP_CATS)  # Capped GENERO
df["NOME_CATEGORIA_cap"] = cap_rare(df["NOME_CATEGORIA"].astype(str), TOP_CATS)  # Capped NOME_CATEGORIA

df = pd.get_dummies(df, columns=["GENERO_cap", "NOME_CATEGORIA_cap"], drop_first=True)  # Compact one-hot encoding

num_cols = [  # Numeric feature columns
    "VALOR_TOTAL_PASSAGEM", "QUANTIDADE_PASSAGENS",
    "dias_desde_ultima", "compras_ate_agora", "valor_medio_historico",
    "compra_ano", "compra_mes", "compra_dia", "compra_dow", "compra_fds", "compra_semana_ano",
    "ORIGEM_freq", "DESTINO_freq", "ORIGEM_ticket_mean", "DESTINO_ticket_mean"
]
dummy_cols = [c for c in df.columns if c.startswith("GENERO_cap_") or c.startswith("NOME_CATEGORIA_cap_")]  # Dummy list

feature_cols = num_cols + dummy_cols  # Full feature set

keep_cols = feature_cols + ["tempo_prox", "evento", "EMAIL_CLIENTE", "ORIGEM", "DESTINO"]  # Columns to keep
df_model = df[keep_cols].dropna(subset=feature_cols + ["tempo_prox", "evento"])  # Final modeling frame

for c in num_cols:  # Cast numeric features to float32
    df_model[c] = df_model[c].astype("float32")
for c in dummy_cols:  # Cast dummies to int8
    df_model[c] = df_model[c].astype("int8")
df_model["tempo_prox"] = df_model["tempo_prox"].astype("float32")  # Cast target duration
df_model["evento"] = df_model["evento"].astype("int8")  # Cast event flag

print("Training CoxPH model...")  # Log progress
cph = CoxPHFitter(penalizer=0.3)  # Cox model with L2 penalty for stability/speed
cph.fit(df_model[feature_cols + ["tempo_prox", "evento"]], duration_col="tempo_prox", event_col="evento", show_progress=True)  # Fit model
print("Model trained!")  # Log success

surv = cph.predict_survival_function(df_model[feature_cols], times=[30]).T  # Survival at 30 days
time_col = surv.columns[0]  # Extract the 30-day column name
df_model["prob_30dias"] = (1.0 - surv[time_col]).astype("float32")  # Convert to event probability by 30 days

top_idx = df_model.groupby("EMAIL_CLIENTE")["prob_30dias"].idxmax()  # Best route per client
top_routes = df_model.loc[top_idx, ["EMAIL_CLIENTE", "ORIGEM", "DESTINO", "prob_30dias"]]  # Select columns
top_routes = top_routes.sort_values("prob_30dias", ascending=False)  # Sort by probability

top_routes.to_csv(OUTPUT_CSV, index=False)  # Write predictions to disk
print(f"Predictions saved to {OUTPUT_CSV}")  # Log output path
