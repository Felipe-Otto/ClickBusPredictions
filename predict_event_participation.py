import pandas as pd  # Data manipulation
from sklearn.model_selection import train_test_split  # Split data for training
from sklearn.linear_model import LogisticRegression  # Logistic Regression model
from sklearn.metrics import accuracy_score  # Model evaluation
import warnings  # Suppress warnings

warnings.filterwarnings("ignore")  # Ignore warnings


def load_data(path_clients, path_events):
    """Load client and event CSVs."""
    df_clients = pd.read_csv(path_clients, low_memory=False)  # Read clients CSV
    df_clients['DATA_COMPRA'] = pd.to_datetime(df_clients['DATA_COMPRA'])  # Convert purchase date
    df_clients['DATA_CADASTRO'] = pd.to_datetime(df_clients['DATA_CADASTRO'])  # Convert registration date

    df_events = pd.read_csv(path_events, low_memory=False)  # Read events CSV
    df_events['Data'] = pd.to_datetime(df_events['Data'], dayfirst=True)  # Convert event date
    return df_clients, df_events  # Return both DataFrames


def build_client_state(df_clients):
    """Extract unique client-state pairs."""
    estado_cols = ['ESTADO_IDA_ORIGEM', 'ESTADO_IDA_DESTINO', 'ESTADO_RETORNO_ORIGEM', 'ESTADO_RETORNO_DESTINO']
    # Combine all state columns into one DataFrame
    df_states = pd.concat([df_clients[['EMAIL_CLIENTE', col]].rename(columns={col: 'Estado'}) for col in estado_cols])
    return df_states.dropna().drop_duplicates()  # Drop missing and duplicate pairs


def compute_history(df_states):
    """Compute visits per client-state and normalized probability."""
    df_hist = df_states.groupby(['EMAIL_CLIENTE', 'Estado']).size().reset_index(name='visitas_estado')  # Count visits
    df_total = df_states.groupby('EMAIL_CLIENTE').size().reset_index(name='total_visitas')  # Total visits per client
    df_hist = df_hist.merge(df_total, on='EMAIL_CLIENTE', how='left')  # Merge totals
    df_hist['prob_historica'] = df_hist['visitas_estado'] / df_hist['total_visitas']  # Normalize probability
    return df_hist  # Return history DataFrame


def prepare_features(df_analise, df_clients):
    """Add features for modeling."""
    df_last = df_clients.groupby("EMAIL_CLIENTE")['DATA_COMPRA'].max().reset_index()  # Last purchase date
    df_analise = df_analise.merge(df_last, on="EMAIL_CLIENTE", how="left")  # Merge with analysis
    df_analise['dias_para_evento'] = (df_analise['Data'] - df_analise['DATA_COMPRA']).dt.days  # Days to event

    df_last_state = df_clients.groupby("EMAIL_CLIENTE").last().reset_index()
    df_last_state = df_last_state[['EMAIL_CLIENTE', 'ESTADO_IDA_ORIGEM']].rename(
        columns={'ESTADO_IDA_ORIGEM': 'ESTADO_CLIENTE'})  # Last origin state
    df_analise = df_analise.merge(df_last_state, on="EMAIL_CLIENTE", how="left")  # Merge client state
    df_analise['cliente_mora_no_estado_evento'] = (df_analise['ESTADO_CLIENTE'] == df_analise['Estado']).astype(
        int)  # Same state flag

    df_last_trip = df_clients.groupby('EMAIL_CLIENTE').last().reset_index()
    df_last_trip = df_last_trip[['EMAIL_CLIENTE', 'VALOR_TOTAL_PASSAGEM', 'QUANTIDADE_PASSAGENS']]  # Last trip info
    df_analise = df_analise.merge(df_last_trip, on='EMAIL_CLIENTE', how='left')  # Merge trip info

    df_hist = df_clients.groupby("EMAIL_CLIENTE").agg(
        compras_historicas=('EMAIL_CLIENTE', 'count'),  # Total purchases
        valor_medio_historico=('VALOR_TOTAL_PASSAGEM', 'mean')  # Avg ticket value
    ).reset_index()
    df_analise = df_analise.merge(df_hist, on="EMAIL_CLIENTE", how="left")  # Merge history

    return df_analise  # Return enriched DataFrame


def train_model(df, features, target='target'):
    """Train Logistic Regression if 2 classes exist, else return None."""
    df_model = df.dropna(subset=features)  # Drop rows with missing features
    X, y = df_model[features], df_model[target]  # Split features and target

    if y.nunique() < 2:  # Check if only one class
        print("Only one class detected, using historical probability as fallback.")
        return None
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)  # Split data
    model = LogisticRegression(random_state=42, solver='liblinear')  # Initialize model
    model.fit(X_train, y_train)  # Train model
    print(f"Model accuracy: {accuracy_score(y_test, model.predict(X_test)):.2f}")  # Print accuracy
    return model  # Return trained model


def predict_probability(df, model, features):
    """Compute event probability per client."""
    if model is None:  # Fallback to historical probability
        df['probabilidade_evento'] = df['prob_historica']
    else:
        df['probabilidade_evento'] = model.predict_proba(df[features].fillna(0))[:, 1]  # Predict probability
    return df  # Return DataFrame with probability


# ----------------- Main -----------------
if __name__ == "__main__":
    PATH_CLIENTES = "resources/travel_data_export.csv"  # Client CSV
    PATH_EVENTOS = "resources/eventos.csv"  # Event CSV
    OUTPUT_PATH = "predicoes_eventos.csv"  # Output CSV

    df_clients, df_events = load_data(PATH_CLIENTES, PATH_EVENTOS)  # Load data
    df_states = build_client_state(df_clients)  # Build client-state table
    df_hist = compute_history(df_states)  # Compute historical visits

    df_analise = df_hist.merge(df_events, left_on='Estado', right_on='Estado', how='right')  # Merge with events
    df_analise[['visitas_estado', 'total_visitas', 'prob_historica']] = df_analise[
        ['visitas_estado', 'total_visitas', 'prob_historica']].fillna(0)  # Fill missing

    df_analise = prepare_features(df_analise, df_clients)  # Add features

    df_analise['target'] = (df_analise['visitas_estado'] > 0).astype(int)  # Target: visited state at least once

    features = ['dias_para_evento', 'cliente_mora_no_estado_evento', 'VALOR_TOTAL_PASSAGEM',
                'QUANTIDADE_PASSAGENS', 'compras_historicas', 'valor_medio_historico']  # Model features

    model = train_model(df_analise, features)  # Train model
    df_analise = predict_probability(df_analise, model, features)  # Predict probabilities

    df_final = df_analise[['EMAIL_CLIENTE', 'probabilidade_evento', 'Evento', 'Data', 'Cidade']].sort_values(
        'probabilidade_evento', ascending=False)  # Final selection
    df_final.to_csv(OUTPUT_PATH, index=False)  # Save CSV
    print(f"Predictions saved to {OUTPUT_PATH}")  # Confirmation
