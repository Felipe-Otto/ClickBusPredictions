"""
Este script processa dados de clientes e eventos para prever a probabilidade
de interesse do cliente em determinados eventos. 
Ele calcula histórico de visitas por estado, adiciona features temporais
e de viagem, treina um modelo de Regressão Logística (ou usa probabilidade histórica)
e exporta as predições em CSV.
"""

# ----------------- Imports -----------------
import pandas as pd  # Manipulação de DataFrames
from sklearn.model_selection import train_test_split  # Divisão de dados em treino/teste
from sklearn.linear_model import LogisticRegression  # Modelo de Regressão Logística
from sklearn.metrics import accuracy_score  # Avaliação de acurácia
import warnings  # Controle de avisos

warnings.filterwarnings("ignore")  # Ignora avisos não críticos

# ----------------- 1) Leitura de dados -----------------
PATH_CLIENTES = "resources/travel_data_export.csv"  # CSV de clientes
PATH_EVENTOS = "resources/eventos.csv"  # CSV de eventos
OUTPUT_PATH = "predicoes_eventos.csv"  # Caminho de saída

# Lê CSV de clientes
df_clients = pd.read_csv(PATH_CLIENTES, low_memory=False)
# Converte colunas de datas
df_clients['DATA_COMPRA'] = pd.to_datetime(df_clients['DATA_COMPRA'])
df_clients['DATA_CADASTRO'] = pd.to_datetime(df_clients['DATA_CADASTRO'])

# Lê CSV de eventos
df_events = pd.read_csv(PATH_EVENTOS, low_memory=False)
# Converte coluna de data do evento (dia primeiro)
df_events['Data'] = pd.to_datetime(df_events['Data'], dayfirst=True)

# ----------------- 2) Construção de pares cliente-estado -----------------
# Colunas de estado para análise
estado_cols = ['ESTADO_IDA_ORIGEM', 'ESTADO_IDA_DESTINO', 'ESTADO_RETORNO_ORIGEM', 'ESTADO_RETORNO_DESTINO']

# Concatena todas as colunas de estado em um DataFrame único
df_states = pd.concat([
    df_clients[['EMAIL_CLIENTE', col]].rename(columns={col: 'Estado'}) 
    for col in estado_cols
])
# Remove duplicados e valores nulos
df_states = df_states.dropna().drop_duplicates()

# ----------------- 3) Histórico de visitas -----------------
# Conta visitas de cada cliente a cada estado
df_hist = df_states.groupby(['EMAIL_CLIENTE', 'Estado']).size().reset_index(name='visitas_estado')
# Conta total de visitas por cliente
df_total = df_states.groupby('EMAIL_CLIENTE').size().reset_index(name='total_visitas')
# Junta totais ao histórico
df_hist = df_hist.merge(df_total, on='EMAIL_CLIENTE', how='left')
# Calcula probabilidade histórica (visitas a um estado / total de visitas)
df_hist['prob_historica'] = df_hist['visitas_estado'] / df_hist['total_visitas']

# ----------------- 4) Merge histórico com eventos -----------------
# Combina histórico com os eventos
df_analise = df_hist.merge(df_events, left_on='Estado', right_on='Estado', how='right')
# Preenche NaN com zero (clientes que nunca visitaram)
df_analise[['visitas_estado', 'total_visitas', 'prob_historica']] = df_analise[
    ['visitas_estado', 'total_visitas', 'prob_historica']].fillna(0)

# ----------------- 5) Features temporais -----------------
# Última compra do cliente
df_last = df_clients.groupby("EMAIL_CLIENTE")['DATA_COMPRA'].max().reset_index()
# Merge com df_analise
df_analise = df_analise.merge(df_last, on="EMAIL_CLIENTE", how="left")
# Dias até o evento
df_analise['dias_para_evento'] = (df_analise['Data'] - df_analise['DATA_COMPRA']).dt.days

# Último estado do cliente (origem)
df_last_state = df_clients.groupby("EMAIL_CLIENTE").last().reset_index()
df_last_state = df_last_state[['EMAIL_CLIENTE', 'ESTADO_IDA_ORIGEM']].rename(
    columns={'ESTADO_IDA_ORIGEM': 'ESTADO_CLIENTE'})
df_analise = df_analise.merge(df_last_state, on="EMAIL_CLIENTE", how="left")
# Flag se cliente mora no estado do evento
df_analise['cliente_mora_no_estado_evento'] = (df_analise['ESTADO_CLIENTE'] == df_analise['Estado']).astype(int)

# Última viagem do cliente
df_last_trip = df_clients.groupby('EMAIL_CLIENTE').last().reset_index()
df_last_trip = df_last_trip[['EMAIL_CLIENTE', 'VALOR_TOTAL_PASSAGEM', 'QUANTIDADE_PASSAGENS']]
df_analise = df_analise.merge(df_last_trip, on='EMAIL_CLIENTE', how='left')

# Histórico agregado de compras
df_hist = df_clients.groupby("EMAIL_CLIENTE").agg(
    compras_historicas=('EMAIL_CLIENTE', 'count'),
    valor_medio_historico=('VALOR_TOTAL_PASSAGEM', 'mean')
).reset_index()
df_analise = df_analise.merge(df_hist, on="EMAIL_CLIENTE", how="left")

# ----------------- 6) Definição do target -----------------
# Target = 1 se cliente já visitou o estado pelo menos uma vez
df_analise['target'] = (df_analise['visitas_estado'] > 0).astype(int)

# ----------------- 7) Features do modelo -----------------
features = [
    'dias_para_evento',
    'cliente_mora_no_estado_evento',
    'VALOR_TOTAL_PASSAGEM',
    'QUANTIDADE_PASSAGENS',
    'compras_historicas',
    'valor_medio_historico'
]

# ----------------- 8) Treinamento do modelo -----------------
# Remove linhas com valores ausentes nas features
df_model = df_analise.dropna(subset=features)
X, y = df_model[features], df_model['target']

# Se houver apenas uma classe, não treina modelo
if y.nunique() < 2:
    print("Apenas uma classe detectada. Usando probabilidade histórica como fallback.")
    model = None
else:
    # Divisão treino/teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Inicializa Regressão Logística
    model = LogisticRegression(random_state=42, solver='liblinear')
    model.fit(X_train, y_train)  # Treina o modelo
    print(f"Acurácia do modelo: {accuracy_score(y_test, model.predict(X_test)):.2f}")

# ----------------- 9) Predição de probabilidades -----------------
if model is None:
    # Fallback para probabilidade histórica
    df_analise['probabilidade_evento'] = df_analise['prob_historica']
else:
    # Probabilidade prevista pelo modelo
    df_analise['probabilidade_evento'] = model.predict_proba(df_analise[features].fillna(0))[:, 1]

# ----------------- 10) Seleção final e exportação -----------------
# Seleciona colunas relevantes e ordena por probabilidade decrescente
df_final = df_analise[['EMAIL_CLIENTE', 'probabilidade_evento', 'Evento', 'Data', 'Cidade']].sort_values(
    'probabilidade_evento', ascending=False)
# Salva CSV
df_final.to_csv(OUTPUT_PATH, index=False)
print(f"Predictions saved to {OUTPUT_PATH}")
