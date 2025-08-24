"""
Este script tem como objetivo carregar dados de um CSV para um banco de dados Oracle,
seguindo um modelo de dados específico (Dimensional Model). Ele também enriquece
as tabelas de dimensão (clientes, localidades, viações) com informações adicionais
geradas de forma sintética (nomes, e-mails, etc.).
"""

import pandas as pd
from pathlib import Path
import oracledb
import uuid
from faker import Faker
import random
import re

# Configuração de acesso ao banco de dados Oracle
# ----------------------
DB_USER = "XXXXXXXX"
DB_PASSWORD = "YYYYYYY"
DB_DSN = "oracle.fiap.com.br:1521/ORCL"


# --- Parte 1: Inserção dos Dados Principais ---

print("Iniciando o processo de carga e enriquecimento de dados...")
print("----------------------------------------------------------")

# Arquivo de entrada e mapeamento de colunas
# ------------------------------------------
INPUT_FILE = Path("resources/clickbus_database.csv")
# Mapeia os nomes das colunas do CSV para os nomes das colunas do banco de dados,
# seguindo a modelagem "fato_compra".
COLUMN_RENAME_MAPPING = {
    "nk_ota_localizer_id": "order_id",
    "fk_contact": "id_cliente",
    "date_purchase": "data_compra",
    "time_purchase": "hora_compra",
    "place_origin_departure": "id_localidade_ida_origem",
    "place_destination_departure": "id_localidade_ida_destino",
    "place_origin_return": "id_localidade_retorno_origem",
    "place_destination_return": "id_localidade_retorno_destino",
    "fk_departure_ota_bus_company": "id_viacao_ida",
    "fk_return_ota_bus_company": "id_viacao_retorno",
    "gmv_success": "valor_total_passagem",
    "total_tickets_quantity_success": "quantidade_passagens",
}

# Carrega e prepara os dados do CSV
# ---------------------------------
print("Carregando e processando o arquivo CSV...")
try:
    # Carrega apenas as colunas necessárias para o DataFrame
    df = pd.read_csv(INPUT_FILE, usecols=COLUMN_RENAME_MAPPING.keys())

    # Renomeia as colunas para o padrão do banco de dados
    df.rename(columns=COLUMN_RENAME_MAPPING, inplace=True)

    # Converte a coluna de data para o tipo `datetime`, tratando erros
    df["data_compra"] = pd.to_datetime(df["data_compra"], errors="coerce")
    df["data_compra"] = df["data_compra"].apply(
        lambda x: None if pd.isna(x) else x
    )
    print("Dados do CSV processados com sucesso!")
except FileNotFoundError:
    print(f"Erro: Arquivo não encontrado em '{INPUT_FILE}'. Por favor, verifique o caminho.")
    exit()
except Exception as e:
    print(f"Ocorreu um erro ao processar o arquivo CSV: {e}")
    exit()


# Conexão e Inserção no Banco de Dados
# ------------------------------------
print("Iniciando conexão com o banco de dados...")
try:
    with oracledb.connect(
        user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN
    ) as connection:
        with connection.cursor() as cursor:
            print("Conexão bem-sucedida!")

            # Inserção na dim_cliente
            # -----------------------
            print("Populando a tabela `dim_cliente`...")
            dim_cliente_df = df[["id_cliente"]].drop_duplicates().dropna()
            rows_dim_cliente = [tuple(row) for row in dim_cliente_df.values]
            if rows_dim_cliente:
                cursor.executemany(
                    "INSERT INTO dim_cliente (id_cliente) VALUES (:1)",
                    rows_dim_cliente,
                )
                print(f"{cursor.rowcount} linhas inseridas na `dim_cliente`.")
            else:
                print("Nenhum dado para inserir na `dim_cliente`.")

            # Inserção na dim_localidade
            # --------------------------
            print("Populando a tabela `dim_localidade`...")
            # Combina e seleciona IDs únicos de todas as colunas de localidade
            all_localidades = pd.concat(
                [
                    df[["id_localidade_ida_origem"]].rename(columns={"id_localidade_ida_origem": "id_localidade"}),
                    df[["id_localidade_ida_destino"]].rename(columns={"id_localidade_ida_destino": "id_localidade"}),
                    df[["id_localidade_retorno_origem"]].rename(columns={"id_localidade_retorno_origem": "id_localidade"}),
                    df[["id_localidade_retorno_destino"]].rename(columns={"id_localidade_retorno_destino": "id_localidade"}),
                ]
            ).drop_duplicates().dropna()
            rows_dim_localidade = [tuple(row) for row in all_localidades.values]
            if rows_dim_localidade:
                cursor.executemany(
                    "INSERT INTO dim_localidade (id_localidade) VALUES (:1)",
                    rows_dim_localidade,
                )
                print(f"{cursor.rowcount} linhas inseridas na `dim_localidade`.")
            else:
                print("Nenhum dado para inserir na `dim_localidade`.")

            # Inserção na dim_viacao
            # ----------------------
            print("Populando a tabela `dim_viacao`...")
            # Combina e seleciona IDs únicos de viação
            viacoes = pd.concat(
                [
                    df[["id_viacao_ida"]].rename(columns={"id_viacao_ida": "id_viacao"}),
                    df[["id_viacao_retorno"]].rename(columns={"id_viacao_retorno": "id_viacao"}),
                ]
            ).drop_duplicates().dropna()
            rows_dim_viacao = [tuple(row) for row in viacoes.values]
            if rows_dim_viacao:
                cursor.executemany(
                    "INSERT INTO dim_viacao (id_viacao) VALUES (:1)",
                    rows_dim_viacao,
                )
                print(f"{cursor.rowcount} linhas inseridas na `dim_viacao`.")
            else:
                print("Nenhum dado para inserir na `dim_viacao`.")

            # Inserção na fato_compra
            # -----------------------
            print("Populando a tabela `fato_compra`...")
            # Gera um UUID (identificador único universal) para cada compra
            df["id_compra"] = [str(uuid.uuid4()) for _ in range(len(df))]
            # Prepara a lista de tuplas para a inserção em massa
            rows_fato_compra = [
                (
                    row["id_compra"],
                    row["id_cliente"],
                    row["id_localidade_ida_origem"],
                    row["id_localidade_ida_destino"],
                    row["id_viacao_ida"],
                    row["id_localidade_retorno_origem"],
                    row["id_localidade_retorno_destino"],
                    row["id_viacao_retorno"],
                    row["data_compra"],
                    row["hora_compra"],
                    row["valor_total_passagem"],
                    r"quantidade_passagens",
                )
                for _, row in df.iterrows()
            ]
            if rows_fato_compra:
                cursor.executemany(
                    "INSERT INTO fato_compra (id_compra, id_cliente, id_localidade_ida_origem, id_localidade_ida_destino, id_viacao_ida, id_localidade_retorno_origem, id_localidade_retorno_destino, id_viacao_retorno, data_compra, hora_compra, valor_total_passagem, quantidade_passagens) "
                    "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)",
                    rows_fato_compra,
                )
                print(f"{cursor.rowcount} linhas inseridas na `fato_compra`.")
            else:
                print("Nenhum dado para inserir na `fato_compra`.")

            # Confirma as transações
            connection.commit()
            print("Carga de dados concluída e transação confirmada.")

except oracledb.Error as e:
    error_obj, = e.args
    print(f"Erro no banco de dados: {error_obj.message}")
except Exception as e:
    print(f"Erro inesperado: {e}")


# --- Parte 2: Enriquecimento das Tabelas de Dimensão ---

print("\n--- Iniciando o processo de enriquecimento das dimensões ---")

# Enriquecimento da tabela `dim_cliente`
# --------------------------------------
print("\nEnriquecendo a tabela `dim_cliente` com dados sintéticos...")
faker = Faker("pt_BR")
domains = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "bol.com.br"]

try:
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT ID_CLIENTE FROM DIM_CLIENTE")
        rows = cursor.fetchall()
        df_cliente = pd.DataFrame(rows, columns=["ID_CLIENTE"])
        print(f"{len(df_cliente)} clientes encontrados para enriquecimento.")

        # Gera dados fake para cada cliente
        names = [faker.name() for _ in range(len(df_cliente))]
        emails = []
        for name in names:
            base = re.sub(r"[^a-z]", "", name.lower().replace(" ", "."))
            domain = random.choice(domains)
            email = f"{base}{random.randint(1,99)}@{domain}"
            emails.append(email)
        birth_dates = [faker.date_of_birth(minimum_age=18, maximum_age=80) for _ in range(len(df_cliente))]
        genders = [random.choice(["M", "F", "Outro"]) for _ in range(len(df_cliente))]
        registration_dates = [faker.date_between(start_date="-12y", end_date="today") for _ in range(len(df_cliente))]
        phones = [faker.phone_number() for _ in range(len(df_cliente))]
        # Gera uma categoria de cliente aleatória (ex: Bronze, Prata, Ouro)
        client_categories = [random.randint(1, 5) for _ in range(len(df_cliente))]

        # Prepara os dados para a atualização
        data_to_update = list(
            zip(
                names, emails, birth_dates, genders, registration_dates, phones, client_categories, df_cliente["ID_CLIENTE"],
            )
        )

        # Atualiza a tabela com os novos dados
        sql = """
        UPDATE DIM_CLIENTE 
        SET NOME_CLIENTE = :1, EMAIL_CLIENTE = :2, DATA_NASCIMENTO = :3, GENERO = :4, DATA_CADASTRO = :5, TELEFONE = :6, ID_CATEGORIA = :7
        WHERE ID_CLIENTE = :8
        """
        cursor.executemany(sql, data_to_update)
        connection.commit()
        print(f"{cursor.rowcount} registros de cliente atualizados.")

except oracledb.Error as e:
    error_obj, = e.args
    print(f"Erro no banco de dados durante o enriquecimento de clientes: {error_obj.message}")
except Exception as e:
    print(f"Erro inesperado durante o enriquecimento de clientes: {e}")


# Enriquecimento da tabela `dim_localidade`
# ----------------------------------------
print("\nEnriquecendo a tabela `dim_localidade` com informações geográficas...")
faker = Faker("pt_BR")
states_df = pd.read_csv("resources/estado.csv")
municipalities_df = pd.read_csv("resources/municipios.csv")
cod_uf_to_sigla = dict(zip(states_df["COD"], states_df["SIGLA"]))
municipalities_list = [
    (row["NOME"], cod_uf_to_sigla.get(row["COD UF"], "Desconhecida"))
    for _, row in municipalities_df.iterrows()
]
regions_by_state = {
    "Norte": ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}

def get_region(uf):
    for region, states in regions_by_state.items():
        if uf in states:
            return region
    return "Desconhecida"

try:
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT ID_LOCALIDADE FROM DIM_LOCALIDADE")
        rows = cursor.fetchall()
        df_localidade = pd.DataFrame(rows, columns=["ID_LOCALIDADE"])
        print(f"{len(df_localidade)} localidades encontradas para enriquecimento.")

        # Gera dados de localização fake
        cities_states = [random.choice(municipalities_list) for _ in range(len(df_localidade))]
        cities = [ce[0] for ce in cities_states]
        states = [ce[1] for ce in cities_states]
        regions = [get_region(uf) for uf in states]
        location_names = [faker.street_name() for _ in range(len(df_localidade))]

        # Adiciona os dados ao DataFrame temporário
        df_localidade["NOME_LOCALIDADE"] = location_names
        df_localidade["CIDADE"] = cities
        df_localidade["ESTADO"] = states
        df_localidade["REGIAO"] = regions

        # Atualiza a tabela `dim_localidade`
        for _, row in df_localidade.iterrows():
            cursor.execute(
                """
                UPDATE DIM_LOCALIDADE
                SET NOME_LOCALIDADE = :nome, CIDADE = :cidade, ESTADO = :estado, REGIAO = :regiao
                WHERE ID_LOCALIDADE = :id_localidade
            """,
                nome=row["NOME_LOCALIDADE"], cidade=row["CIDADE"], estado=row["ESTADO"],
                regiao=row["REGIAO"], id_localidade=row["ID_LOCALIDADE"],
            )
        connection.commit()
        print(f"{len(df_localidade)} registros de localidade atualizados.")

except oracledb.Error as e:
    error_obj, = e.args
    print(f"Erro no banco de dados durante o enriquecimento de localidades: {error_obj.message}")
except Exception as e:
    print(f"Erro inesperado durante o enriquecimento de localidades: {e}")


# Enriquecimento da tabela `dim_viacao`
# ------------------------------------
print("\nEnriquecendo a tabela `dim_viacao` com nomes de empresa...")
faker = Faker("pt_BR")
movement_words = ["Express", "Rapid", "Fly", "Transit", "Go", "Line", "Track", "Horizon", "Atlas", "Nova"]
brazil_words = ["Brasil", "Rio", "Sol", "Verde", "Azul", "Amazônia", "Tropic", "Samba", "Leste", "Norte"]

def generate_bus_company_name():
    return f"{random.choice(movement_words)} {random.choice(brazil_words)}"

try:
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT ID_VIACAO FROM DIM_VIACAO")
        rows = cursor.fetchall()
        df_viacao = pd.DataFrame(rows, columns=["ID_VIACAO"])
        print(f"{len(df_viacao)} viações encontradas para enriquecimento.")

        # Gera nomes de viação fake
        df_viacao["NOME_VIACAO"] = [generate_bus_company_name() for _ in range(len(df_viacao))]

        # Atualiza a tabela `dim_viacao`
        for _, row in df_viacao.iterrows():
            cursor.execute(
                """
                UPDATE DIM_VIACAO
                SET NOME_VIACAO = :nome
                WHERE ID_VIACAO = :id_viacao
            """,
                nome=row["NOME_VIACAO"], id_viacao=row["ID_VIACAO"],
            )
        connection.commit()
        print(f"{len(df_viacao)} registros de viação atualizados.")

except oracledb.Error as e:
    error_obj, = e.args
    print(f"Erro no banco de dados durante o enriquecimento de viações: {error_obj.message}")
except Exception as e:
    print(f"Erro inesperado durante o enriquecimento de viações: {e}")


print("\nProcesso de carga e enriquecimento de dados finalizado!")