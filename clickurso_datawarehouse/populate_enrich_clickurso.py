# ETL em Python: extrai CSV, transforma e carrega no MySQL AWS com dados sintéticos
import pandas as pd  # Manipulação de tabelas
from pathlib import Path  # Manipulação de caminhos
import mysql.connector  # Conexão MySQL
import uuid  # IDs únicos
from faker import Faker  # Geração de dados sintéticos
import random  # Operações aleatórias
import re  # Expressões regulares
import gc  # Coleta de memória


# Renomeia colunas do CSV para melhor legibilidade
column_rename_map = {
    'nk_ota_localizer_id': 'order_id',
    'fk_contact': 'id_cliente',
    'date_purchase': 'data_compra',
    'time_purchase': 'hora_compra',
    'place_origin_departure': 'id_localidade_ida_origem',
    'place_destination_departure': 'id_localidade_ida_destino',
    'place_origin_return': 'id_localidade_retorno_origem',
    'place_destination_return': 'id_localidade_retorno_destino',
    'fk_departure_ota_bus_company': 'id_viacao_ida',
    'fk_return_ota_bus_company': 'id_viacao_retorno',
    'gmv_success': 'valor_total_passagem',
    'total_tickets_quantity_success': 'quantidade_passagens',
}

# Csv disponibilizado pela ClickBus
raw_database = Path('resources/clickbus_database.csv')


def clickurso_database_connection():
    """
    Cria e retorna a conexão com o MySQL.
    """

    # Armazena conexão com MySQL
    conn = mysql.connector.connect(
        user='', #Apagado por segurança
        password='', #Apagado por segurança
        host='', #Apagado por segurança
        port=, #Apagado por segurança
        database='clickurso'
    )

    # Retorna conexão
    return conn


def clickurso_load_raw_data():
    """
    Carrega csv, renomeia colunas, converte tipos e retorna DataFrame pré-processado
    """
    print("Loading and processing the CSV file...")
    try:
        # Carrega o csv, lendo as colunas mapeadas
        raw_clickbus_data = pd.read_csv(raw_database, usecols=column_rename_map.keys())

        # Renomeia colunas do dataframe
        raw_clickbus_data.rename(columns=column_rename_map, inplace=True)

        # Converte para o formato datetime
        raw_clickbus_data['data_compra'] = pd.to_datetime(raw_clickbus_data['data_compra'], errors='coerce')
        raw_clickbus_data['data_compra'] = raw_clickbus_data["data_compra"].apply(lambda x: None if pd.isna(x) else x)

        print("CSV data processed successfully!")

        # Retorna o dataframe
        return raw_clickbus_data
    except FileNotFoundError:
        print(f"Error: File not found at '{raw_database}'.")
        exit()
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")
        exit()


def clickurso_insert_dimension_data(df, connection):
    """
    Extrai registros únicos e insere nas dimensões, evitando duplicatas
    """
    with connection.cursor() as cursor:
        # Inserção na dim_cliente
        print('Populating `dim_cliente` table...')

        # Seleciona apenas a coluna id_cliente, remove duplicatas e valores nulos
        dim_cliente_df = df[['id_cliente']].drop_duplicates().dropna()

        # Converte cada linha do DataFrame em tupla para inserção no banco
        rows_dim_cliente = [tuple(row) for row in dim_cliente_df.values]

        if rows_dim_cliente:
            cursor.executemany('INSERT INTO dim_cliente (id_cliente) VALUES (%s)', rows_dim_cliente)
            print(f'{cursor.rowcount} rows inserted into `dim_cliente`.')

        # Inserção na dim_localidade
        print('Populating `dim_localidade` table...')

        # Combina todas as colunas de localidade (ida e retorno) em uma única coluna, removendo duplicatas e nulos
        all_localidades = pd.concat([
            df[['id_localidade_ida_origem']].rename(columns={'id_localidade_ida_origem': 'id_localidade'}),
            df[['id_localidade_ida_destino']].rename(columns={'id_localidade_ida_destino': 'id_localidade'}),
            df[['id_localidade_retorno_origem']].rename(columns={'id_localidade_retorno_origem': 'id_localidade'}),
            df[['id_localidade_retorno_destino']].rename(columns={'id_localidade_retorno_destino': 'id_localidade'}),
        ]).drop_duplicates().dropna()

        # Converte cada linha do DataFrame em tupla para inserção no banco
        rows_dim_localidade = [tuple(row) for row in all_localidades.values]

        if rows_dim_localidade:
            cursor.executemany('INSERT INTO dim_localidade (id_localidade) VALUES (%s)', rows_dim_localidade)
            print(f'{cursor.rowcount} rows inserted into `dim_localidade`.')

        # Inserção na dim_viacao
        print('Populating `dim_viacao` table...')

        # Junta IDs de viações de ida e retorno em uma coluna única, remove duplicatas e nulos
        viacoes = pd.concat([
            df[['id_viacao_ida']].rename(columns={'id_viacao_ida': 'id_viacao'}),
            df[['id_viacao_retorno']].rename(columns={'id_viacao_retorno': 'id_viacao'}),
        ]).drop_duplicates().dropna()

        # Converte cada linha em tupla para inserção no banco
        rows_dim_viacao = [tuple(row) for row in viacoes.values]

        if rows_dim_viacao:
            cursor.executemany('INSERT INTO dim_viacao (id_viacao) VALUES (%s)', rows_dim_viacao)
            print(f'{cursor.rowcount} rows inserted into `dim_viacao`.')


def clickurso_insert_fact_data(df, connection):
    """
    Insere os dados da tabela de fatos 'fato_compra'.
    """
    with connection.cursor() as cursor:
        print('Populating `fato_compra` table...')

        # Gera um UUID para cada compra para garantir unicidade
        df['id_compra'] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Converte cada linha do DataFrame em tupla para inserção no banco
        rows_fato_compra = [
            (
                row['id_compra'], row['id_cliente'], row['id_localidade_ida_origem'],
                row['id_localidade_ida_destino'], row['id_viacao_ida'],
                row['id_localidade_retorno_origem'], row['id_localidade_retorno_destino'],
                row['id_viacao_retorno'], row['data_compra'], row['hora_compra'],
                row['valor_total_passagem'], row['quantidade_passagens'],
            )
            for _, row in df.iterrows()
        ]

        if rows_fato_compra:
            # Executa a inserção em massa performática
            cursor.executemany(
                'INSERT INTO fato_compra (id_compra, id_cliente, id_localidade_ida_origem, id_localidade_ida_destino, id_viacao_ida, id_localidade_retorno_origem, id_localidade_retorno_destino, id_viacao_retorno, data_compra, hora_compra, valor_total_passagem, quantidade_passagens) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                rows_fato_compra,
            )

            print(f'{cursor.rowcount} rows inserted into `fato_compra`.')


def clickurso_enrich_customer_data(connection):
    """
    Enriquece dim_cliente com dados sintéticos usando Faker, atualizando registros existentes
    """

    # Inicializa Faker para gerar dados em português (Brasil)
    faker = Faker("pt_BR")

    # # Lista de domínios para emails sintéticos
    domains = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "bol.com.br"]

    with connection.cursor() as cursor:
        # Busca todos os IDs de clientes da tabela dim_cliente e cria um DataFrame
        cursor.execute("SELECT id_cliente FROM dim_cliente")
        df_cliente = pd.DataFrame(cursor.fetchall(), columns=["id_cliente"])

        print(f"{len(df_cliente)} customers found for enrichment.")

        # Gera uma lista de nomes fictícios para cada cliente
        names = [faker.name() for _ in range(len(df_cliente))]

        # Cria emails sintéticos baseados nos nomes e domínios aleatórios
        emails = [
            f"{re.sub(r'[^a-z]', '', name.lower().replace(' ', '.'))}{random.randint(1, 99)}@{random.choice(domains)}"
            for name in names]

        # Gera datas de nascimento aleatórias entre 18 e 80 anos
        birth_dates = [faker.date_of_birth(minimum_age=18, maximum_age=80) for _ in range(len(df_cliente))]

        # Seleciona aleatoriamente o gênero do cliente
        genders = [random.choice(["M", "F", "Outro"]) for _ in range(len(df_cliente))]

        # Gera datas de cadastro aleatórias nos últimos 12 anos (Fundação da ClickBus)
        registration_dates = [faker.date_between(start_date="-12y", end_date="today") for _ in range(len(df_cliente))]

        # Atribui categorias aleatórias aos clientes (1 a 3)
        client_categories = [random.randint(1, 3) for _ in range(len(df_cliente))]

        # Combina todas as informações sintéticas com o ID do cliente em uma lista de tuplas para atualização
        data_to_update = list(zip(names, emails, birth_dates, genders, registration_dates, client_categories,
                                  df_cliente["id_cliente"]))

        # Define a query SQL para atualizar os dados sintéticos na tabela dim_cliente
        sql = """
        UPDATE dim_cliente 
        SET nome_cliente = %s, email_cliente = %s, data_nascimento = %s, genero = %s, data_cadastro = %s, telefone = %s, id_categoria = %s
        WHERE id_cliente = %s
        """

        # Executa a atualização em lote usando os dados gerados
        cursor.executemany(sql, data_to_update)

        # Confirma as alterações no banco de dados
        connection.commit()

        # Exibe quantos registros de clientes foram atualizados
        print(f"{cursor.rowcount} customer records updated.")


def clickurso_enrich_location_data(connection):
    """
    Enriquece `dim_localidade` com dados geográficos sintéticos usando CSVs de referência.
    """

    # Inicializa Faker com localidade pt_BR
    faker = Faker('pt_BR')

    # Lê CSVs de estados e municípios
    states_df = pd.read_csv('resources/estado.csv')
    municipalities_df = pd.read_csv('resources/municipios.csv')

    # Mapeia código do estado para sigla
    cod_uf_to_sigla = dict(zip(states_df['COD'], states_df['SIGLA']))

    # Cria lista de municípios com suas siglas
    municipalities_list = [
        (row['NOME'], cod_uf_to_sigla.get(row['COD UF'], 'Desconhecida'))
        for _, row in municipalities_df.iterrows()
    ]

    # Define estados por região
    regions_by_state = {
        'Norte': ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO'],
        'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
        'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
        'Sul': ['PR', 'RS', 'SC'],
    }

    # Função para retornar região a partir da sigla do estado
    def get_region(uf):
        """Retorna a região de um estado."""
        for region, states in regions_by_state.items():
            if uf in states:
                return region
        return 'Desconhecida'

    # Inicia cursor para atualização de localidades
    with connection.cursor() as cursor:
        cursor.execute('SELECT id_localidade FROM dim_localidade')
        df_localidade = pd.DataFrame(cursor.fetchall(), columns=['id_localidade'])
        print(f'{len(df_localidade)} locations found for enrichment.')

        # Seleciona aleatoriamente cidades e estados para cada localidade
        cities_states = [random.choice(municipalities_list) for _ in range(len(df_localidade))]
        cities = [ce[0] for ce in cities_states]
        states = [ce[1] for ce in cities_states]
        regions = [get_region(uf) for uf in states]
        location_names = [faker.street_name() for _ in range(len(df_localidade))]

        # Prepara dados para atualização no banco
        data_to_update = list(zip(location_names, cities, states, regions, df_localidade['id_localidade']))

        # SQL de atualização
        sql = '''
        UPDATE dim_localidade
        SET nome_localidade = %s, cidade = %s, estado = %s, regiao = %s
        WHERE id_localidade = %s
        '''
        cursor.executemany(sql, data_to_update)
        connection.commit()
        print(f'{cursor.rowcount} location records updated.')


def clickurso_enrich_bus_company_data(connection):
    """
    Enriquece a tabela `dim_viacao` com nomes de empresas de ônibus sintéticos.
    """
    # Listas de palavras para gerar nomes de viações
    movement_words = ['Express', 'Rapid', 'Fly', 'Transit', 'Go', 'Line', 'Track', 'Horizon', 'Atlas', 'Nova']
    brazil_words = ['Brasil', 'Rio', 'Sol', 'Verde', 'Azul', 'Amazônia', 'Tropic', 'Samba', 'Leste', 'Norte']

    # Função para gerar nome de viação fake
    def generate_bus_company_name():
        """Gera um nome de viação fake."""
        return f"{random.choice(movement_words)} {random.choice(brazil_words)}"

    # Atualiza tabela dim_viacao com nomes gerados
    with connection.cursor() as cursor:
        cursor.execute('SELECT id_viacao FROM dim_viacao')
        df_viacao = pd.DataFrame(cursor.fetchall(), columns=['id_viacao'])
        print(f'{len(df_viacao)} bus companies found for enrichment.')

        # Gera nomes para cada viação
        df_viacao['nome_viacao'] = [generate_bus_company_name() for _ in range(len(df_viacao))]

        # Prepara dados para atualização
        data_to_update = list(zip(df_viacao['nome_viacao'], df_viacao['id_viacao']))

        # SQL de atualização
        sql = '''
        UPDATE dim_viacao
        SET nome_viacao = %s
        WHERE id_viacao = %s
        '''
        cursor.executemany(sql, data_to_update)
        connection.commit()
        print(f'{cursor.rowcount} bus company records updated.')


def main():
    """
    Função principal que orquestra o pipeline de ETL.
    """
    # Carrega e processa os dados do CSV
    df = clickurso_load_raw_data()

    try:
        with clickurso_database_connection() as connection:
            print("Database connection successful!")

            # --- Parte 1: Inserção dos Dados Principais ---
            # Insere os dados das tabelas de dimensão e fato
            clickurso_insert_dimension_data(df, connection)
            clickurso_insert_fact_data(df, connection)
            connection.commit()
            print("Data loading completed and transactions committed.")

            # --- Parte 2: Enriquecimento das Dimensões ---
            print("\n--- Starting data enrichment process ---")
            # Enriquecimento da dimensão de clientes
            clickurso_enrich_customer_data(connection)
            # Enriquecimento da dimensão de localidades
            clickurso_enrich_location_data(connection)
            # Enriquecimento da dimensão de viações
            clickurso_enrich_bus_company_data(connection)

            print("\nData loading and enrichment process finished!")

    except mysql.connector.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    # Coleta de lixo manual para liberar a memória do DataFrame
    del df
    gc.collect()


if __name__ == "__main__":
    main()

