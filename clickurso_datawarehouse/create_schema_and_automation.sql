-- ######################################################################
-- # DDL (Data Definition Language) - Cria��o das Tabelas do Data Warehouse
-- ######################################################################

-- ======================================================================
-- Tabela de Dimens�o: dim_cliente
-- Armazena informa��es sobre os clientes.
-- ======================================================================
CREATE TABLE dim_cliente (
    -- ID do cliente, chave prim�ria da tabela.
    id_cliente VARCHAR2(64) PRIMARY KEY,
    -- Nome completo do cliente.
    nome_cliente VARCHAR2(150),
    -- Endere�o de e-mail do cliente.
    email_cliente VARCHAR2(150),
    -- Data de cadastro do cliente no sistema.
    data_cadastro DATE,
    -- Data de nascimento do cliente.
    data_nascimento DATE,
    -- G�nero do cliente.
    genero VARCHAR2(20),
    -- Telefone de contato do cliente.
    telefone VARCHAR2(20),
    -- Chave estrangeira para a tabela de categoria do cliente.
    id_categoria NUMBER(10)
);

-- ======================================================================
-- Tabela de Dimens�o: dim_localidade
-- Armazena informa��es sobre as localidades de origem e destino.
-- ======================================================================
CREATE TABLE dim_localidade (
    -- ID da localidade, chave prim�ria da tabela.
    id_localidade VARCHAR2(64) PRIMARY KEY,
    -- Nome da localidade (ex: Aeroporto de Congonhas).
    nome_localidade VARCHAR2(200),
    -- Cidade da localidade.
    cidade VARCHAR2(150),
    -- Sigla do estado (UF) da localidade.
    estado VARCHAR2(2),
    -- Regi�o do estado (ex: Sudeste, Sul).
    regiao VARCHAR2(20)
);

-- ======================================================================
-- Tabela de Dimens�o: dim_viacao
-- Armazena informa��es sobre as companhias de �nibus.
-- ======================================================================
CREATE TABLE dim_viacao (
    -- ID da via��o, chave prim�ria da tabela.
    id_viacao VARCHAR2(64) PRIMARY KEY,
    -- Nome completo da via��o (ex: Via��o Cometa).
    nome_viacao VARCHAR2(200)
);

-- ======================================================================
-- Tabela de Dimens�o: dim_categoria_cliente
-- Tabela de apoio para categorizar clientes (ex: 'Novo', 'Recorrente').
-- ======================================================================
CREATE TABLE dim_categoria_cliente (
    -- ID da categoria, chave prim�ria.
    id_categoria NUMBER(10) PRIMARY KEY,
    -- Nome da categoria.
    nome_categoria VARCHAR2(50),
    -- Descri��o detalhada da categoria.
    descricao VARCHAR2(200)
);

-- ======================================================================
-- Tabela de Fatos: fato_compra
-- Armazena as transa��es de compra de passagens.
-- ======================================================================
CREATE TABLE fato_compra (
    -- ID �nico de cada compra, chave prim�ria.
    id_compra VARCHAR2(64) PRIMARY KEY,
    -- Chave estrangeira para a dimens�o de cliente.
    id_cliente VARCHAR2(64),
    -- Chaves estrangeiras para a dimens�o de localidade (viagem de ida).
    id_localidade_ida_origem VARCHAR2(64),
    id_localidade_ida_destino VARCHAR2(64),
    -- Chave estrangeira para a dimens�o de via��o (viagem de ida).
    id_viacao_ida VARCHAR2(64),
    -- Chaves estrangeiras para a dimens�o de localidade (viagem de retorno).
    id_localidade_retorno_origem VARCHAR2(64),
    id_localidade_retorno_destino VARCHAR2(64),
    -- Chave estrangeira para a dimens�o de via��o (viagem de retorno).
    id_viacao_retorno VARCHAR2(64),
    -- Data e hora da compra.
    data_compra DATE,
    hora_compra VARCHAR2(20),
    -- Valor total da passagem. Usado para m�tricas.
    valor_total_passagem NUMBER(10, 2),
    -- Quantidade de passagens compradas na transa��o.
    quantidade_passagens NUMBER,

    -- ######################################################################
    -- # Defini��o das Chaves Estrangeiras (Foreign Keys)
    -- ######################################################################
    CONSTRAINT fk_compra_cliente
        FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),

    CONSTRAINT fk_ida_localidade_origem
        FOREIGN KEY (id_localidade_ida_origem) REFERENCES dim_localidade(id_localidade),

    CONSTRAINT fk_ida_localidade_destino
        FOREIGN KEY (id_localidade_ida_destino) REFERENCES dim_localidade(id_localidade),

    CONSTRAINT fk_ida_viacao
        FOREIGN KEY (id_viacao_ida) REFERENCES dim_viacao(id_viacao),

    CONSTRAINT fk_retorno_localidade_origem
        FOREIGN KEY (id_localidade_retorno_origem) REFERENCES dim_localidade(id_localidade),

    CONSTRAINT fk_retorno_localidade_destino
        FOREIGN KEY (id_localidade_retorno_destino) REFERENCES dim_localidade(id_localidade),

    CONSTRAINT fk_retorno_viacao
        FOREIGN KEY (id_viacao_retorno) REFERENCES dim_viacao(id_viacao)
);

-- ======================================================================
-- DML (Data Manipulation Language) e PROCEDURES
-- ======================================================================

-- Insere os dados iniciais na tabela de categorias de clientes.
INSERT INTO DIM_CATEGORIA_CLIENTE (ID_CATEGORIA, NOME_CATEGORIA, DESCRICAO) VALUES (1, 'Novo', 'Cliente realizou poucas compras, in�cio de relacionamento.');
INSERT INTO DIM_CATEGORIA_CLIENTE (ID_CATEGORIA, NOME_CATEGORIA, DESCRICAO) VALUES (2, 'Recorrente', 'Cliente com compras frequentes nos �ltimos meses.');
INSERT INTO DIM_CATEGORIA_CLIENTE (ID_CATEGORIA, NOME_CATEGORIA, DESCRICAO) VALUES (3, 'VIP', 'Cliente com alto volume de compras ou ticket m�dio elevado.');

-- ======================================================================
-- PROCEDURE: atualiza_categoria_clientes
--
-- Esta procedure � executada ap�s a popula��o do banco e tem a fun��o
-- de atualizar a categoria de cada cliente com base no seu hist�rico
-- de compras na tabela de fatos.
-- ======================================================================
CREATE OR REPLACE PROCEDURE atualiza_categoria_clientes IS
    -- Cursor para iterar sobre os clientes e a contagem de suas compras.
    CURSOR c_clientes IS
        SELECT fv.id_cliente,
               COUNT(*) AS qtd_compras
        FROM fato_compra fv
        GROUP BY fv.id_cliente;

    v_id_cliente   dim_cliente.id_cliente%TYPE;
    v_qtd_compras  NUMBER;
    v_categoria    NUMBER;
BEGIN
    -- Abre o cursor para iniciar a itera��o.
    OPEN c_clientes;
    LOOP
        -- Busca o pr�ximo cliente e sua contagem de compras.
        FETCH c_clientes INTO v_id_cliente, v_qtd_compras;
        -- Sai do loop quando n�o houver mais registros.
        EXIT WHEN c_clientes%NOTFOUND;

        -- L�gica de classifica��o do cliente com base na quantidade de compras.
        IF v_qtd_compras <= 10 THEN
            v_categoria := 1; -- Novo
        ELSIF v_qtd_compras BETWEEN 11 AND 100 THEN
            v_categoria := 2; -- Recorrente
        ELSE
            v_categoria := 3; -- VIP
        END IF;

        -- Atualiza a categoria do cliente na tabela dim_cliente.
        UPDATE dim_cliente
            SET id_categoria = v_categoria
          WHERE id_cliente = v_id_cliente;
    END LOOP;
    -- Fecha o cursor.
    CLOSE c_clientes;

    -- Confirma as altera��es no banco de dados.
    COMMIT;
END;
/

-- Executa a procedure para realizar a atualiza��o inicial.
EXEC atualiza_categoria_clientes;


-- ######################################################################
-- # Otimiza��o de Performance: Cria��o de �ndices
-- ######################################################################
-- �ndices s�o criados para otimizar o desempenho das consultas,
-- especialmente em tabelas grandes como a de fatos.

-- �ndice na tabela FATO_COMPRA para as chaves estrangeiras mais usadas.
CREATE INDEX IDX_FATO_COMPRA_CLIENTE ON FATO_COMPRA (ID_CLIENTE);
CREATE INDEX IDX_FATO_COMPRA_VIAGEM_IDA ON FATO_COMPRA (ID_LOCALIDADE_IDA_ORIGEM, ID_LOCALIDADE_IDA_DESTINO, ID_VIACAO_IDA);
CREATE INDEX IDX_FATO_COMPRA_VIAGEM_RETORNO ON FATO_COMPRA (ID_LOCALIDADE_RETORNO_ORIGEM, ID_LOCALIDADE_RETORNO_DESTINO, ID_VIACAO_RETORNO);

-- �ndice na tabela DIM_CLIENTE para a chave estrangeira de categoria.
CREATE INDEX IDX_DIM_CLIENTE_CATEGORIA ON DIM_CLIENTE (ID_CATEGORIA);


-- ======================================================================
-- Automa��o de Tarefas Di�rias com DBMS_SCHEDULER
--
-- Este bloco cria um "Job" no Oracle para agendar a execu��o da procedure
-- 'atualiza_categoria_clientes' todos os dias � meia-noite. Isso garante
-- que as categorias dos clientes estejam sempre atualizadas.
-- ======================================================================
BEGIN
    -- Remove o job existente se ele j� tiver sido criado, para evitar erros.
    DBMS_SCHEDULER.DROP_JOB (
        job_name => 'JOB_ATUALIZA_CLIENTES_DIARIO',
        force => TRUE
    );
EXCEPTION
    -- Ignora o erro se o job n�o existir.
    WHEN OTHERS THEN
        NULL;
END;
/

-- Cria��o do job agendado.
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        -- Nome �nico para o job.
        job_name => 'JOB_ATUALIZA_CLIENTES_DIARIO',
        -- Tipo de a��o a ser executada: uma procedure armazenada.
        job_type => 'STORED_PROCEDURE',
        -- Nome da procedure que o job ir� executar.
        job_action => 'ATUALIZA_CATEGORIA_CLIENTES',
        -- Agendamento: 'frequ�ncia=DI�RIA'.
        repeat_interval => 'FREQ=DAILY; BYHOUR=0; BYMINUTE=0; BYSECOND=0;',
        -- Habilita o job para execu��o.
        enabled => TRUE,
        -- Adiciona um coment�rio descritivo para o job.
        comments => 'Job di�rio para atualizar a categoria dos clientes com base nas compras.'
    );
END;
/
