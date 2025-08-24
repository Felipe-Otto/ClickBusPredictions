-- DDL (Data Definition Language)
-- Cria o banco de dados 'clickurso' se ele ainda não existir
CREATE DATABASE IF NOT EXISTS clickurso;

-- Seleciona o banco de dados 'clickurso' para uso
USE clickurso;

-- Define o engine de armazenamento para InnoDB, que suporta chaves estrangeiras e transações
SET default_storage_engine = InnoDB;

-- Tabela de Dimensão: dim_cliente
CREATE TABLE dim_cliente (
    id_cliente VARCHAR(64) PRIMARY KEY,
    nome_cliente VARCHAR(150),
    email_cliente VARCHAR(150),
    data_cadastro DATE,
    data_nascimento DATE,
    genero VARCHAR(20),
    telefone VARCHAR(20),
    id_categoria INT
);

-- Tabela de Dimensão: dim_localidade
CREATE TABLE dim_localidade (
    id_localidade VARCHAR(64) PRIMARY KEY,
    nome_localidade VARCHAR(200),
    cidade VARCHAR(150),
    estado VARCHAR(2),
    regiao VARCHAR(20)
);

-- Tabela de Dimensão: dim_viacao
CREATE TABLE dim_viacao (
    id_viacao VARCHAR(64) PRIMARY KEY,
    nome_viacao VARCHAR(200)
);

-- Tabela de Dimensão: dim_categoria_cliente
CREATE TABLE dim_categoria_cliente (
    id_categoria INT PRIMARY KEY,
    nome_categoria VARCHAR(50),
    descricao VARCHAR(200)
);

-- Tabela de Fatos: fato_compra
CREATE TABLE fato_compra (
    id_compra VARCHAR(64) PRIMARY KEY,
    id_cliente VARCHAR(64),
    id_localidade_ida_origem VARCHAR(64),
    id_localidade_ida_destino VARCHAR(64),
    id_viacao_ida VARCHAR(64),
    id_localidade_retorno_origem VARCHAR(64),
    id_localidade_retorno_destino VARCHAR(64),
    id_viacao_retorno VARCHAR(64),
    data_compra DATE,
    hora_compra VARCHAR(20),
    valor_total_passagem DECIMAL(10, 2),
    quantidade_passagens INT,
    
    -- Definição das Chaves Estrangeiras (Foreign Keys)
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

-- Adiciona a chave estrangeira à tabela dim_cliente
ALTER TABLE dim_cliente
    ADD CONSTRAINT fk_dimcliente_categoria
    FOREIGN KEY (id_categoria)
    REFERENCES dim_categoria_cliente(id_categoria);


-- DML (Data Manipulation Language) e PROCEDURES
-- Inserção inicial de dados na tabela de categorias de clientes.
INSERT INTO dim_categoria_cliente (id_categoria, nome_categoria, descricao)
VALUES (1, 'Novo', 'Cliente realizou poucas compras, início de relacionamento.'),
       (2, 'Recorrente', 'Cliente com compras frequentes nos últimos meses.'),
       (3, 'VIP', 'Cliente com alto volume de compras ou ticket médio elevado.');

-- Stored Procedure para Atualizar a Categoria dos Clientes
DELIMITER //

CREATE PROCEDURE atualiza_categoria_clientes()
BEGIN
    DECLARE v_id_cliente VARCHAR(64);
    DECLARE v_qtd_compras INT;
    DECLARE done INT DEFAULT FALSE;
    
    -- Cursor para iterar sobre os clientes e a contagem de suas compras
    DECLARE c_clientes CURSOR FOR
        SELECT id_cliente,
               COUNT(*) AS qtd_compras
        FROM fato_compra
        GROUP BY id_cliente;
    
    -- Define um manipulador para o final do cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN c_clientes;
    
    read_loop: LOOP
        FETCH c_clientes INTO v_id_cliente, v_qtd_compras;
        
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Lógica de classificação
        IF v_qtd_compras <= 10 THEN
            UPDATE dim_cliente SET id_categoria = 1 WHERE id_cliente = v_id_cliente;
        ELSEIF v_qtd_compras BETWEEN 11 AND 100 THEN
            UPDATE dim_cliente SET id_categoria = 2 WHERE id_cliente = v_id_cliente;
        ELSE
            UPDATE dim_cliente SET id_categoria = 3 WHERE id_cliente = v_id_cliente;
        END IF;
    END LOOP;

    CLOSE c_clientes;
END//

DELIMITER ;

-- Habilita o scheduler de eventos no MySQL
SET GLOBAL event_scheduler = ON;

-- Cria um evento que executa a procedure a cada 24 horas
CREATE EVENT event_atualiza_categoria_clientes
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_TIMESTAMP
DO
    CALL atualiza_categoria_clientes();

-- Otimização de Performance: Criação de Índices
CREATE INDEX idx_fato_compra_cliente ON fato_compra (id_cliente);
CREATE INDEX idx_fato_compra_ida ON fato_compra (id_localidade_ida_origem, id_localidade_ida_destino, id_viacao_ida);
CREATE INDEX idx_fato_compra_retorno ON fato_compra (id_localidade_retorno_origem, id_localidade_retorno_destino, id_viacao_retorno);

CREATE INDEX idx_dim_cliente_categoria ON dim_cliente (id_categoria);
