-- ============================================================
-- DDL: Objetos do framework de qualidade de dados
-- Banco: PostgreSQL (AWS RDS)
-- Schema: qualidade_dados
-- Executar conectado com um usuário que tenha CREATE SCHEMA
-- e CREATE TABLE no banco de destino.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS qualidade_dados;

-- ------------------------------------------------------------
-- Domínio de dimensões de qualidade
-- ------------------------------------------------------------
CREATE TABLE qualidade_dados.dimensao (
    cod_dimensao  SERIAL       PRIMARY KEY,
    dsc_dimensao  VARCHAR(100) NOT NULL,
    CONSTRAINT uk_dimensao_dsc UNIQUE (dsc_dimensao)
);

INSERT INTO qualidade_dados.dimensao (dsc_dimensao) VALUES
    ('Consistency'),
    ('Completeness'),
    ('Uniqueness'),
    ('Conformity'),
    ('Accuracy');

-- ------------------------------------------------------------
-- Registro de execuções
-- ------------------------------------------------------------
CREATE TABLE qualidade_dados.execucao (
    seq_execucao    SERIAL       PRIMARY KEY,
    dsc_owner       VARCHAR(128),
    tip_fonte       VARCHAR(20),
    sta_execucao    VARCHAR(20)  NOT NULL,
    dat_execucao    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dat_finalizacao TIMESTAMP,
    dsc_observacao  TEXT,
    CONSTRAINT ck_execucao_sta CHECK (sta_execucao IN ('RUNNING', 'SUCCESS', 'ERROR'))
);

-- ------------------------------------------------------------
-- Scores de qualidade (MDDQ / DDQ / SCHEMA_SCORE por indicador)
-- ------------------------------------------------------------
CREATE TABLE qualidade_dados.resultado_qualidade (
    seq_resultado_qualidade SERIAL      PRIMARY KEY,
    seq_execucao            INTEGER     NOT NULL
        REFERENCES qualidade_dados.execucao(seq_execucao),
    cod_medida              VARCHAR(10),
    cod_dimensao            INTEGER
        REFERENCES qualidade_dados.dimensao(cod_dimensao),
    tip_resultado           VARCHAR(20),
    dsc_resultado           VARCHAR(400),
    num_peso                NUMERIC(10,4),
    num_valor               NUMERIC(10,4)
);

-- ------------------------------------------------------------
-- Problemas encontrados (CPF / CNPJ / CGF inválidos)
-- ------------------------------------------------------------
CREATE TABLE qualidade_dados.problema_execucao (
    seq_problema_execucao SERIAL      PRIMARY KEY,
    seq_execucao          INTEGER     NOT NULL
        REFERENCES qualidade_dados.execucao(seq_execucao),
    cod_medida            VARCHAR(10),
    cod_dimensao          INTEGER
        REFERENCES qualidade_dados.dimensao(cod_dimensao),
    dsc_problema          VARCHAR(200),
    dsc_owner             VARCHAR(128),
    nom_tabela            VARCHAR(128),
    nom_coluna            VARCHAR(128),
    dsc_valor             VARCHAR(30)
);
