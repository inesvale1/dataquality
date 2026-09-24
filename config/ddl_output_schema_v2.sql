-- ============================================================
-- DDL: Esquema unico de saída para os 3 pipelines — QUALIDADE_DADOS (v2)
--
-- Evolui config/ddl_output_schema.sql (mantém DIMENSAO, TIPO_MEDIDA,
-- MEDIDA e RESULTADO_QUALIDADE) e incorpora o que já existia apenas
-- no banco (arquivo QUALIDADE_DADOS_OLD.sql, gerado por DBMS_METADATA
-- em 30/08/2026): MEDIDA, TIPO_MEDIDA e a antiga PARMS_EXECUCAO, aqui
-- generalizada e renomeada para CONFIGURACAO_EXECUCAO.
--
-- Diferença desta versão em relação à primeira proposta: os 3
-- pipelines (Catálogo Técnico, Glossário de Negócio, Qualidade de
-- Dados) passam a gravar num ÚNICO esquema QUALIDADE_DADOS, em vez de
-- um esquema por programa. EXECUCAO e CONFIGURACAO_EXECUCAO são
-- compartilhadas pelos três; o que diferencia uma execução do
-- Catálogo Técnico de uma do Glossário ou da Qualidade de Dados é a
-- coluna EXECUCAO.TIP_PROGRAMA. As tabelas de resultado de saída de
-- cada programa continuam sendo tabelas próprias (nomes não colidem),
-- todas dentro do mesmo esquema.
--
-- Novidades em relação ao OLD:
--   - CONFIGURACAO_EXECUCAO substitui PARMS_EXECUCAO (mesmo conceito,
--     + coluna TIP_VALOR), agora compartilhada pelos 3 programas.
--   - EXECUCAO ganha TIP_PROGRAMA e TIP_MODO (INTEGRADO/ISOLADO) e não
--     fixa mais um único DSC_OWNER: os schemas de negócio processados
--     (cadastro/receita2/sitram2, um ou vários) ficam registrados como
--     parâmetro de configuração, não como coluna da execução.
--   - PROBLEMA_EXECUCAO ganha colunas de sugestão de correção (Regras
--     ou IA), inspiradas em dq_tables_oracle_v2.sql (DQ_METADATA_ISSUES).
--   - Novas tabelas de resultado para os outros 2 programas:
--     CATALOGO_TABELA / CATALOGO_COLUNA / INDICADOR_CATALOGO
--     (Catálogo Técnico) e REPOSITORIO_PROCESSADO / TERMO_GLOSSARIO
--     (Glossário de Negócio) — eles hoje não gravam em banco algum.
--   - MEDIDA é populada com o catálogo real de indicadores hoje usado
--     em run_quality.config.json (scoring.metadata_metric_weights e
--     scoring.data_quality_metric_weights).
--
-- Convenção de prefixos (compartilhada pelos 3 pipelines):
--   COD_  Identificadores/códigos de domínio     NOM_  Nomes
--   DSC_  Descrições/textos curtos (<=4000)      TXT_  Textos longos
--   NUM_  Valores numéricos                      DAT_  Datas/timestamps
--   SIT_  Flags (CHAR(1) Y/N)                    TIP_  Tipos/categorias
--   STA_  Status                                 SEQ_  Chave substituta
-- Compatível com Oracle 11g (sem IDENTITY; sequences + app-side NEXTVAL).
-- ============================================================

-- ------------------------------------------------------------
-- 1. EXECUCAO
--    Uma linha por disparo de QUALQUER um dos 3 pipelines
--    (Integrado ou Isolado). TIP_PROGRAMA diz qual programa rodou;
--    os schemas de negócio processados e as fases habilitadas
--    (ex.: run_model_quality/run_data_quality) ficam registrados
--    em CONFIGURACAO_EXECUCAO, não como colunas fixas aqui — assim
--    a mesma tabela serve aos 3 programas sem colunas especificas
--    de um so deles.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_EXECUCAO
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.EXECUCAO (
    SEQ_EXECUCAO     NUMBER        NOT NULL,
    TIP_PROGRAMA     VARCHAR2(30)  NOT NULL,
    TIP_MODO         VARCHAR2(20)  NOT NULL,
    TIP_FONTE        VARCHAR2(20),
    STA_EXECUCAO     VARCHAR2(20)  NOT NULL,
    DAT_EXECUCAO     TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    DAT_FINALIZACAO  TIMESTAMP,
    DSC_OBSERVACAO   VARCHAR2(4000),
    CONSTRAINT PK_EXECUCAO PRIMARY KEY (SEQ_EXECUCAO),
    CONSTRAINT CK_EXECUCAO_PROGRAMA
        CHECK (TIP_PROGRAMA IN ('CATALOGO_TECNICO', 'GLOSSARIO_NEGOCIO', 'QUALIDADE_DADOS')),
    CONSTRAINT CK_EXECUCAO_MODO CHECK (TIP_MODO IN ('INTEGRADO', 'ISOLADO')),
    CONSTRAINT CK_EXECUCAO_FONTE CHECK (TIP_FONTE IN ('CSV', 'ORACLE', 'ATHENA', 'S3', 'GIT')),
    CONSTRAINT CK_EXECUCAO_STA CHECK (STA_EXECUCAO IN ('RUNNING', 'SUCCESS', 'SUCCESS_WARN', 'ERROR'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.EXECUCAO                 IS 'Registro de cada execucao dos 3 pipelines (Catalogo Tecnico, Glossario de Negocio, Qualidade de Dados).';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.SEQ_EXECUCAO     IS 'Identificador unico da execucao (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.TIP_PROGRAMA     IS 'Programa que gerou a execucao: CATALOGO_TECNICO, GLOSSARIO_NEGOCIO ou QUALIDADE_DADOS.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.TIP_MODO         IS 'Modo de disparo: INTEGRADO (em cadeia com os demais pipelines) ou ISOLADO.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.TIP_FONTE        IS 'Fonte de metadados: CSV, ORACLE, ATHENA, S3 ou GIT (Glossario de Negocio); pode ser nula quando nao se aplica.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.STA_EXECUCAO     IS 'Situacao da execucao: RUNNING, SUCCESS, SUCCESS_WARN ou ERROR.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.DAT_EXECUCAO     IS 'Data e hora de inicio da execucao.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.DAT_FINALIZACAO  IS 'Data e hora de finalizacao da execucao.';
COMMENT ON COLUMN QUALIDADE_DADOS.EXECUCAO.DSC_OBSERVACAO   IS 'Observacoes livres sobre a execucao.';

CREATE INDEX IDX_EXECUCAO_PROGRAMA ON QUALIDADE_DADOS.EXECUCAO (TIP_PROGRAMA, DAT_EXECUCAO);


-- ------------------------------------------------------------
-- 2. CONFIGURACAO_EXECUCAO
--    Substitui PARMS_EXECUCAO. Snapshot, em formato metadado
--    (variavel, valor), de TODAS as variaveis de configuracao
--    usadas em cada execucao — de qualquer um dos 3 pipelines,
--    incluindo grupos aninhados (scoring, validation_config,
--    llm_comment_generation, sources_repos.<schema>, ...) e listas
--    (schemas, delete_cols, plural_exceptions, ...), via
--    NUM_ITEM_PARAMETRO. NUNCA armazena segredo: apenas nomes de
--    servico/usuario de keyring, exatamente como o config.json ja faz.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_CONFIG_EXECUCAO
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO (
    SEQ_CONFIG_EXECUCAO  NUMBER        NOT NULL,
    SEQ_EXECUCAO         NUMBER        NOT NULL,
    NOM_GRUPO_PARAMETRO  VARCHAR2(100),
    NOM_PARAMETRO        VARCHAR2(200) NOT NULL,
    NUM_ITEM_PARAMETRO   NUMBER(4)     DEFAULT 1 NOT NULL,
    TIP_VALOR            VARCHAR2(20)  DEFAULT 'STRING' NOT NULL,
    DSC_VALOR_PARAMETRO  VARCHAR2(4000),
    CONSTRAINT PK_CONFIG_EXECUCAO PRIMARY KEY (SEQ_CONFIG_EXECUCAO),
    CONSTRAINT FK_CONFIG_EXECUCAO FOREIGN KEY (SEQ_EXECUCAO)
        REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT UK_CONFIG_EXECUCAO
        UNIQUE (SEQ_EXECUCAO, NOM_GRUPO_PARAMETRO, NOM_PARAMETRO, NUM_ITEM_PARAMETRO),
    CONSTRAINT CK_CONFIG_TIP_VALOR CHECK (TIP_VALOR IN ('STRING', 'NUMBER', 'BOOLEAN', 'JSON'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO IS 'Metadados (variavel, valor) da configuracao usada em cada execucao, dos 3 pipelines. Substitui PARMS_EXECUCAO.';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.SEQ_CONFIG_EXECUCAO IS 'Identificador unico do registro (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.SEQ_EXECUCAO        IS 'Execucao a que este parametro pertence.';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.NOM_GRUPO_PARAMETRO IS 'Secao/grupo do parametro no config.json do programa (ex.: scoring, sources_repos.cadastro, raiz = NULL).';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.NOM_PARAMETRO       IS 'Nome da variavel de configuracao (ex.: run_model_quality, db_host, schemas, git_url).';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.NUM_ITEM_PARAMETRO  IS 'Posicao do item quando o parametro e uma lista de valores (1..N).';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.TIP_VALOR           IS 'Tipo do valor original no JSON: STRING, NUMBER, BOOLEAN ou JSON (objeto/lista serializada).';
COMMENT ON COLUMN QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO.DSC_VALOR_PARAMETRO IS 'Valor do parametro em formato textual. Nunca contem segredo (apenas referencias de keyring).';

CREATE INDEX IDX_CONFIG_EXEC ON QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO (SEQ_EXECUCAO);

-- Exemplos de povoamento (uma execucao de cada programa):
--
-- Catalogo Tecnico (technicalcatalogpipeline/config/catalog.config.json):
-- INSERT INTO QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO, TIP_PROGRAMA, TIP_MODO, TIP_FONTE, STA_EXECUCAO)
--   VALUES (QUALIDADE_DADOS.SQ_EXECUCAO.NEXTVAL, 'CATALOGO_TECNICO', 'ISOLADO', 'CSV', 'RUNNING');
-- INSERT INTO QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO (SEQ_CONFIG_EXECUCAO, SEQ_EXECUCAO, NOM_GRUPO_PARAMETRO, NOM_PARAMETRO, NUM_ITEM_PARAMETRO, TIP_VALOR, DSC_VALOR_PARAMETRO)
--   VALUES (QUALIDADE_DADOS.SQ_CONFIG_EXECUCAO.NEXTVAL, :seq_execucao, NULL, 'schemas', 1, 'STRING', 'cadastro');
--
-- Glossario de Negocio (businessglossarypipeline/config/catalogo.config.json):
-- INSERT INTO QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO, TIP_PROGRAMA, TIP_MODO, TIP_FONTE, STA_EXECUCAO)
--   VALUES (QUALIDADE_DADOS.SQ_EXECUCAO.NEXTVAL, 'GLOSSARIO_NEGOCIO', 'INTEGRADO', 'GIT', 'RUNNING');
-- INSERT INTO QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO (SEQ_CONFIG_EXECUCAO, SEQ_EXECUCAO, NOM_GRUPO_PARAMETRO, NOM_PARAMETRO, NUM_ITEM_PARAMETRO, TIP_VALOR, DSC_VALOR_PARAMETRO)
--   VALUES (QUALIDADE_DADOS.SQ_CONFIG_EXECUCAO.NEXTVAL, :seq_execucao, 'sources_repos.cadastro', 'git_url', 1, 'STRING', 'https://git.sefaz.ce.gov.br/cadastro/cadastro-business.git');
--
-- Qualidade de Dados (dataquality/config/run_quality.config.json):
-- INSERT INTO QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO, TIP_PROGRAMA, TIP_MODO, TIP_FONTE, STA_EXECUCAO)
--   VALUES (QUALIDADE_DADOS.SQ_EXECUCAO.NEXTVAL, 'QUALIDADE_DADOS', 'INTEGRADO', 'CSV', 'RUNNING');
-- INSERT INTO QUALIDADE_DADOS.CONFIGURACAO_EXECUCAO (SEQ_CONFIG_EXECUCAO, SEQ_EXECUCAO, NOM_GRUPO_PARAMETRO, NOM_PARAMETRO, NUM_ITEM_PARAMETRO, TIP_VALOR, DSC_VALOR_PARAMETRO)
--   VALUES (QUALIDADE_DADOS.SQ_CONFIG_EXECUCAO.NEXTVAL, :seq_execucao, NULL, 'run_model_quality', 1, 'BOOLEAN', 'true');
-- ... NULL, 'run_data_quality', 1, 'BOOLEAN', 'true'
-- ... 'scoring', 'mddq_phase_weight', 1, 'NUMBER', '30'
-- ... 'scoring', 'ddq_phase_weight', 1, 'NUMBER', '70'
-- ... 'scoring.metadata_metric_weights', 'MQID005', 1, 'NUMBER', '10'
-- ... 'llm_comment_generation', 'model', 1, 'STRING', 'gpt-4o-mini'


-- ============================================================
-- Resultados de saída — CATÁLOGO TÉCNICO
-- ============================================================

-- ------------------------------------------------------------
-- 3. CATALOGO_TABELA — uma linha por tabela catalogada.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_CATALOGO_TABELA
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.CATALOGO_TABELA (
    SEQ_CATALOGO_TABELA  NUMBER        NOT NULL,
    SEQ_EXECUCAO         NUMBER        NOT NULL,
    DSC_OWNER            VARCHAR2(128) NOT NULL,
    NOM_TABELA           VARCHAR2(128) NOT NULL,
    TIP_ORIGEM           VARCHAR2(20)  NOT NULL,
    TXT_COMENTARIO       VARCHAR2(4000),
    NUM_COLUNAS          NUMBER(10),
    NUM_LINHAS           NUMBER(18),
    SIT_POSSUI_PK        CHAR(1)       DEFAULT 'N' NOT NULL,
    DAT_CATALOGACAO      TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_CATALOGO_TABELA PRIMARY KEY (SEQ_CATALOGO_TABELA),
    CONSTRAINT FK_CATTAB_EXECUCAO FOREIGN KEY (SEQ_EXECUCAO)
        REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT UK_CATALOGO_TABELA UNIQUE (SEQ_EXECUCAO, DSC_OWNER, NOM_TABELA),
    CONSTRAINT CK_CATTAB_ORIGEM CHECK (TIP_ORIGEM IN ('CSV', 'ORACLE', 'ATHENA', 'S3')),
    CONSTRAINT CK_CATTAB_PK CHECK (SIT_POSSUI_PK IN ('Y', 'N'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.CATALOGO_TABELA IS 'Tabelas catalogadas em cada execucao do pipeline de Catalogo Tecnico.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.SEQ_CATALOGO_TABELA IS 'Identificador unico do registro (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.SEQ_EXECUCAO        IS 'Execucao (TIP_PROGRAMA = CATALOGO_TECNICO) que catalogou esta tabela.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.DSC_OWNER           IS 'Schema de negocio catalogado (cadastro, receita2, sitram2, ...).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.NOM_TABELA          IS 'Nome da tabela catalogada.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.TIP_ORIGEM          IS 'Fonte de onde os metadados desta tabela foram lidos.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.TXT_COMENTARIO      IS 'Comentario de negocio registrado para a tabela.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.NUM_COLUNAS         IS 'Numero de colunas catalogadas para a tabela.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.NUM_LINHAS          IS 'Numero de linhas da tabela, quando disponivel na fonte.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.SIT_POSSUI_PK       IS 'Indica se a tabela possui chave primaria ou unica (Y/N).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_TABELA.DAT_CATALOGACAO     IS 'Data e hora em que a tabela foi catalogada.';

CREATE INDEX IDX_CATTAB_EXEC  ON QUALIDADE_DADOS.CATALOGO_TABELA (SEQ_EXECUCAO);
CREATE INDEX IDX_CATTAB_OWNER ON QUALIDADE_DADOS.CATALOGO_TABELA (DSC_OWNER, NOM_TABELA);


-- ------------------------------------------------------------
-- 4. CATALOGO_COLUNA — uma linha por coluna, filha de CATALOGO_TABELA.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_CATALOGO_COLUNA
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.CATALOGO_COLUNA (
    SEQ_CATALOGO_COLUNA  NUMBER        NOT NULL,
    SEQ_CATALOGO_TABELA  NUMBER        NOT NULL,
    NOM_COLUNA           VARCHAR2(128) NOT NULL,
    TIP_DADO             VARCHAR2(50),
    NUM_TAMANHO          NUMBER(10),
    NUM_ESCALA           NUMBER(10),
    SIT_NULLABLE         CHAR(1)       DEFAULT 'Y' NOT NULL,
    TXT_COMENTARIO       VARCHAR2(4000),
    SIT_IS_PK            CHAR(1)       DEFAULT 'N' NOT NULL,
    SIT_IS_FK            CHAR(1)       DEFAULT 'N' NOT NULL,
    SIT_IS_UNIQUE        CHAR(1)       DEFAULT 'N' NOT NULL,
    CONSTRAINT PK_CATALOGO_COLUNA PRIMARY KEY (SEQ_CATALOGO_COLUNA),
    CONSTRAINT FK_CATCOL_TABELA FOREIGN KEY (SEQ_CATALOGO_TABELA)
        REFERENCES QUALIDADE_DADOS.CATALOGO_TABELA (SEQ_CATALOGO_TABELA),
    CONSTRAINT UK_CATALOGO_COLUNA UNIQUE (SEQ_CATALOGO_TABELA, NOM_COLUNA),
    CONSTRAINT CK_CATCOL_NULLABLE CHECK (SIT_NULLABLE IN ('Y', 'N')),
    CONSTRAINT CK_CATCOL_PK CHECK (SIT_IS_PK IN ('Y', 'N')),
    CONSTRAINT CK_CATCOL_FK CHECK (SIT_IS_FK IN ('Y', 'N')),
    CONSTRAINT CK_CATCOL_UNIQUE CHECK (SIT_IS_UNIQUE IN ('Y', 'N'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.CATALOGO_COLUNA IS 'Colunas catalogadas de cada tabela, filhas de CATALOGO_TABELA.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SEQ_CATALOGO_COLUNA IS 'Identificador unico do registro (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SEQ_CATALOGO_TABELA IS 'Tabela a que esta coluna pertence.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.NOM_COLUNA          IS 'Nome da coluna catalogada.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.TIP_DADO            IS 'Tipo de dado de origem (ex.: VARCHAR2, NUMBER, DATE).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.NUM_TAMANHO         IS 'Tamanho/precisao definido para a coluna.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.NUM_ESCALA          IS 'Escala numerica da coluna.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SIT_NULLABLE        IS 'Indica se a coluna aceita nulos (Y/N).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.TXT_COMENTARIO      IS 'Comentario de negocio registrado para a coluna.';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SIT_IS_PK           IS 'Indica se a coluna participa de uma chave primaria (Y/N).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SIT_IS_FK           IS 'Indica se a coluna participa de uma chave estrangeira (Y/N).';
COMMENT ON COLUMN QUALIDADE_DADOS.CATALOGO_COLUNA.SIT_IS_UNIQUE       IS 'Indica se a coluna possui constraint UNIQUE (Y/N).';

CREATE INDEX IDX_CATCOL_TABELA ON QUALIDADE_DADOS.CATALOGO_COLUNA (SEQ_CATALOGO_TABELA);


-- ------------------------------------------------------------
-- 5. INDICADOR_CATALOGO — indicadores consolidados (completude,
--    cobertura, ...) por execucao e, opcionalmente, por schema.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_INDICADOR_CATALOGO
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.INDICADOR_CATALOGO (
    SEQ_INDICADOR_CATALOGO NUMBER        NOT NULL,
    SEQ_EXECUCAO           NUMBER        NOT NULL,
    DSC_OWNER              VARCHAR2(128),
    COD_INDICADOR          VARCHAR2(20)  NOT NULL,
    DSC_DESCRICAO          VARCHAR2(500),
    NUM_VALOR              NUMBER(10,4),
    CONSTRAINT PK_INDICADOR_CATALOGO PRIMARY KEY (SEQ_INDICADOR_CATALOGO),
    CONSTRAINT FK_INDCAT_EXECUCAO FOREIGN KEY (SEQ_EXECUCAO)
        REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT UK_INDICADOR_CATALOGO UNIQUE (SEQ_EXECUCAO, DSC_OWNER, COD_INDICADOR)
);

COMMENT ON TABLE  QUALIDADE_DADOS.INDICADOR_CATALOGO IS 'Indicadores consolidados do Catalogo Tecnico por execucao/schema (ex.: completude, cobertura).';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.SEQ_INDICADOR_CATALOGO IS 'Identificador unico do registro (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.SEQ_EXECUCAO           IS 'Execucao que gerou este indicador.';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.DSC_OWNER             IS 'Schema de negocio associado; NULL para indicador agregado da execucao inteira.';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.COD_INDICADOR         IS 'Codigo do indicador (ex.: CTID001 = completude de comentarios de tabela).';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.DSC_DESCRICAO         IS 'Descricao textual do indicador.';
COMMENT ON COLUMN QUALIDADE_DADOS.INDICADOR_CATALOGO.NUM_VALOR             IS 'Valor calculado do indicador.';

CREATE INDEX IDX_INDCAT_EXEC ON QUALIDADE_DADOS.INDICADOR_CATALOGO (SEQ_EXECUCAO);


-- ============================================================
-- Resultados de saída — GLOSSÁRIO DE NEGÓCIO
-- ============================================================

-- ------------------------------------------------------------
-- 6. REPOSITORIO_PROCESSADO — repositorios git lidos em cada execucao.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_REPOSITORIO_PROC
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.REPOSITORIO_PROCESSADO (
    SEQ_REPOSITORIO_PROC  NUMBER        NOT NULL,
    SEQ_EXECUCAO          NUMBER        NOT NULL,
    DSC_OWNER             VARCHAR2(128) NOT NULL,
    NOM_REPOSITORIO       VARCHAR2(300) NOT NULL,
    NOM_REF               VARCHAR2(100),
    TIP_CONTEUDO          VARCHAR2(20)  DEFAULT 'CODIGO' NOT NULL,
    NUM_ARQUIVOS_LIDOS    NUMBER(10),
    DAT_PROCESSAMENTO     TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_REPOSITORIO_PROC PRIMARY KEY (SEQ_REPOSITORIO_PROC),
    CONSTRAINT FK_REPOPROC_EXECUCAO FOREIGN KEY (SEQ_EXECUCAO)
        REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT CK_REPOPROC_TIPO CHECK (TIP_CONTEUDO IN ('CODIGO', 'DOCS'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.REPOSITORIO_PROCESSADO IS 'Repositorios git lidos em cada execucao do pipeline de Glossario de Negocio.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.SEQ_REPOSITORIO_PROC IS 'Identificador unico do registro (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.SEQ_EXECUCAO         IS 'Execucao (TIP_PROGRAMA = GLOSSARIO_NEGOCIO) que processou este repositorio.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.DSC_OWNER            IS 'Schema de negocio associado ao repositorio (cadastro, receita2, sitram2, ...).';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.NOM_REPOSITORIO      IS 'URL (ou nome curto) do repositorio git.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.NOM_REF              IS 'Branch ou tag lido do repositorio.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.TIP_CONTEUDO         IS 'Natureza do conteudo lido: CODIGO ou DOCS.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.NUM_ARQUIVOS_LIDOS   IS 'Numero de arquivos efetivamente lidos do repositorio nesta execucao.';
COMMENT ON COLUMN QUALIDADE_DADOS.REPOSITORIO_PROCESSADO.DAT_PROCESSAMENTO    IS 'Data e hora em que o repositorio foi processado.';

CREATE INDEX IDX_REPOPROC_EXEC ON QUALIDADE_DADOS.REPOSITORIO_PROCESSADO (SEQ_EXECUCAO);


-- ------------------------------------------------------------
-- 7. TERMO_GLOSSARIO — o glossario propriamente dito: termos
--    extraidos (IA ou manual), com fila de revisao humana. Um
--    termo persiste entre execucoes (chave unica por schema); uma
--    nova execucao atualiza descricao/data, sem resetar STA_REVISAO
--    se o termo ja foi revisado.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_TERMO_GLOSSARIO
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.TERMO_GLOSSARIO (
    SEQ_TERMO_GLOSSARIO  NUMBER        NOT NULL,
    SEQ_EXECUCAO         NUMBER        NOT NULL,
    DSC_OWNER            VARCHAR2(128) NOT NULL,
    NOM_TERMO            VARCHAR2(200) NOT NULL,
    TXT_DESCRICAO        VARCHAR2(4000),
    DSC_ORIGEM           VARCHAR2(300),
    TIP_GERACAO          VARCHAR2(20)  DEFAULT 'IA' NOT NULL,
    STA_REVISAO          VARCHAR2(20)  DEFAULT 'PENDENTE' NOT NULL,
    NOM_REVISOR          VARCHAR2(128),
    DAT_REVISAO          TIMESTAMP,
    DAT_GERACAO          TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_TERMO_GLOSSARIO PRIMARY KEY (SEQ_TERMO_GLOSSARIO),
    CONSTRAINT FK_TERMO_EXECUCAO FOREIGN KEY (SEQ_EXECUCAO)
        REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT UK_TERMO_GLOSSARIO UNIQUE (DSC_OWNER, NOM_TERMO),
    CONSTRAINT CK_TERMO_GERACAO CHECK (TIP_GERACAO IN ('IA', 'MANUAL')),
    CONSTRAINT CK_TERMO_REVISAO CHECK (STA_REVISAO IN ('PENDENTE', 'APROVADO', 'REJEITADO'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.TERMO_GLOSSARIO IS 'Termos do glossario de negocio, gerados por IA ou manualmente, com fila de revisao.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.SEQ_TERMO_GLOSSARIO IS 'Identificador unico do termo (PK substituta).';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.SEQ_EXECUCAO        IS 'Ultima execucao que gerou ou atualizou este termo.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.DSC_OWNER           IS 'Schema de negocio a que o termo pertence.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.NOM_TERMO           IS 'Nome do termo de negocio (ex.: DAE, CNPJ Alfanumerico).';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.TXT_DESCRICAO       IS 'Descricao do termo, gerada por IA ou escrita manualmente.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.DSC_ORIGEM          IS 'Repositorio/arquivo de onde o termo foi extraido.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.TIP_GERACAO         IS 'Origem do termo: IA ou MANUAL.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.STA_REVISAO         IS 'Situacao de revisao humana: PENDENTE, APROVADO ou REJEITADO.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.NOM_REVISOR         IS 'Identificacao de quem revisou o termo.';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.DAT_REVISAO         IS 'Data e hora da revisao (aprovacao ou rejeicao).';
COMMENT ON COLUMN QUALIDADE_DADOS.TERMO_GLOSSARIO.DAT_GERACAO         IS 'Data e hora em que o termo foi gerado/atualizado pela ultima vez.';

CREATE INDEX IDX_TERMO_EXEC   ON QUALIDADE_DADOS.TERMO_GLOSSARIO (SEQ_EXECUCAO);
CREATE INDEX IDX_TERMO_STATUS ON QUALIDADE_DADOS.TERMO_GLOSSARIO (STA_REVISAO);


-- ============================================================
-- Resultados de saída — QUALIDADE DE DADOS (domínio + resultados)
-- ============================================================

-- ------------------------------------------------------------
-- 8. DIMENSAO — domínio de dimensões de qualidade (inalterado)
-- ------------------------------------------------------------
CREATE TABLE QUALIDADE_DADOS.DIMENSAO (
    COD_DIMENSAO   NUMBER        NOT NULL,
    DSC_DIMENSAO   VARCHAR2(100) NOT NULL,
    CONSTRAINT PK_DIMENSAO PRIMARY KEY (COD_DIMENSAO),
    CONSTRAINT UK_DIMENSAO_DSC UNIQUE (DSC_DIMENSAO)
);

COMMENT ON TABLE  QUALIDADE_DADOS.DIMENSAO              IS 'Tabela de dominio das dimensoes de qualidade de dados.';
COMMENT ON COLUMN QUALIDADE_DADOS.DIMENSAO.COD_DIMENSAO IS 'Codigo identificador da dimensao de qualidade de dados.';
COMMENT ON COLUMN QUALIDADE_DADOS.DIMENSAO.DSC_DIMENSAO IS 'Descricao da dimensao de qualidade de dados.';

INSERT INTO QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO, DSC_DIMENSAO) VALUES (1, 'Consistency');
INSERT INTO QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO, DSC_DIMENSAO) VALUES (2, 'Completeness');
INSERT INTO QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO, DSC_DIMENSAO) VALUES (3, 'Uniqueness');
INSERT INTO QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO, DSC_DIMENSAO) VALUES (4, 'Conformity');
INSERT INTO QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO, DSC_DIMENSAO) VALUES (5, 'Accuracy');
COMMIT;


-- ------------------------------------------------------------
-- 9. TIPO_MEDIDA — domínio das categorias de medida (inalterado)
-- ------------------------------------------------------------
CREATE TABLE QUALIDADE_DADOS.TIPO_MEDIDA (
    TIP_MEDIDA       NUMBER(1,0)   NOT NULL,
    DSC_TIPO_MEDIDA  VARCHAR2(50)  NOT NULL,
    CONSTRAINT PK_TIPO_MEDIDA PRIMARY KEY (TIP_MEDIDA)
);

COMMENT ON TABLE  QUALIDADE_DADOS.TIPO_MEDIDA                 IS 'Tabela de dominio das categorias de medidas utilizadas pela tabela MEDIDA.';
COMMENT ON COLUMN QUALIDADE_DADOS.TIPO_MEDIDA.TIP_MEDIDA      IS 'Codigo identificador do tipo de medida.';
COMMENT ON COLUMN QUALIDADE_DADOS.TIPO_MEDIDA.DSC_TIPO_MEDIDA IS 'Descricao do tipo de medida.';

INSERT INTO QUALIDADE_DADOS.TIPO_MEDIDA (TIP_MEDIDA, DSC_TIPO_MEDIDA) VALUES (1, 'Medida bruta de metadados (MQME)');
INSERT INTO QUALIDADE_DADOS.TIPO_MEDIDA (TIP_MEDIDA, DSC_TIPO_MEDIDA) VALUES (2, 'Indicador de qualidade de metadados (MQID / MDDQ)');
INSERT INTO QUALIDADE_DADOS.TIPO_MEDIDA (TIP_MEDIDA, DSC_TIPO_MEDIDA) VALUES (3, 'Indicador de qualidade de dados (DDQ)');
INSERT INTO QUALIDADE_DADOS.TIPO_MEDIDA (TIP_MEDIDA, DSC_TIPO_MEDIDA) VALUES (4, 'Escore consolidado (fase ou schema)');
COMMIT;


-- ------------------------------------------------------------
-- 10. MEDIDA — catálogo das medidas/indicadores (inalterado na
--     estrutura; populado com o catálogo real de scoring.*)
-- ------------------------------------------------------------
CREATE TABLE QUALIDADE_DADOS.MEDIDA (
    COD_MEDIDA      VARCHAR2(10)  NOT NULL,
    TIP_MEDIDA      NUMBER(1,0)   NOT NULL,
    DSC_MEDIDA      VARCHAR2(200) NOT NULL,
    COD_DIMENSAO    NUMBER(1,0),
    DSC_OBSERVACAO  VARCHAR2(1000),
    CONSTRAINT PK_MEDIDA PRIMARY KEY (COD_MEDIDA),
    CONSTRAINT FK_DIM_MED FOREIGN KEY (COD_DIMENSAO) REFERENCES QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO),
    CONSTRAINT FK_TIP_MED_MED FOREIGN KEY (TIP_MEDIDA) REFERENCES QUALIDADE_DADOS.TIPO_MEDIDA (TIP_MEDIDA)
);

COMMENT ON TABLE  QUALIDADE_DADOS.MEDIDA                IS 'Catalogo das medidas, metricas, indicadores e escores finais calculados no processo de qualidade de dados.';
COMMENT ON COLUMN QUALIDADE_DADOS.MEDIDA.COD_MEDIDA     IS 'Codigo da medida, metrica, indicador ou escore final (ex.: MQID001).';
COMMENT ON COLUMN QUALIDADE_DADOS.MEDIDA.TIP_MEDIDA     IS 'Tipo da medida, referenciando TIPO_MEDIDA.';
COMMENT ON COLUMN QUALIDADE_DADOS.MEDIDA.DSC_MEDIDA     IS 'Descricao da medida, metrica, indicador ou escore final.';
COMMENT ON COLUMN QUALIDADE_DADOS.MEDIDA.COD_DIMENSAO   IS 'Codigo da dimensao de qualidade associada, quando aplicavel.';
COMMENT ON COLUMN QUALIDADE_DADOS.MEDIDA.DSC_OBSERVACAO IS 'Observacao adicional ou nota de calculo da medida.';

-- Catalogo MDDQ (metadata_metric_weights em run_quality.config.json)
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID001', 2, 'Table names in singular',                      1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID002', 2, 'Table with recommended name length',            1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID003', 2, 'Columns with correct prefixes',                  1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID004', 2, 'Columns with recommended name size',             1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID005', 2, 'Columns with comments',                          2, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID006', 2, 'Table with standard PK prefixes',                1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID007', 2, 'Table with standard FK prefixes',                1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID008', 2, 'Table with standard UK prefixes',                1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID009', 2, 'Columns with valid num_distinct',                3, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID010', 2, 'Columns with num_nulls',                         2, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID011', 2, 'Tables with at least one integrity constraint',  1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID012', 2, 'Identifier-like columns protected by PK/UK',     1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID013', 2, 'Compliance between type and naming',             1, NULL);
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID014', 2, 'Tables with comments',                           2, NULL);

-- Catalogo DDQ (data_quality_metric_weights em run_quality.config.json)
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('DQID001', 3, 'Format Conformity',                              4, 'Antes chamada apenas de "Format Conformity" no config; peso atual 0.');
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('DQID002', 3, 'Uniqueness (conteudo real dos dados)',           3, 'Distinta de MQID009, que avalia apenas metadados; peso atual 0.');
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('DQID003', 3, 'Redundancy detection',                           3, 'Peso atual 0.');
INSERT INTO QUALIDADE_DADOS.MEDIDA VALUES ('MQID015', 3, 'Validade de documentos (CPF/CNPJ/CGF)',          5, 'Unico indicador DDQ com peso > 0 na configuracao atual (100).');
COMMIT;


-- ------------------------------------------------------------
-- 11. RESULTADO_QUALIDADE — scores de qualidade (inalterado)
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_RESULTADO_QUALIDADE
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.RESULTADO_QUALIDADE (
    SEQ_RESULTADO_QUALIDADE  NUMBER        NOT NULL,
    SEQ_EXECUCAO             NUMBER        NOT NULL,
    DSC_OWNER                VARCHAR2(128),
    COD_MEDIDA               VARCHAR2(10),
    COD_DIMENSAO             NUMBER,
    TIP_RESULTADO            VARCHAR2(20)  NOT NULL,
    DSC_RESULTADO            VARCHAR2(400),
    NUM_PESO                 NUMBER(10,4),
    NUM_VALOR                NUMBER(10,4),
    CONSTRAINT PK_RESULTADO_QUALIDADE PRIMARY KEY (SEQ_RESULTADO_QUALIDADE),
    CONSTRAINT FK_RESULTADO_EXECUCAO  FOREIGN KEY (SEQ_EXECUCAO)  REFERENCES QUALIDADE_DADOS.EXECUCAO  (SEQ_EXECUCAO),
    CONSTRAINT FK_RESULTADO_DIMENSAO  FOREIGN KEY (COD_DIMENSAO)  REFERENCES QUALIDADE_DADOS.DIMENSAO  (COD_DIMENSAO),
    CONSTRAINT FK_RESULTADO_MEDIDA    FOREIGN KEY (COD_MEDIDA)    REFERENCES QUALIDADE_DADOS.MEDIDA    (COD_MEDIDA),
    CONSTRAINT CK_RESULTADO_TIPO CHECK (TIP_RESULTADO IN ('MDDQ', 'DDQ', 'ESCORE_ESQUEMA', 'INDICADOR'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.RESULTADO_QUALIDADE IS 'Resultados calculados de qualidade de dados gerados em uma execucao (por schema, quando aplicavel).';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.SEQ_RESULTADO_QUALIDADE IS 'Chave primaria substituta do registro de resultado.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.SEQ_EXECUCAO           IS 'Execucao (TIP_PROGRAMA = QUALIDADE_DADOS) que gerou o registro.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.DSC_OWNER              IS 'Schema de negocio a que o resultado se refere; NULL para escore agregado de toda a execucao.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.COD_MEDIDA             IS 'Medida ou componente de escore, referenciando MEDIDA.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.COD_DIMENSAO           IS 'Dimensao de qualidade associada, quando aplicavel.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.TIP_RESULTADO          IS 'Tipo do resultado: MDDQ, DDQ, ESCORE_ESQUEMA ou INDICADOR.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.DSC_RESULTADO          IS 'Descricao textual do indicador ou componente de escore.';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.NUM_PESO               IS 'Peso do componente no calculo do escore ponderado (scoring.* no config).';
COMMENT ON COLUMN QUALIDADE_DADOS.RESULTADO_QUALIDADE.NUM_VALOR              IS 'Valor calculado do indicador ou do escore final (0 a 100).';

CREATE INDEX IDX_RESULTADO_EXEC  ON QUALIDADE_DADOS.RESULTADO_QUALIDADE (SEQ_EXECUCAO);
CREATE INDEX IDX_RESULTADO_OWNER ON QUALIDADE_DADOS.RESULTADO_QUALIDADE (SEQ_EXECUCAO, DSC_OWNER);


-- ------------------------------------------------------------
-- 12. PROBLEMA_EXECUCAO — problemas encontrados, agora com
--     sugestao de correcao (regras deterministicas ou IA), no
--     espirito de dq_tables_oracle_v2.sql/DQ_METADATA_ISSUES.
-- ------------------------------------------------------------
CREATE SEQUENCE QUALIDADE_DADOS.SQ_PROBLEMA_EXECUCAO
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE QUALIDADE_DADOS.PROBLEMA_EXECUCAO (
    SEQ_PROBLEMA_EXECUCAO     NUMBER        NOT NULL,
    SEQ_EXECUCAO              NUMBER        NOT NULL,
    COD_MEDIDA                VARCHAR2(10),
    COD_DIMENSAO              NUMBER,
    DSC_PROBLEMA               VARCHAR2(200),
    DSC_OWNER                 VARCHAR2(128),
    NOM_TABELA                VARCHAR2(128),
    NOM_COLUNA                VARCHAR2(128),
    DSC_VALOR                 VARCHAR2(30),
    DSC_SUGESTAO_VALOR        VARCHAR2(200),
    DSC_SUGESTAO_ORIGEM       VARCHAR2(50),
    NUM_SUGESTAO_CONFIANCA    NUMBER(4,2),
    TXT_SUGESTAO_DDL          VARCHAR2(4000),
    CONSTRAINT PK_PROBLEMA_EXECUCAO PRIMARY KEY (SEQ_PROBLEMA_EXECUCAO),
    CONSTRAINT FK_PROBLEMA_EXECUCAO  FOREIGN KEY (SEQ_EXECUCAO)  REFERENCES QUALIDADE_DADOS.EXECUCAO (SEQ_EXECUCAO),
    CONSTRAINT FK_PROBLEMA_DIMENSAO  FOREIGN KEY (COD_DIMENSAO)  REFERENCES QUALIDADE_DADOS.DIMENSAO (COD_DIMENSAO),
    CONSTRAINT FK_PROBLEMA_MEDIDA    FOREIGN KEY (COD_MEDIDA)    REFERENCES QUALIDADE_DADOS.MEDIDA   (COD_MEDIDA),
    CONSTRAINT CK_PROBLEMA_SUG_ORIGEM CHECK (DSC_SUGESTAO_ORIGEM IN ('RULES', 'LLM'))
);

COMMENT ON TABLE  QUALIDADE_DADOS.PROBLEMA_EXECUCAO IS 'Problemas identificados durante uma execucao do processo de qualidade de dados, com sugestao de correcao.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.SEQ_PROBLEMA_EXECUCAO IS 'Chave primaria substituta do registro de problema.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.SEQ_EXECUCAO         IS 'Execucao que gerou o problema.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.COD_MEDIDA           IS 'Codigo da regra de qualidade violada, referenciando MEDIDA.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.COD_DIMENSAO         IS 'Dimensao de qualidade associada ao problema, quando aplicavel.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.DSC_PROBLEMA         IS 'Descricao legivel da regra violada ou do problema encontrado.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.DSC_OWNER            IS 'Schema que contem a tabela em que o problema foi encontrado.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.NOM_TABELA           IS 'Nome da tabela em que o problema foi encontrado.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.NOM_COLUNA           IS 'Nome da coluna em que o problema foi encontrado.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.DSC_VALOR            IS 'Valor invalido ou ambiguo encontrado nos dados (ex.: CPF, CNPJ, CGF).';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.DSC_SUGESTAO_VALOR   IS 'Valor ou comentario sugerido para corrigir o problema.';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.DSC_SUGESTAO_ORIGEM  IS 'Origem da sugestao: RULES (heuristica deterministica) ou LLM (llm_comment_generation).';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.NUM_SUGESTAO_CONFIANCA IS 'Grau de confianca da sugestao (0 a 1).';
COMMENT ON COLUMN QUALIDADE_DADOS.PROBLEMA_EXECUCAO.TXT_SUGESTAO_DDL     IS 'Comando DDL sugerido para corrigir o problema (ex.: COMMENT ON COLUMN ...).';

CREATE INDEX IDX_PROBLEMA_EXEC  ON QUALIDADE_DADOS.PROBLEMA_EXECUCAO (SEQ_EXECUCAO);
CREATE INDEX IDX_PROBLEMA_OWNER ON QUALIDADE_DADOS.PROBLEMA_EXECUCAO (DSC_OWNER, NOM_TABELA);

-- ============================================================
-- FIM DO SCRIPT — esquema único QUALIDADE_DADOS (v2)
--
-- Migracao a partir do que hoje esta em producao (QUALIDADE_DADOS_OLD.sql):
--   1. Criar as tabelas novas/alteradas listadas acima em paralelo.
--   2. Copiar DIMENSAO, TIPO_MEDIDA, MEDIDA (dados existentes preservados).
--   3. Migrar PARMS_EXECUCAO -> CONFIGURACAO_EXECUCAO (mesma semantica,
--      SEQ_PARMS_EXECUCAO -> SEQ_CONFIG_EXECUCAO, colunas iguais + TIP_VALOR).
--   4. Copiar EXECUCAO preenchendo TIP_PROGRAMA = 'QUALIDADE_DADOS' e
--      TIP_MODO = 'ISOLADO' para todo o historico existente (o Catalogo
--      Tecnico e o Glossario de Negocio ainda nao gravavam em banco).
--   5. Copiar RESULTADO_QUALIDADE e PROBLEMA_EXECUCAO (colunas novas de
--      PROBLEMA_EXECUCAO ficam NULL para o historico existente).
--   6. CATALOGO_TABELA/CATALOGO_COLUNA/INDICADOR_CATALOGO e
--      REPOSITORIO_PROCESSADO/TERMO_GLOSSARIO comecam vazias: passam a
--      ser povoadas a partir da primeira execucao dos respectivos
--      pipelines apontada para este esquema.
-- ============================================================
