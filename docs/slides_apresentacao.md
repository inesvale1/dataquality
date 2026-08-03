# Apresentação: Sistema de Avaliação de Qualidade de Dados
**Doutorado UFC — Implementação**

---

## Slide 1 — Título

**Sistema de Avaliação de Qualidade de Dados em Esquemas de Banco de Dados**

- Avaliação automatizada de qualidade de metadados e dados
- Geração de relatórios e métricas de conformidade
- Integração com múltiplas fontes: CSV, Oracle, Amazon S3

---

## Slide 2 — Visão Geral do Sistema

O sistema executa **duas fases independentes** sobre um esquema de banco de dados:

| Fase | Entrada | O que avalia |
|------|---------|--------------|
| Qualidade de Metadados | Metadados do esquema (CSV / Oracle / S3) | Estrutura, convenções e completude dos metadados |
| Qualidade de Dados | Amostras de dados reais | Conformidade de formato e redundância nos dados |

**Saída principal:** Relatórios Excel estruturados por esquema, com métricas e issues identificadas.

---

## Slide 3 — Fase 1: Qualidade de Metadados

### O que é avaliado?

- **Nomenclatura de tabelas**: singular, máx. 30 caracteres
- **Nomenclatura de colunas**: prefixos padrão (`COD_`, `DAT_`, `DSC_`, `NOM_`, etc.), máx. 30 caracteres
- **Comentários/Descrições**: presença de comentários em tabelas e colunas
- **Chaves primárias**: existência de PK em toda tabela; prefixo/sufixo correto
- **Chaves estrangeiras e únicas**: conformidade nos nomes (`FK_`, `UK_`)
- **Colunas identificadoras protegidas**: CPF, CNPJ, EMAIL, CEP, PLACA, RENAVAM, CHASSI, MATRÍCULA, PROTOCOLO devem estar protegidas por constraint
- **Alinhamento tipo × prefixo**: ex. prefixo `QTD_` deve ser numérico; `DAT_` deve ser do tipo data

### Resultado

- Lista de **issues** (não conformidades) por tabela/coluna
- **15 métricas de qualidade** (MQID001–MQID015) organizadas nas dimensões:
  - Consistência, Completude, Unicidade, Conformidade
- Identificação de **candidatos** para a fase de Qualidade de Dados

---

## Slide 4 — Fase 2: Qualidade de Dados

### O que é avaliado?

A partir das colunas candidatas identificadas na Fase 1, o sistema valida amostras de dados reais em duas dimensões:

#### Conformidade de Formato
Valida semanticamente o conteúdo das colunas:

| Tipo | Regra aplicada |
|------|---------------|
| CPF | 11 dígitos (ou 14 com máscara); validação do dígito verificador |
| CNPJ | 14 dígitos (ou 18 com máscara); validação do dígito verificador |
| CEP | 8 ou 9 caracteres |
| E-mail | Padrão RFC via regex |
| Telefone/Celular | Formatos nacionais |
| Data | Formatos de data esperados |

#### Detecção de Redundância
- Identifica colunas com alta repetição de valores (candidatas a duplicatas)
- Analisa estatísticas de distribuição de valores

### Resultado

- **Percentual de conformidade** por coluna candidata
- **Issues de dados**: valores inválidos ou ambíguos encontrados
- Métricas de redundância por coluna

---

## Slide 5 — Métricas Geradas (Qualidade de Metadados)

As 15 métricas são agrupadas em quatro dimensões de qualidade:

| Código | Dimensão | Descrição |
|--------|----------|-----------|
| MQID001–MQID004 | Consistência | Conformidade de nomenclatura (tabelas, colunas, PKs, FKs/UKs) |
| MQID005–MQID007 | Completude | Presença de comentários (tabela, coluna) e PKs em todas as tabelas |
| MQID008–MQID009 | Unicidade | Sem colunas duplicadas; identificadores protegidos por constraint |
| MQID010–MQID015 | Conformidade | Alinhamento tipo×prefixo; prefixos/sufixos de constraints corretos |

Cada métrica é calculada como **percentual de conformidade** (0%–100%).

---

## Slide 6 — Relatórios Excel Gerados

### Arquivo 1: `issues_metadata_<esquema>_<timestamp>.xlsx`

| Aba | Conteúdo |
|-----|----------|
| `SCHEMA_METADATA` | Metadados brutos do esquema de entrada |
| `METADATA_MEASURE` | Contagens estruturais (tabelas, colunas, PKs, FKs, nulos, células) |
| `METADATA_ISSUE` | Lista consolidada de não conformidades identificadas |
| `METADATA_METRIC` | 15 indicadores de qualidade (percentuais) |
| `DATA_QUALITY_RULE_CANDIDATES` | Colunas candidatas para validação de dados |

### Arquivo 2: `issues_dados_<esquema>_<timestamp>.xlsx`

| Aba | Conteúdo |
|-----|----------|
| `DATA_QUALITY_RULE_CANDIDATES` | Colunas candidatas usadas na validação |
| `DATA_QUALITY_METRICS` | Percentuais de conformidade de formato e redundância |
| `DATA_QUALITY_ISSUES` | Valores inválidos ou ambíguos encontrados nas amostras |
| `DATA_ISSUES` | Detalhamento de violações em códigos de documento (CPF/CNPJ) |

---

## Slide 7 — Fontes de Dados Suportadas

O sistema é flexível quanto à origem dos dados:

| Fonte | Metadados | Amostras de Dados |
|-------|-----------|-------------------|
| **CSV** | `schema/inputs/metadata_<esquema>.csv` | `samples/amostra_<esquema>.csv` |
| **Oracle DB** | Extração via conexão JDBC/cx_Oracle | Query por tabela com `SAMPLE_LIMIT` |
| **Amazon S3** | Leitura de CSV no bucket configurado | Leitura de CSV no bucket configurado |

Configuração via arquivo JSON (`config/run_quality.json`):
- `run_model_quality`, `run_data_quality`: ativar/desativar fases
- `sample_limit`: quantidade de linhas amostradas por tabela
- `exclude_tables`: padrões de tabelas a ignorar (views, temporárias)

---

## Slide 8 — Integração com LLM (Claude API)

O sistema possui integração opcional com a API da Anthropic (Claude):

- **Geração automática de sugestões** de correção para cada issue identificada
- O `MetadataContextBuilder` monta o contexto semântico do esquema
- O `MetadataIssueSuggester` chama o LLM para gerar descrições em linguagem natural
- Pode ser ativado/desativado via configuração (`llm_comment_generation`)

**Benefício:** Relatórios mais ricos, com sugestões de correção legíveis diretamente nas abas de issues.

---

## Slide 9 — Estado Atual da Implementação

### Funcionando e testado:
- Leitura de metadados via CSV e Oracle
- Validação de todos os 15 indicadores de metadados
- Identificação de candidatos para qualidade de dados
- Validação de CPF, CNPJ, CEP, e-mail, telefone em dados reais
- Exportação de relatórios Excel com todas as abas
- Integração com Claude API para sugestões automáticas (opcional)
- Coleta de telemetria de execução (JSON)

### Em desenvolvimento / próximos passos:
- Expansão dos tipos de formato validados
- Novas dimensões de qualidade de dados (completude, consistência)
- Interface de configuração simplificada
- Validação em bancos além do Oracle

---

## Slide 10 — Conclusão

- Sistema **modular e configurável** para avaliação de qualidade em esquemas relacionais
- Cobre qualidade em **dois níveis**: estrutural (metadados) e semântico (dados)
- Gera **evidências quantitativas** de qualidade: 15 métricas de metadados + métricas de dados por coluna
- Relatórios **auditáveis** em Excel, prontos para uso em processos de governança de dados
- Integração com **IA generativa** para enriquecimento dos relatórios

> *A implementação atual permite executar o pipeline completo sobre qualquer esquema Oracle ou CSV, gerando relatórios automáticos de qualidade de dados.*
