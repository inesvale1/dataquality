# Diagrama de Fases do Sistema de Qualidade de Dados

Para renderizar: cole o bloco abaixo em https://mermaid.live ou use a extensão "Markdown Preview Mermaid Support" no VS Code.

---

```mermaid
flowchart TD
    %% ── ENTRADAS ──────────────────────────────────────────────
    subgraph ENTRADA["📥  Fontes de Entrada"]
        direction LR
        SRC_META["Metadados\n(CSV · Oracle · S3)"]
        SRC_DATA["Amostras de Dados\n(CSV · Oracle · S3)"]
    end

    %% ── FASE 1 ────────────────────────────────────────────────
    subgraph FASE1["🗂️  FASE 1 — Qualidade de Metadados"]
        direction TB
        F1A["Leitura & normalização\ndos metadados"]
        F1B["Validação de regras\nde metadados\n(15 métricas · MQID001–015)"]
        F1C["Cálculo de métricas\nde qualidade\n(Consistência · Completude\nUnicidade · Conformidade)"]
        F1D["Identificação de\ncandidatos para\nqualidade de dados"]
        F1E["Geração de sugestões\nvia LLM — Claude API\n(opcional)"]

        F1A --> F1B --> F1C --> F1D
        F1C --> F1E
    end

    %% ── FASE 2 ────────────────────────────────────────────────
    subgraph FASE2["🔍  FASE 2 — Qualidade de Dados"]
        direction TB
        F2A["Leitura das\namostras de dados"]
        F2B["Conformidade de Formato\n(CPF · CNPJ · CEP\nE-mail · Telefone · Data)"]
        F2C["Detecção de\nRedundância\n(duplicatas / distribuição)"]
        F2D["Cálculo de métricas\nde qualidade de dados\n(% conformidade por coluna)"]

        F2A --> F2B & F2C --> F2D
    end

    %% ── SAÍDAS ────────────────────────────────────────────────
    subgraph SAIDA["📊  Relatórios Excel Gerados"]
        direction TB
        R1["issues_metadados\n─────────────────\nSCHEMA_METADATA\nMETADATA_MEASURE\nMETADATA_ISSUE\nMETADATA_METRIC\nDATA_QUALITY_RULE_CANDIDATES"]
        R2["issues_dados\n─────────────────\nDATA_QUALITY_RULE_CANDIDATES\nDATA_QUALITY_METRICS\nDATA_QUALITY_ISSUES\nDATA_ISSUES"]
        TEL["Telemetria\n(JSON)"]
    end

    %% ── FLUXO PRINCIPAL ───────────────────────────────────────
    SRC_META --> F1A
    SRC_DATA --> F2A
    F1D -->|"candidatos"| F2A
    F1C --> R1
    F1D --> R1
    F2D --> R2
    FASE1 -.->|"telemetria"| TEL
    FASE2 -.->|"telemetria"| TEL

    %% ── ESTILOS ───────────────────────────────────────────────
    style ENTRADA fill:#e8f4fd,stroke:#2980b9,color:#000
    style FASE1   fill:#eafaf1,stroke:#27ae60,color:#000
    style FASE2   fill:#fef9e7,stroke:#f39c12,color:#000
    style SAIDA   fill:#f5eef8,stroke:#8e44ad,color:#000

    style F1B fill:#d5f5e3,stroke:#27ae60,color:#000
    style F1C fill:#d5f5e3,stroke:#27ae60,color:#000
    style F2B fill:#fdebd0,stroke:#f39c12,color:#000
    style F2C fill:#fdebd0,stroke:#f39c12,color:#000
```

---

## Versão em tabela (para montar em PowerPoint / Draw.io)

### Etapas do Processo — Metadados

| Nº | Etapa | Módulo Principal | Saída |
|----|-------|-----------------|-------|
| 1 | Leitura & normalização dos metadados | `MetadataSourceBuilder` | DataFrame de metadados |
| 2 | Validação de regras de metadados (15 métricas) | `MetadataValidator` | Lista de issues por tabela/coluna |
| 3 | Cálculo de métricas de qualidade | `MetadataQualityMetricsCalculator` | MQID001–015 (% conformidade) |
| 4 | Identificação de candidatos de dados | `MetadataValidator` (candidatos) | Lista de colunas candidatas |
| 5 | Geração de sugestões via LLM (opcional) | `MetadataIssueSuggester` + Claude API | Descrições em linguagem natural |
| 6 | Exportação do relatório Excel | `ExcelReportExporter` | `issues_metadata_<esquema>.xlsx` |

### Etapas do Processo — Dados

| Nº | Etapa | Módulo Principal | Saída |
|----|-------|-----------------|-------|
| 1 | Leitura das amostras de dados | `SampleSource` (CSV/Oracle/S3) | DataFrame de amostras |
| 2 | Validação de conformidade de formato | `DataQualityValidator` + `DocumentCodeClassifier` | Valores válidos / inválidos / ambíguos |
| 3 | Detecção de redundância | `DataQualityValidator` (redundância) | Estatísticas de distribuição |
| 4 | Cálculo de métricas de dados | `DocumentCodeQualityAnalyzer` | % conformidade por coluna |
| 5 | Exportação do relatório Excel | `ExcelReportExporter` | `issues_dados_<esquema>.xlsx` |

---

## Diagrama simplificado — fluxo linear (para slides)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ENTRADAS                                        │
│         Metadados (CSV / Oracle / S3)   Amostras (CSV / Oracle / S3)   │
└──────────────────┬──────────────────────────────┬───────────────────────┘
                   │                              │
                   ▼                              │
    ╔══════════════════════════════╗              │
    ║  FASE 1 — METADADOS          ║              │
    ║  ① Leitura & normalização    ║              │
    ║  ② Validação de regras       ║──candidatos──┤
    ║  ③ Cálculo de métricas       ║              │
    ║  ④ Sugestões via LLM (opt.)  ║              │
    ╚══════════════════════════════╝              │
                   │                              ▼
                   │           ╔══════════════════════════════╗
                   │           ║  FASE 2 — DADOS              ║
                   │           ║  ① Leitura de amostras       ║
                   │           ║  ② Conformidade de formato   ║
                   │           ║  ③ Detecção de redundância   ║
                   │           ║  ④ Cálculo de métricas       ║
                   │           ╚══════════════════════════════╝
                   │                              │
                   ▼                              ▼
    ┌──────────────────────┐    ┌──────────────────────────────┐
    │ issues_metadados.xlsx│    │    issues_dados.xlsx         │
    │ • METADATA_MEASURE   │    │ • DATA_QUALITY_METRICS       │
    │ • METADATA_ISSUE     │    │ • DATA_QUALITY_ISSUES        │
    │ • METADATA_METRIC    │    │ • DATA_ISSUES                │
    └──────────────────────┘    └──────────────────────────────┘
```
