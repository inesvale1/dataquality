"""Loads local_canonical_context_<schema>.json (businessglossarypipeline,
Fase 2) -- plus metadata_context_<schema>.json (technicalcatalogpipeline,
Fase 1) for the FK graph -- into a Neo4j knowledge graph, one schema at a
time or several in the same run (all in the same graph, kept apart by a
`schema` property and a schema-qualified `uid` merge key, so cross-schema
reruns never collide).

local_canonical_context already carries most of the graph as explicit,
cross-referencing IDs (BC-..., TA-..., BR-..., VD-...) -- this script is
mostly a direct translation of those references into Cypher MERGE
statements, not a new extraction step.

Node labels:
    Schema, TechnicalAsset (table), BusinessConcept, BusinessRule, ValueDomain,
    BusinessKey (CPF/CNPJ/... -- not schema-scoped, see below)

Relationships:
    (TechnicalAsset)-[:BELONGS_TO]->(Schema)
    (TechnicalAsset)-[:REPRESENTS]->(BusinessConcept)
    (BusinessConcept)-[:RELATES_TO {relationship, description, confidence}]->(BusinessConcept)
    (BusinessRule)-[:APPLIES_TO]->(BusinessConcept)
    (BusinessConcept)-[:HAS_DOMAIN]->(ValueDomain)
    (TechnicalAsset)-[:REFERENCES {via_column}]->(TechnicalAsset)   -- FK graph, from metadata_context
    (TechnicalAsset)-[:HAS_KEY {column}]->(BusinessKey)             -- cross-schema, see below

Every other node above is namespaced per schema (uid = "<schema>::<local_id>"),
so nothing above connects across schemas by itself -- cadastro/receita2/sitram2
are technically 3 separate Oracle schemas processed independently in Fase 1/2,
and their business_concepts/technical_assets never get compared against each
other. BusinessKey is the deliberate exception: one global node per natural
business key (CPF, CNPJ, CEP, EMAIL, PLACA, RENAVAM, CHASSI, MATRICULA --
CROSS_SCHEMA_KEY_TOKENS below, same list as
app/orchestration/denodo_catalog_input_builder.py's _PERSONAL_DATA_TOKENS),
linked from every TechnicalAsset (in any schema) that has a column matching
that token. Two tables in different schemas sharing a BusinessKey is exactly
the kind of relationship a formal Oracle FK wouldn't capture (SEFAZ-CE
generally avoids cross-schema FK constraints by design) but that matters for
finding how e.g. cadastro (the CNPJ/CPF registry) connects to receita2/sitram2.

Two ways to load, same data either way:

1. Direct connection (needs network access to the Neo4j instance on its Bolt
   port -- works for a local Neo4j Desktop/Docker instance, but corporate
   firewalls commonly block outbound Bolt to a cloud instance like Aura):

    pip install neo4j   (already in requirements.txt)
    python scripts/build_knowledge_graph.py --neo4j-password <senha> \\
        [--schemas cadastro receita2 sitram2] [--neo4j-uri bolt://localhost:7687] \\
        [--neo4j-user neo4j] [--base-folder ../schema]

2. Generate a .cypher script to paste into a Neo4j Browser tab that's
   already connected (e.g. the Aura Console's own browser-based Query tab --
   that connection rides on the browser's session, not this machine's
   outbound network, so it works even when direct driver access is blocked):

    python scripts/build_knowledge_graph.py --emit-cypher grafo.cypher \\
        [--schemas cadastro receita2 sitram2] [--base-folder ../schema]

Idempotent either way: every write is a MERGE keyed by a stable uid, so
rerunning after a fresh Fase 1/2 regeneration (or re-pasting the script)
updates properties in place instead of duplicating nodes/edges.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8-sig"))


# ---------------------------------------------------------------------------
# Collection: local_canonical_context / metadata_context -> plain node/edge
# rows, shared by both the live-driver loader and the .cypher-file renderer
# below so the two backends can never drift out of sync with each other.
# ---------------------------------------------------------------------------


def collect_graph_data(
    schema_name: str,
    canonical: dict[str, Any],
    metadata_context: dict[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    def uid(local_id: str) -> str:
        return f"{schema_name}::{local_id}"

    technical_assets = [
        {
            "uid": uid(ta["id"]),
            "local_id": ta["id"],
            "name": ta.get("name", ""),
            "asset_type": ta.get("asset_type", ""),
            "role": ta.get("role", ""),
            "schema": schema_name,
        }
        for ta in canonical.get("technical_assets", []) or []
    ]
    business_concepts = [
        {
            "uid": uid(bc["id"]),
            "local_id": bc["id"],
            "name": bc.get("preferred_name", ""),
            "type": bc.get("type", ""),
            "description": bc.get("description_evidence", ""),
            "schema": schema_name,
        }
        for bc in canonical.get("business_concepts", []) or []
    ]
    business_rules = [
        {
            "uid": uid(br["id"]),
            "local_id": br["id"],
            "description": br.get("description", ""),
            "condition": br.get("condition", ""),
            "effect": br.get("effect", ""),
            "schema": schema_name,
        }
        for br in canonical.get("business_rules", []) or []
    ]
    value_domains = [
        {
            "uid": uid(vd["id"]),
            "local_id": vd["id"],
            "name": vd.get("name", ""),
            "description": vd.get("description", ""),
            "schema": schema_name,
            "values_json": json.dumps(vd.get("values", []), ensure_ascii=False),
        }
        for vd in canonical.get("value_domains", []) or []
    ]

    concept_relationships = [
        {
            "src": uid(rel["source_concept_id"]),
            "tgt": uid(rel["target_concept_id"]),
            "relationship": rel.get("relationship", ""),
            "description": rel.get("description", ""),
            "confidence": rel.get("confidence", 0),
        }
        for rel in canonical.get("concept_relationships", []) or []
        if rel.get("source_concept_id") and rel.get("target_concept_id")
    ]

    represents_edges = [
        {"ta_uid": uid(ta["id"]), "bc_uid": uid(bc_id)}
        for ta in canonical.get("technical_assets", []) or []
        for bc_id in ta.get("business_concepts", []) or []
    ]

    # business_rules[].concepts and business_concepts[].business_rules are the
    # same relationship described from either side -- whichever the LLM
    # happened to populate for a given entry -- so both are collected and
    # deduplicated together rather than picking just one side.
    applies_to_pairs: set[tuple[str, str]] = set()
    for bc in canonical.get("business_concepts", []) or []:
        for br_id in bc.get("business_rules", []) or []:
            applies_to_pairs.add((uid(br_id), uid(bc["id"])))
    for br in canonical.get("business_rules", []) or []:
        for bc_id in br.get("concepts", []) or []:
            applies_to_pairs.add((uid(br["id"]), uid(bc_id)))
    applies_to_edges = [{"br_uid": br_uid, "bc_uid": bc_uid} for br_uid, bc_uid in sorted(applies_to_pairs)]

    has_domain_edges = [
        {"bc_uid": uid(bc["id"]), "vd_uid": uid(vd_id)}
        for bc in canonical.get("business_concepts", []) or []
        for vd_id in bc.get("value_domains", []) or []
    ]

    fk_edges: list[dict[str, Any]] = []
    if metadata_context:
        # metadata_context's per-column "references" ({"table": ..., "column": ...})
        # is technicalcatalogpipeline's own FK inference (core/context_builder.py
        # ::_infer_reference) -- reused here instead of re-deriving it. Technical
        # asset ids in local_canonical_context follow "TA-<TABLE_NAME>" (prompt
        # convention), so a FK's source/target table name maps straight to a
        # TechnicalAsset id without a separate lookup.
        seen_fk: set[tuple[str, str, str]] = set()
        for col in metadata_context.get("columns", []) or []:
            reference = col.get("references") or {}
            ref_table = str(reference.get("table", "")).strip()
            table_name = str(col.get("table_name", "")).strip()
            if not ref_table or not table_name:
                continue
            column_name = str(col.get("column_name", ""))
            key = (table_name.upper(), ref_table.upper(), column_name)
            if key in seen_fk:
                continue
            seen_fk.add(key)
            fk_edges.append(
                {
                    "src_uid": uid(f"TA-{table_name.upper()}"),
                    "tgt_uid": uid(f"TA-{ref_table.upper()}"),
                    "via_column": column_name,
                }
            )

    return {
        "technical_assets": technical_assets,
        "business_concepts": business_concepts,
        "business_rules": business_rules,
        "value_domains": value_domains,
        "concept_relationships": concept_relationships,
        "represents_edges": represents_edges,
        "applies_to_edges": applies_to_edges,
        "has_domain_edges": has_domain_edges,
        "fk_edges": fk_edges,
    }


# Natural/business keys likely to appear identically across schemas even
# without a formal cross-schema Oracle FK (which SEFAZ-CE generally avoids by
# design -- see the module docstring). Deliberately narrower than
# ValidationConfig.type_naming.identifier_name_patterns (which also matches
# generic ID/COD/PROTOCOLO): those match nearly every table and would turn
# BusinessKey into a near-complete graph instead of a meaningful one. Mirrors
# app/orchestration/denodo_catalog_input_builder.py's _PERSONAL_DATA_TOKENS.
CROSS_SCHEMA_KEY_TOKENS = ("CPF", "CNPJ", "CEP", "EMAIL", "PLACA", "RENAVAM", "CHASSI", "MATRICULA")


def collect_business_keys(metadata_contexts: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Cross-schema pass: one global BusinessKey node per token in
    CROSS_SCHEMA_KEY_TOKENS, linked from every TechnicalAsset (in any of the
    schemas passed in) whose column name contains that token. Unlike
    collect_graph_data, this looks at every schema's metadata_context
    together -- that's the whole point, it's the one thing meant to connect
    across schema boundaries.
    """
    seen_keys: set[str] = set()
    business_keys: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str]] = set()
    has_key_edges: list[dict[str, Any]] = []

    for schema_name, metadata_context in metadata_contexts.items():
        for col in metadata_context.get("columns", []) or []:
            column_name = str(col.get("column_name", "")).upper()
            table_name = str(col.get("table_name", "")).strip()
            if not column_name or not table_name:
                continue
            tokens = set(re.split(r"[^A-Z0-9]+", column_name))
            for key_token in CROSS_SCHEMA_KEY_TOKENS:
                if key_token not in tokens:
                    continue
                if key_token not in seen_keys:
                    seen_keys.add(key_token)
                    business_keys.append({"name": key_token})
                ta_uid = f"{schema_name}::TA-{table_name.upper()}"
                edge_key = (ta_uid, key_token, column_name)
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                has_key_edges.append({"ta_uid": ta_uid, "key_name": key_token, "column": column_name})

    return {"business_keys": business_keys, "has_key_edges": has_key_edges}


CONSTRAINT_STATEMENTS = [
    "CREATE CONSTRAINT schema_name IF NOT EXISTS FOR (s:Schema) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT technical_asset_uid IF NOT EXISTS FOR (t:TechnicalAsset) REQUIRE t.uid IS UNIQUE",
    "CREATE CONSTRAINT business_concept_uid IF NOT EXISTS FOR (c:BusinessConcept) REQUIRE c.uid IS UNIQUE",
    "CREATE CONSTRAINT business_rule_uid IF NOT EXISTS FOR (r:BusinessRule) REQUIRE r.uid IS UNIQUE",
    "CREATE CONSTRAINT value_domain_uid IF NOT EXISTS FOR (v:ValueDomain) REQUIRE v.uid IS UNIQUE",
    "CREATE CONSTRAINT business_key_name IF NOT EXISTS FOR (k:BusinessKey) REQUIRE k.name IS UNIQUE",
]

# (row_key_in_data -> cypher property name) for each SET clause; "uid" is
# always the MERGE key, never repeated in SET.
_NODE_SPECS: list[tuple[str, str, list[str]]] = [
    ("technical_assets", "TechnicalAsset", ["local_id", "name", "asset_type", "role", "schema"]),
    ("business_concepts", "BusinessConcept", ["local_id", "name", "type", "description", "schema"]),
    ("business_rules", "BusinessRule", ["local_id", "description", "condition", "effect", "schema"]),
    ("value_domains", "ValueDomain", ["local_id", "name", "description", "schema", "values_json"]),
]


# ---------------------------------------------------------------------------
# Backend 1: live driver connection (bolt://... or neo4j+s://...)
# ---------------------------------------------------------------------------


def load_via_driver(driver: Any, schema_name: str, graph: dict[str, list[dict[str, Any]]], database: str = "neo4j") -> None:
    with driver.session(database=database) as session:
        session.execute_write(lambda tx: tx.run("MERGE (s:Schema {name: $name})", name=schema_name))

        for data_key, label, set_props in _NODE_SPECS:
            rows = graph[data_key]
            if not rows:
                continue
            set_clause = ", ".join(f"n.{p} = row.{p}" for p in set_props)
            query = f"UNWIND $rows AS row MERGE (n:{label} {{uid: row.uid}}) SET {set_clause}"
            session.execute_write(lambda tx, q=query, r=rows: tx.run(q, rows=r))

        session.execute_write(
            lambda tx: tx.run(
                "UNWIND $rows AS row MATCH (t:TechnicalAsset {uid: row.uid}), (s:Schema {name: $schema}) MERGE (t)-[:BELONGS_TO]->(s)",
                rows=graph["technical_assets"],
                schema=schema_name,
            )
        )

        if graph["concept_relationships"]:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    UNWIND $rows AS row
                    MATCH (a:BusinessConcept {uid: row.src}), (b:BusinessConcept {uid: row.tgt})
                    MERGE (a)-[edge:RELATES_TO {relationship: row.relationship}]->(b)
                    SET edge.description = row.description, edge.confidence = row.confidence
                    """,
                    rows=graph["concept_relationships"],
                )
            )
        if graph["represents_edges"]:
            session.execute_write(
                lambda tx: tx.run(
                    "UNWIND $rows AS row MATCH (t:TechnicalAsset {uid: row.ta_uid}), (c:BusinessConcept {uid: row.bc_uid}) MERGE (t)-[:REPRESENTS]->(c)",
                    rows=graph["represents_edges"],
                )
            )
        if graph["applies_to_edges"]:
            session.execute_write(
                lambda tx: tx.run(
                    "UNWIND $rows AS row MATCH (r:BusinessRule {uid: row.br_uid}), (c:BusinessConcept {uid: row.bc_uid}) MERGE (r)-[:APPLIES_TO]->(c)",
                    rows=graph["applies_to_edges"],
                )
            )
        if graph["has_domain_edges"]:
            session.execute_write(
                lambda tx: tx.run(
                    "UNWIND $rows AS row MATCH (c:BusinessConcept {uid: row.bc_uid}), (v:ValueDomain {uid: row.vd_uid}) MERGE (c)-[:HAS_DOMAIN]->(v)",
                    rows=graph["has_domain_edges"],
                )
            )
        if graph["fk_edges"]:
            # MATCH (not MERGE) on both endpoints: only creates the edge when
            # both tables already exist as TechnicalAsset nodes -- skips
            # references to tables outside the loaded schema's
            # technical_assets instead of creating dangling stubs.
            session.execute_write(
                lambda tx: tx.run(
                    "UNWIND $rows AS row MATCH (a:TechnicalAsset {uid: row.src_uid}), (b:TechnicalAsset {uid: row.tgt_uid}) MERGE (a)-[edge:REFERENCES {via_column: row.via_column}]->(b)",
                    rows=graph["fk_edges"],
                )
            )


def load_business_keys_via_driver(driver: Any, business_keys: dict[str, list[dict[str, Any]]], database: str = "neo4j") -> None:
    # Runs after every schema's load_via_driver call: MATCH on TechnicalAsset
    # below requires those nodes to already exist.
    with driver.session(database=database) as session:
        if business_keys["business_keys"]:
            session.execute_write(
                lambda tx: tx.run(
                    "UNWIND $rows AS row MERGE (k:BusinessKey {name: row.name})",
                    rows=business_keys["business_keys"],
                )
            )
        if business_keys["has_key_edges"]:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    UNWIND $rows AS row
                    MATCH (t:TechnicalAsset {uid: row.ta_uid}), (k:BusinessKey {name: row.key_name})
                    MERGE (t)-[edge:HAS_KEY {column: row.column}]->(k)
                    """,
                    rows=business_keys["has_key_edges"],
                )
            )


# ---------------------------------------------------------------------------
# Backend 2: render a self-contained .cypher script (no driver, no network
# from this machine -- paste it into any already-connected Neo4j Browser tab,
# e.g. the Aura Console's own web-based Query editor)
# ---------------------------------------------------------------------------


def _cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, list):
        return "[" + ", ".join(_cypher_literal(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k}: {_cypher_literal(v)}" for k, v in value.items()) + "}"
    return _cypher_literal(str(value))


def render_cypher_script(
    graphs: list[tuple[str, dict[str, list[dict[str, Any]]]]],
    business_keys: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    lines: list[str] = [
        "// Gerado por dataquality/scripts/build_knowledge_graph.py --emit-cypher",
        "// Cole este script inteiro no editor de consulta do Neo4j Browser (Aura Console",
        "// > Query, ou Neo4j Desktop) e execute. Idempotente: pode rodar de novo sem duplicar.",
        "",
    ]
    for statement in CONSTRAINT_STATEMENTS:
        lines.append(statement + ";")
    lines.append("")

    for schema_name, graph in graphs:
        lines.append(f"// ===== {schema_name} =====")
        lines.append(f"MERGE (s:Schema {{name: {_cypher_literal(schema_name)}}});")
        lines.append("")

        for data_key, label, set_props in _NODE_SPECS:
            rows = graph[data_key]
            if not rows:
                continue
            set_clause = ", ".join(f"n.{p} = row.{p}" for p in set_props)
            lines.append(f"UNWIND {_cypher_literal(rows)} AS row")
            lines.append(f"MERGE (n:{label} {{uid: row.uid}})")
            lines.append(f"SET {set_clause};")
            lines.append("")

        if graph["technical_assets"]:
            lines.append(f"UNWIND {_cypher_literal(graph['technical_assets'])} AS row")
            lines.append(f"MATCH (t:TechnicalAsset {{uid: row.uid}}), (s:Schema {{name: {_cypher_literal(schema_name)}}})")
            lines.append("MERGE (t)-[:BELONGS_TO]->(s);")
            lines.append("")

        edge_specs = [
            ("concept_relationships", "BusinessConcept", "src", "BusinessConcept", "tgt",
             "MERGE (a)-[edge:RELATES_TO {relationship: row.relationship}]->(b) SET edge.description = row.description, edge.confidence = row.confidence"),
            ("represents_edges", "TechnicalAsset", "ta_uid", "BusinessConcept", "bc_uid",
             "MERGE (a)-[:REPRESENTS]->(b)"),
            ("applies_to_edges", "BusinessRule", "br_uid", "BusinessConcept", "bc_uid",
             "MERGE (a)-[:APPLIES_TO]->(b)"),
            ("has_domain_edges", "BusinessConcept", "bc_uid", "ValueDomain", "vd_uid",
             "MERGE (a)-[:HAS_DOMAIN]->(b)"),
            ("fk_edges", "TechnicalAsset", "src_uid", "TechnicalAsset", "tgt_uid",
             "MERGE (a)-[edge:REFERENCES {via_column: row.via_column}]->(b)"),
        ]
        for data_key, src_label, src_key, tgt_label, tgt_key, merge_clause in edge_specs:
            rows = graph[data_key]
            if not rows:
                continue
            lines.append(f"UNWIND {_cypher_literal(rows)} AS row")
            lines.append(f"MATCH (a:{src_label} {{uid: row.{src_key}}}), (b:{tgt_label} {{uid: row.{tgt_key}}})")
            lines.append(f"{merge_clause};")
            lines.append("")

    if business_keys:
        lines.append("// ===== Chaves de negocio entre esquemas (CPF/CNPJ/...) =====")
        if business_keys["business_keys"]:
            lines.append(f"UNWIND {_cypher_literal(business_keys['business_keys'])} AS row")
            lines.append("MERGE (k:BusinessKey {name: row.name});")
            lines.append("")
        if business_keys["has_key_edges"]:
            lines.append(f"UNWIND {_cypher_literal(business_keys['has_key_edges'])} AS row")
            lines.append("MATCH (t:TechnicalAsset {uid: row.ta_uid}), (k:BusinessKey {name: row.key_name})")
            lines.append("MERGE (t)-[edge:HAS_KEY {column: row.column}]->(k);")
            lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--schemas", nargs="+", default=["cadastro", "receita2", "sitram2"])
    parser.add_argument("--base-folder", default="../schema", help="Root folder containing <schema>/{inputs,outputs}")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default=None, help="Required unless --emit-cypher is used")
    parser.add_argument("--neo4j-database", default="neo4j", help="Database name inside the DBMS (default: neo4j)")
    parser.add_argument(
        "--emit-cypher",
        default=None,
        metavar="OUTPUT.cypher",
        help="Write a self-contained .cypher script instead of connecting directly -- "
        "paste it into a Neo4j Browser tab that's already connected (e.g. Aura Console's "
        "Query editor). No Neo4j connection or credentials needed from this machine.",
    )
    args = parser.parse_args()

    if not args.emit_cypher and not args.neo4j_password:
        parser.error("--neo4j-password is required unless --emit-cypher is used")

    base_folder = Path(args.base_folder)
    collected: list[tuple[str, dict[str, list[dict[str, Any]]]]] = []
    metadata_contexts: dict[str, dict[str, Any]] = {}

    for schema_name in args.schemas:
        canonical_path = base_folder / schema_name / "outputs" / f"local_canonical_context_{schema_name}.json"
        metadata_path = base_folder / schema_name / "inputs" / f"metadata_context_{schema_name}.json"

        canonical = _load_json(canonical_path)
        if canonical is None:
            print(f"[aviso] {canonical_path} nao encontrado -- rode businessglossarypipeline/scripts/generate_canonical_context.py primeiro. Pulando {schema_name}.")
            continue

        metadata_context = _load_json(metadata_path)
        if metadata_context is None:
            print(f"[aviso] {metadata_path} nao encontrado -- arestas REFERENCES (FK) e chaves de negocio entre esquemas ficarao de fora para {schema_name}.")
        else:
            metadata_contexts[schema_name] = metadata_context

        graph = collect_graph_data(schema_name, canonical, metadata_context)
        collected.append((schema_name, graph))
        print(
            f"{schema_name}: {len(graph['technical_assets'])} tabelas, "
            f"{len(graph['business_concepts'])} conceitos, "
            f"{len(graph['concept_relationships'])} relacoes de conceito, "
            f"{len(graph['business_rules'])} regras, "
            f"{len(graph['value_domains'])} dominios de valor, "
            f"{len(graph['fk_edges'])} arestas de FK"
        )

    if not collected:
        print("Nada para carregar.")
        sys.exit(1)

    business_keys = collect_business_keys(metadata_contexts)
    print(
        f"Chaves de negocio entre esquemas: {len(business_keys['business_keys'])} tipos "
        f"({', '.join(k['name'] for k in business_keys['business_keys'])}), "
        f"{len(business_keys['has_key_edges'])} tabelas ligadas a alguma chave"
    )

    if args.emit_cypher:
        script = render_cypher_script(collected, business_keys)
        output_path = Path(args.emit_cypher)
        output_path.write_text(script, encoding="utf-8")
        print(f"\nScript Cypher gravado em: {output_path.resolve()} ({len(script):,} caracteres)")
        print("Cole o conteudo desse arquivo no editor de consulta do Neo4j Browser e execute.")
        return

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        print(f"[erro] Nao foi possivel conectar a {args.neo4j_uri}: {exc}")
        sys.exit(1)

    with driver.session(database=args.neo4j_database) as session:
        for statement in CONSTRAINT_STATEMENTS:
            session.run(statement)

    for schema_name, graph in collected:
        print(f"Carregando {schema_name}...")
        load_via_driver(driver, schema_name, graph, database=args.neo4j_database)

    print("Carregando chaves de negocio entre esquemas...")
    load_business_keys_via_driver(driver, business_keys, database=args.neo4j_database)

    driver.close()
    print("\nConcluido. Abra o Neo4j Browser e rode, por exemplo:")
    print("  MATCH (n) RETURN n LIMIT 300")
    print("  MATCH (c:BusinessConcept)-[r:RELATES_TO]->(c2:BusinessConcept) RETURN c, r, c2")
    print("  MATCH (t1:TechnicalAsset)-[:HAS_KEY]->(k:BusinessKey)<-[:HAS_KEY]-(t2:TechnicalAsset)")
    print("  WHERE t1.schema <> t2.schema RETURN t1, k, t2")


if __name__ == "__main__":
    main()
