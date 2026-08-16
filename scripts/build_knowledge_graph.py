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
    Schema, TechnicalAsset (table), BusinessConcept, BusinessRule, ValueDomain

Relationships:
    (TechnicalAsset)-[:BELONGS_TO]->(Schema)
    (TechnicalAsset)-[:REPRESENTS]->(BusinessConcept)
    (BusinessConcept)-[:RELATES_TO {relationship, description, confidence}]->(BusinessConcept)
    (BusinessRule)-[:APPLIES_TO]->(BusinessConcept)
    (BusinessConcept)-[:HAS_DOMAIN]->(ValueDomain)
    (TechnicalAsset)-[:REFERENCES {via_column}]->(TechnicalAsset)   -- FK graph, from metadata_context

Usage:
    pip install neo4j   (already in requirements.txt)
    python scripts/build_knowledge_graph.py --neo4j-password <senha> \\
        [--schemas cadastro receita2 sitram2] [--neo4j-uri bolt://localhost:7687] \\
        [--neo4j-user neo4j] [--base-folder ../schema]

Idempotent: every write is a MERGE keyed by a stable uid, so rerunning after
a fresh Fase 1/2 regeneration updates properties in place instead of
duplicating nodes/edges.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from neo4j import Driver, GraphDatabase


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8-sig"))


class KnowledgeGraphLoader:
    def __init__(self, driver: Driver):
        self.driver = driver

    def ensure_constraints(self) -> None:
        statements = [
            "CREATE CONSTRAINT schema_name IF NOT EXISTS FOR (s:Schema) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT technical_asset_uid IF NOT EXISTS FOR (t:TechnicalAsset) REQUIRE t.uid IS UNIQUE",
            "CREATE CONSTRAINT business_concept_uid IF NOT EXISTS FOR (c:BusinessConcept) REQUIRE c.uid IS UNIQUE",
            "CREATE CONSTRAINT business_rule_uid IF NOT EXISTS FOR (r:BusinessRule) REQUIRE r.uid IS UNIQUE",
            "CREATE CONSTRAINT value_domain_uid IF NOT EXISTS FOR (v:ValueDomain) REQUIRE v.uid IS UNIQUE",
        ]
        with self.driver.session() as session:
            for statement in statements:
                session.run(statement)

    def load_schema(
        self,
        schema_name: str,
        canonical: dict[str, Any],
        metadata_context: dict[str, Any] | None,
    ) -> None:
        with self.driver.session() as session:
            session.execute_write(self._merge_schema_node, schema_name)

            for ta in canonical.get("technical_assets", []) or []:
                session.execute_write(self._merge_technical_asset, schema_name, ta)
            for bc in canonical.get("business_concepts", []) or []:
                session.execute_write(self._merge_business_concept, schema_name, bc)
            for br in canonical.get("business_rules", []) or []:
                session.execute_write(self._merge_business_rule, schema_name, br)
            for vd in canonical.get("value_domains", []) or []:
                session.execute_write(self._merge_value_domain, schema_name, vd)

            for rel in canonical.get("concept_relationships", []) or []:
                if rel.get("source_concept_id") and rel.get("target_concept_id"):
                    session.execute_write(self._merge_concept_relationship, schema_name, rel)

            for ta in canonical.get("technical_assets", []) or []:
                for bc_id in ta.get("business_concepts", []) or []:
                    session.execute_write(self._merge_represents_edge, schema_name, ta["id"], bc_id)

            for bc in canonical.get("business_concepts", []) or []:
                for br_id in bc.get("business_rules", []) or []:
                    session.execute_write(self._merge_applies_to_edge, schema_name, br_id, bc["id"])
                for vd_id in bc.get("value_domains", []) or []:
                    session.execute_write(self._merge_has_domain_edge, schema_name, bc["id"], vd_id)

            # business_rules[].concepts is the inverse of business_concepts[].business_rules
            # in the schema -- covered from either side depending on which one the LLM
            # actually populated for a given entry, so merge both directions.
            for br in canonical.get("business_rules", []) or []:
                for bc_id in br.get("concepts", []) or []:
                    session.execute_write(self._merge_applies_to_edge, schema_name, br["id"], bc_id)

            if metadata_context:
                self._load_fk_edges(session, schema_name, metadata_context)

    # -- node merges ----------------------------------------------------

    @staticmethod
    def _merge_schema_node(tx, schema_name: str) -> None:
        tx.run("MERGE (s:Schema {name: $name})", name=schema_name)

    @staticmethod
    def _merge_technical_asset(tx, schema_name: str, ta: dict[str, Any]) -> None:
        tx.run(
            """
            MERGE (t:TechnicalAsset {uid: $uid})
            SET t.local_id = $local_id, t.name = $name, t.asset_type = $asset_type,
                t.role = $role, t.schema = $schema
            WITH t
            MATCH (s:Schema {name: $schema})
            MERGE (t)-[:BELONGS_TO]->(s)
            """,
            uid=f"{schema_name}::{ta['id']}",
            local_id=ta["id"],
            name=ta.get("name", ""),
            asset_type=ta.get("asset_type", ""),
            role=ta.get("role", ""),
            schema=schema_name,
        )

    @staticmethod
    def _merge_business_concept(tx, schema_name: str, bc: dict[str, Any]) -> None:
        tx.run(
            """
            MERGE (c:BusinessConcept {uid: $uid})
            SET c.local_id = $local_id, c.name = $name, c.type = $type,
                c.description = $description, c.schema = $schema
            """,
            uid=f"{schema_name}::{bc['id']}",
            local_id=bc["id"],
            name=bc.get("preferred_name", ""),
            type=bc.get("type", ""),
            description=bc.get("description_evidence", ""),
            schema=schema_name,
        )

    @staticmethod
    def _merge_business_rule(tx, schema_name: str, br: dict[str, Any]) -> None:
        tx.run(
            """
            MERGE (r:BusinessRule {uid: $uid})
            SET r.local_id = $local_id, r.description = $description,
                r.condition = $condition, r.effect = $effect, r.schema = $schema
            """,
            uid=f"{schema_name}::{br['id']}",
            local_id=br["id"],
            description=br.get("description", ""),
            condition=br.get("condition", ""),
            effect=br.get("effect", ""),
            schema=schema_name,
        )

    @staticmethod
    def _merge_value_domain(tx, schema_name: str, vd: dict[str, Any]) -> None:
        tx.run(
            """
            MERGE (v:ValueDomain {uid: $uid})
            SET v.local_id = $local_id, v.name = $name, v.description = $description,
                v.schema = $schema, v.values_json = $values_json
            """,
            uid=f"{schema_name}::{vd['id']}",
            local_id=vd["id"],
            name=vd.get("name", ""),
            description=vd.get("description", ""),
            schema=schema_name,
            values_json=json.dumps(vd.get("values", []), ensure_ascii=False),
        )

    # -- relationship merges ---------------------------------------------

    @staticmethod
    def _merge_concept_relationship(tx, schema_name: str, rel: dict[str, Any]) -> None:
        tx.run(
            """
            MATCH (a:BusinessConcept {uid: $src}), (b:BusinessConcept {uid: $tgt})
            MERGE (a)-[edge:RELATES_TO {relationship: $relationship}]->(b)
            SET edge.description = $description, edge.confidence = $confidence
            """,
            src=f"{schema_name}::{rel['source_concept_id']}",
            tgt=f"{schema_name}::{rel['target_concept_id']}",
            relationship=rel.get("relationship", ""),
            description=rel.get("description", ""),
            confidence=rel.get("confidence", 0),
        )

    @staticmethod
    def _merge_represents_edge(tx, schema_name: str, ta_id: str, bc_id: str) -> None:
        tx.run(
            """
            MATCH (t:TechnicalAsset {uid: $ta_uid}), (c:BusinessConcept {uid: $bc_uid})
            MERGE (t)-[:REPRESENTS]->(c)
            """,
            ta_uid=f"{schema_name}::{ta_id}",
            bc_uid=f"{schema_name}::{bc_id}",
        )

    @staticmethod
    def _merge_applies_to_edge(tx, schema_name: str, br_id: str, bc_id: str) -> None:
        tx.run(
            """
            MATCH (r:BusinessRule {uid: $br_uid}), (c:BusinessConcept {uid: $bc_uid})
            MERGE (r)-[:APPLIES_TO]->(c)
            """,
            br_uid=f"{schema_name}::{br_id}",
            bc_uid=f"{schema_name}::{bc_id}",
        )

    @staticmethod
    def _merge_has_domain_edge(tx, schema_name: str, bc_id: str, vd_id: str) -> None:
        tx.run(
            """
            MATCH (c:BusinessConcept {uid: $bc_uid}), (v:ValueDomain {uid: $vd_uid})
            MERGE (c)-[:HAS_DOMAIN]->(v)
            """,
            bc_uid=f"{schema_name}::{bc_id}",
            vd_uid=f"{schema_name}::{vd_id}",
        )

    def _load_fk_edges(self, session, schema_name: str, metadata_context: dict[str, Any]) -> None:
        # metadata_context's per-column "references" ({"table": ..., "column": ...})
        # is technicalcatalogpipeline's own FK inference (core/context_builder.py
        # ::_infer_reference) -- reused here instead of re-deriving it. Technical
        # asset ids in local_canonical_context follow "TA-<TABLE_NAME>"
        # (context_metadata / prompt convention), so a FK's source/target table
        # name maps straight to a TechnicalAsset id without a separate lookup.
        for col in metadata_context.get("columns", []) or []:
            reference = col.get("references") or {}
            ref_table = str(reference.get("table", "")).strip()
            table_name = str(col.get("table_name", "")).strip()
            if not ref_table or not table_name:
                continue
            session.execute_write(
                self._merge_fk_edge,
                schema_name,
                f"TA-{table_name.upper()}",
                f"TA-{ref_table.upper()}",
                str(col.get("column_name", "")),
            )

    @staticmethod
    def _merge_fk_edge(tx, schema_name: str, src_ta_id: str, tgt_ta_id: str, column_name: str) -> None:
        # MATCH (not MERGE) on both endpoints: only creates the edge when both
        # tables already exist as TechnicalAsset nodes (i.e. both came through
        # local_canonical_context) -- skips references to tables outside the
        # loaded schema's technical_assets instead of creating dangling stubs.
        tx.run(
            """
            MATCH (a:TechnicalAsset {uid: $src_uid}), (b:TechnicalAsset {uid: $tgt_uid})
            MERGE (a)-[edge:REFERENCES {via_column: $column_name}]->(b)
            """,
            src_uid=f"{schema_name}::{src_ta_id}",
            tgt_uid=f"{schema_name}::{tgt_ta_id}",
            column_name=column_name,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--schemas", nargs="+", default=["cadastro", "receita2", "sitram2"])
    parser.add_argument("--base-folder", default="../schema", help="Root folder containing <schema>/{inputs,outputs}")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", required=True)
    args = parser.parse_args()

    base_folder = Path(args.base_folder)
    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        print(f"[erro] Nao foi possivel conectar a {args.neo4j_uri}: {exc}")
        sys.exit(1)

    loader = KnowledgeGraphLoader(driver)
    loader.ensure_constraints()

    for schema_name in args.schemas:
        canonical_path = base_folder / schema_name / "outputs" / f"local_canonical_context_{schema_name}.json"
        metadata_path = base_folder / schema_name / "inputs" / f"metadata_context_{schema_name}.json"

        canonical = _load_json(canonical_path)
        if canonical is None:
            print(f"[aviso] {canonical_path} nao encontrado -- rode businessglossarypipeline/scripts/generate_canonical_context.py primeiro. Pulando {schema_name}.")
            continue

        metadata_context = _load_json(metadata_path)
        if metadata_context is None:
            print(f"[aviso] {metadata_path} nao encontrado -- arestas REFERENCES (FK) ficarao de fora para {schema_name}.")

        print(f"Carregando {schema_name}...")
        loader.load_schema(schema_name, canonical, metadata_context)
        print(
            f"  OK: {len(canonical.get('technical_assets', []))} tabelas, "
            f"{len(canonical.get('business_concepts', []))} conceitos, "
            f"{len(canonical.get('concept_relationships', []))} relacoes de conceito, "
            f"{len(canonical.get('business_rules', []))} regras, "
            f"{len(canonical.get('value_domains', []))} dominios de valor"
        )

    driver.close()
    print("\nConcluido. Abra o Neo4j Browser e rode, por exemplo:")
    print("  MATCH (n) RETURN n LIMIT 300")
    print("  MATCH (c:BusinessConcept)-[r:RELATES_TO]->(c2:BusinessConcept) RETURN c, r, c2")


if __name__ == "__main__":
    main()
