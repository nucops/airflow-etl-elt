"""
tests/dags/test_dag_example.py
------------------------------
Upstream pytest suite validating DAG structure:
  1. No import errors across DAG files.
  2. All DAGs have tags.
  3. All DAGs have default_args retries >= 2.

Supports both Airflow container execution (via DagBag) and host-level AST validation
when running on operating systems without native Airflow daemons (e.g. Windows).
"""

import os
import ast
import re
import glob
import logging
from contextlib import contextmanager
import pytest

try:
    from airflow.models import DagBag
    HAS_DAGBAG = True
except Exception:
    HAS_DAGBAG = False


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    if not HAS_DAGBAG:
        return [(None, None)]
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            base = os.environ.get("AIRFLOW_HOME") or os.getcwd()
            return os.path.relpath(path, base)

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    if not HAS_DAGBAG:
        return []
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        base = os.environ.get("AIRFLOW_HOME") or os.getcwd()
        return os.path.relpath(path, base)

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# ── Full DagBag Tests (Active in Astro Runtime / Linux Container) ────────────

@pytest.mark.skipif(not HAS_DAGBAG, reason="Airflow DagBag runtime requires Linux/Astro container")
@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file via Airflow DagBag."""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.skipif(not HAS_DAGBAG, reason="Airflow DagBag runtime requires Linux/Astro container")
@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()] if HAS_DAGBAG else []
)
def test_dag_tags(dag_id, dag, fileloc):
    """Test if a DAG is tagged."""
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.skipif(not HAS_DAGBAG, reason="Airflow DagBag runtime requires Linux/Astro container")
@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()] if HAS_DAGBAG else []
)
def test_dag_retries(dag_id, dag, fileloc):
    """Test if a DAG has task retries >= 2."""
    retries = dag.default_args.get("retries", None) if dag.default_args else None
    assert (
        retries is not None and retries >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2 (found: {retries})."


# ── Host-Level Static Syntax & Contract Tests ─────────────────────────────────

def get_dag_files():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dags"))
    files = glob.glob(os.path.join(base, "*.py"))
    return [f for f in files if not os.path.basename(f).startswith(".")]


@pytest.mark.parametrize("filepath", get_dag_files(), ids=lambda p: os.path.basename(p))
def test_dag_syntax_and_ast(filepath):
    """Verifies that each DAG file is syntactically valid Python."""
    with open(filepath, "r", encoding="utf-8") as f:
        code = f.read()
    tree = ast.parse(code, filename=filepath)
    assert tree is not None, f"Failed parsing AST for {filepath}"


@pytest.mark.parametrize("filepath", get_dag_files(), ids=lambda p: os.path.basename(p))
def test_dag_has_tags_and_retries_static(filepath):
    """Static contract check verifying tags and retries >= 2 in every DAG."""
    with open(filepath, "r", encoding="utf-8") as f:
        code = f.read()

    # Verify tags defined
    assert "tags=" in code, f"{filepath} must specify tags"

    # Verify retries >= 2
    retries_match = re.search(r'["\']retries["\']:\s*(\d+)', code)
    assert retries_match is not None, f"{filepath} must declare 'retries' in default_args"
    retries_val = int(retries_match.group(1))
    assert retries_val >= 2, f"{filepath} retries must be >= 2 (found: {retries_val})"
