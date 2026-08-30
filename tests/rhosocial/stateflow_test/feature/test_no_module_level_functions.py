# tests/rhosocial/stateflow_test/feature/test_no_module_level_functions.py
"""Guard: no standalone (module-level) functions in the stateflow package.

Enforces the "全面面向对象" principle: every function must live inside a
class as a method, classmethod, or staticmethod. Module level is restricted
to imports, class definitions, constants, and type aliases.

See ``.claude/rules/sync-async-non-interoperability.md`` and
``.claude/rules/namespacing.md``.
"""

import ast
from pathlib import Path

import pytest

_PACKAGE_ROOT = Path(__file__).resolve().parents[4] / "src" / "rhosocial" / "stateflow"


def _stateflow_source_files():
    """Return all .py files under the stateflow package (excluding compiled)."""
    if not _PACKAGE_ROOT.is_dir():
        pytest.skip(f"package source not found at {_PACKAGE_ROOT}")
    return sorted(_PACKAGE_ROOT.rglob("*.py"))


def _module_level_function_nodes(source: str):
    """Yield (lineno, kind, name) for module-level functions and lambda assigns."""
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node.lineno, type(node).__name__, node.name
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value if isinstance(node, ast.Assign) else node.value
            if isinstance(value, ast.Lambda):
                target = node.targets[0] if isinstance(node, ast.Assign) else node.target
                name = getattr(target, "id", getattr(target, "attr", "<lambda>"))
                yield node.lineno, "Lambda", name


def test_no_module_level_functions():
    """No top-level ``def`` / ``async def`` / ``lambda`` assignment in src."""
    violations = []
    for path in _stateflow_source_files():
        source = path.read_text(encoding="utf-8")
        for lineno, kind, name in _module_level_function_nodes(source):
            violations.append(f"{path.relative_to(_PACKAGE_ROOT.parent.parent.parent)}:{lineno}: {kind} {name}")
    assert not violations, (
        "Module-level functions are forbidden (functions must live in classes). "
        "Found:\n" + "\n".join(violations)
    )
