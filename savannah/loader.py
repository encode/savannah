from typing import Dict, Set, List, Tuple
import pkgutil
import typing
from importlib import import_module
from databases import Database
from .migration import Migration


def build_dependants(dependencies: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """
    Given a dependencies mapping, return the reversed dependants dictionary.
    """
    dependants = {name: set() for name in dependencies.keys()}
    for child, parents in dependencies.items():
        for parent in parents:
            dependants[parent].add(child)
    return dependants


def order_dependencies(
    dependencies: Dict[str, Set[str]], dependants: Dict[str, Set[str]]
) -> List[str]:
    """
    Given the dependencies and dependants mappings, return an ordered list
    of the dependencies.
    """
    # The root nodes are the only ones with no dependencies.
    root_nodes = sorted([name for name, deps in dependencies.items() if not deps])

    ordered = list(root_nodes)
    seen = set(root_nodes)
    children = set()
    for node in root_nodes:
        children |= dependants[node]

    while children:
        for node in sorted(children):
            if dependencies[node].issubset(seen):
                ordered.append(node)
                seen.add(node)
                children.remove(node)
                children |= dependants[node]
                break
        else:
            raise Exception()

    return ordered


def load_migrations(applied: Set[str], dir_name: str) -> List[Migration]:
    migration_classes = {}
    dependencies = {}

    names = [name for _, name, is_pkg in pkgutil.iter_modules([dir_name])]
    for name in names:
        module = import_module(f"{dir_name}.{name}")
        migration_cls = getattr(module, "Migration")
        migration_classes[name] = migration_cls
        dependencies[name] = set(migration_cls.dependencies)

    dependants = build_dependants(dependencies)
    names = order_dependencies(dependencies, dependants)

    migrations = []
    for name in names:
        migration_cls = migration_classes[name]
        is_applied = name in applied
        dependant_list = sorted(dependants[name])
        migration = migration_cls(
            name=name, is_applied=is_applied, dependants=dependant_list
        )
        migrations.append(migration)
    return migrations
