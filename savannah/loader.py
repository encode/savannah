from typing import Dict, Set, List, Tuple
import pkgutil
from importlib import import_module


class LoaderInfo:
    def __init__(self, migrations, leaf_nodes):
        self.migrations = migrations
        self.leaf_nodes = leaf_nodes


def build_dependants(dependencies: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """
    Given a dependencies dictionary, return the reversed dependants dictionary.
    """
    dependants = {name: set() for name in dependencies.keys()}
    for child, parents in dependencies.items():
        for parent in parents:
            dependants[parent].add(child)
    return dependants


def order_dependencies(dependencies: Dict[str, Set[str]]) -> Tuple[List[str], List[str]]:
    """
    Given a dependencies dictionary, return an ordered list of nodes.
    """
    # The initial nodes are the only ones with no dependencies.
    initial_nodes = sorted([name for name, deps in dependencies.items() if not deps])

    # Build our reversed 'dependants' dictionary.
    dependants = build_dependants(dependencies)

    # The leaf nodes is the only ones with no dependants.
    leaf_nodes = sorted([name for name, deps in dependants.items() if not deps])

    ordered = list(initial_nodes)
    seen = set(initial_nodes)
    children = set()
    for node in initial_nodes:
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

    return ordered, leaf_nodes


def load_migrations(applied: Set[str], dir_name: str):
    migrations = {}
    dependencies = {}

    names = [name for _, name, is_pkg in pkgutil.iter_modules([dir_name])]
    for name in names:
        module = import_module(f"{dir_name}.{name}")
        migration_cls = getattr(module, "Migration")
        migration = migration_cls(name=name, is_applied=name in applied)
        migrations[name] = migration
        dependencies[name] = set(migration.dependencies)

    names, leaf_nodes = order_dependencies(dependencies)
    return LoaderInfo(
        migrations={name: migrations[name] for name in names},
        leaf_nodes=leaf_nodes
    )
