from ..operations.create_table import CreateTable
import black


class InitialGenerator:
    def __init__(self, from_state: dict, to_state: dict):
        self.from_state = from_state
        self.to_state = to_state

    def generate(self):
        operations = []
        tables = self.to_state["metadata"].tables.values()
        for table in tables:
            operation = CreateTable(
                table.name, columns=[c.copy() for c in table.columns]
            )
            operations.append(operation)
        return operations

    def write_migration_to_disk(self, path: str) -> None:
        operations = self.generate()
        text = f"""\
import savannah
import sqlalchemy


class Migration(savannah.Migration):
    dependencies = []
    operations = {operations!r}
"""
        text = black.format_file_contents(text, fast=False, mode=black.FileMode())
        with open(path, "w") as fout:
            fout.write(text)
