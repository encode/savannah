class EmptyGenerator:
    def generate(self, from_state, to_state):
        pass

    def write_migration_to_disk(self, path, dependencies):
        with open(path, "w") as fout:
            fout.write(
                f"""\
import savannah


class Migration(savannah.Migration):
    dependencies = {dependencies!r}
    operations = []
"""
            )
