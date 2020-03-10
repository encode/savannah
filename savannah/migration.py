from typing import List


class Migration:
    dependencies: List[str] = []
    operations = []

    def __init__(self, name: str, is_applied: bool, dependants: List[str]) -> None:
        self.name = name
        self.is_applied = is_applied
        self.dependants = dependants

    @property
    def is_root(self) -> bool:
        return not self.dependencies

    @property
    def is_leaf(self) -> bool:
        return not self.dependants

    async def upgrade(self):
        print(f"Applying {self.name}")

    async def downgrade(self):
        print(f"Unapplying {self.name}")
