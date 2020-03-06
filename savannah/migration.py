class Migration:
    dependencies = []
    operations = []

    def __init__(self, name: str, is_applied: bool):
        self.name = name
        self.is_applied = is_applied

    async def upgrade(self):
        print(f'Applying {self.name}')

    async def downgrade(self):
        print(f'Unapplying {self.name}')
