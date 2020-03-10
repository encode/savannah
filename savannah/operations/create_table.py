class CreateTable:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        class_name = self.__class__.__name__

        column_repr = ""
        for column in self.columns:
            if column.primary_key:
                column_repr += f"sqlalchemy.Column({column.name!r}, sqlalchemy.{column.type!r}, primary_key=True), "
            else:
                column_repr += (
                    f"sqlalchemy.Column({column.name!r}, sqlalchemy.{column.type!r}), "
                )
        column_repr = column_repr.rstrip(", ")

        return f"savannah.{class_name}(table_name={self.table_name!r}, columns=[{column_repr}])"
