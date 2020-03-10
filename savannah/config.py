import sqlalchemy
from importlib import import_module


class Config:
    def __init__(self, metadata: str):
        self.metadata = metadata

    def get_initial_state(self):
        return {"metadata": sqlalchemy.MetaData()}

    def get_current_state(self):
        module_str, _, attr_str = self.metadata.partition(":")
        module = import_module(module_str)
        metadata = getattr(module, attr_str)
        return {"metadata": metadata}

    def write_config_to_disk(self, path):
        with open(path, "w") as fout:
            fout.write(
                f"""\
import savannah


config = savannah.Config(metadata={self.metadata!r})
"""
            )


def load_config(dir: str = "migrations") -> Config:
    module = import_module(dir)
    config = getattr(module, "config")
    assert isinstance(config, Config)
    return config
