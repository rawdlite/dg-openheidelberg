import toml
import os

#TODO: rename shared config path to reflect that it is not only for mcp
DEFAULT_CONFIGFILE = os.path.expanduser(os.path.join('~','.config','mcp','config.toml'))

class Config:
    """
    get config from file

    """

    def __init__(self, configfile=DEFAULT_CONFIGFILE) -> None:
        self.configfile = configfile
        self.config = toml.load(configfile)

    def get(self, key) -> dict | None:
        return self.config.get(key)