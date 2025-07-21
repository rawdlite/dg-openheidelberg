import toml
import os

DEFAULT_CONFIGFILE = os.path.expanduser(os.path.join('~','.config','dg-openheidelberg','config.toml'))

class Config:
    """
    get config from file

    """

    def __init__(self, configfile=DEFAULT_CONFIGFILE) -> None:
        self.configfile = configfile
        self.config = toml.load(configfile)

    def get(self, key) -> dict | str:
        return self.config[key]