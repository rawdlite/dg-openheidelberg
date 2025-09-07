import toml
import os

DEFAULT_CONFIGFILE = os.getenv("ONBOARDING_CONFIG", "/srv/live/dagster/config/config.toml")
print(f"**config:** {DEFAULT_CONFIGFILE}")

class Config:
    """
    get config from file

    """

    def __init__(self, configfile=DEFAULT_CONFIGFILE) -> None:
        self.configfile = configfile
        self.config = toml.load(configfile)

    def get(self, key) -> dict:
        return self.config.get(key, {})
