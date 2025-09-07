import os
import unittest
from config import Config

class TestConfig(unittest.TestCase):

    def test_config_init_with_env(self):
        os.environ["ONBOARDING_CONFIG"] = "tests/test_config.toml"
        cfg = Config()
        self.assertEqual(cfg.get("section"), {"foo": "bar"})

    def test_config_init_with_param(self):
        cfg = Config("tests/test_config.toml")
        self.assertEqual(cfg.get("section"), {"foo": "bar"})
