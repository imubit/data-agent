import argparse
import logging
import logging.config
import os
import platform
import sys
import threading
from collections.abc import Mapping
from typing import Any

import aiodebug
from dynaconf import Dynaconf, loaders
from dynaconf.utils.boxing import DynaBox
from dynaconf.utils.inspect import get_history

DEFAULT_DYNAMIC_CONFIG_FILENAME = "config.yaml"

log = logging.getLogger(__name__)


def deep_diff(current, default):
    """Recursively find differences between current and default configs."""
    diff = {}
    for key, current_value in current.items():
        default_value = default.get(key, None)
        if isinstance(current_value, Mapping) and isinstance(default_value, Mapping):
            nested_diff = deep_diff(current_value, default_value)
            if nested_diff:
                diff[key] = nested_diff
        elif current_value != default_value:
            diff[key] = current_value
    return diff


class ConfigManager:
    def __init__(
        self, loop=None, parser=None, config_file: str = None, enable_persistence=True
    ):
        self._enable_persistence = enable_persistence
        self._lock = threading.RLock()
        self.base_path = self._determine_base_path(config_file)
        os.makedirs(self.base_path, exist_ok=True)
        self.dynamic_config = os.path.join(
            self.base_path, config_file or DEFAULT_DYNAMIC_CONFIG_FILENAME
        )
        log.debug(f"Configuration directory is {self.base_path}")

        # Configure log file path
        self.logs_dir = str(os.path.join(self.base_path, "logs"))

        self.settings = Dynaconf(
            envvar_prefix="DATA_AGENT",
            root_path=self.base_path,
            settings_files=[self._default_config_path(), self.dynamic_config, "*.yaml"],
            merge_enabled=True,
            environments=False,
            load_dotenv=True,
        )
        self._default_settings = next(
            item["value"]
            for item in get_history(self.settings)
            if item["loader"] == "yaml" and "config_default.yaml" in item["identifier"]
        )

        # Allow dot-notation access via proxy
        self.__dict__.update(self.settings)

        # Init command args
        self._init_cli_args(parser)

        # Initialize automatic values
        if not self.get("service.id"):
            self.set("service.id", platform.node())

        self._init_logging_config(loop=loop)

    def _init_cli_args(self, parser):
        if parser is None:
            parser = argparse.ArgumentParser(description="Data Agent")

        # parser.add_argument(
        #     "--env", "-e", help="Runtime environment", default=self.settings.current_env
        # )
        parser.add_argument(
            "--service.id",
            "-i",
            dest="service.id",
            metavar="AGENT_ID",
            help="Data agent service Id",
        )
        parser.add_argument(
            "--broker.uri",
            "-b",
            dest="broker.uri",
            metavar="BROKER_URI",
            help="AMQP broker URI",
            default=self.settings.broker.uri,
        )
        parser.add_argument(
            "--verbose",
            "-debug",
            dest="verbose",
            action="store_true",
            help="print debugging messages",
        )

        args, self.unknown_args = parser.parse_known_args()
        # self.settings.setenv(args.env)
        self.settings.update(vars(args))

    def _init_logging_config(self, loop):
        if self.settings.verbose:
            log.info("Verbose mode")
            self.settings.set("log.level", 2)
        else:
            self.settings.set("log.level", 0)
        log.debug(f"Logging level is {self.settings.log.level}")

        os.makedirs(self.logs_dir, exist_ok=True)

        handlers = self.settings.get("log.handlers")
        handlers["file"]["filename"] = str(
            os.path.join(self.logs_dir, self.settings.get("log.handlers.file.filename"))
        )
        handlers["err_file"]["filename"] = str(
            os.path.join(
                self.logs_dir, self.settings.get("log.handlers.err_file.filename")
            )
        )
        self.settings.set("log.handlers", handlers)

        log.debug(
            f"Logging file path {self.settings.get('log.handlers.file.filename')}"
        )

        logging.config.dictConfig(self.settings.get("log"))

        if self.settings.get("trace.slow_callbacks") > 0:
            aiodebug.log_slow_callbacks.enable(
                self.settings.get("trace.slow_callbacks")
            )
            log.info("Slow callbacks tracing enabled!")

        if loop is not None and self.settings.get("trace.asyncio_debug_mode"):
            loop.set_debug(enabled=True)
            log.info("Asyncio debug mode enabled!")

    def _default_config_path(self) -> str:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(module_dir, sys.platform, "config_default.yaml")

    def _determine_base_path(self, config_file: str = None) -> str:
        if config_file:
            return os.path.dirname(config_file)
        if platform.system() == "Windows":
            if self._is_windows_service():
                return os.path.dirname(sys.executable)
            return os.path.join(os.getenv("APPDATA"), "data-agent")
        return "/etc/data-agent"

    def _is_windows_service(self) -> bool:
        return sys.executable.endswith("exe") and not sys.stdout.isatty()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self.settings.get(key, default)

    def set(self, key: str, value: Any, persist: bool = True):
        with self._lock:
            self.settings.set(key, value)
            if persist:
                self._persist()

    def remove(self, key: str, persist: bool = True):
        with self._lock:
            keys = key.split(".")
            cfg = self.settings
            for k in keys[:-1]:
                cfg = cfg.get(k)
                if cfg is None:
                    return  # Key path does not exist, nothing to remove

            # Finally remove only the leaf
            if isinstance(cfg, dict):
                cfg.pop(keys[-1], None)
            else:
                try:
                    delattr(cfg, keys[-1])
                except AttributeError:
                    pass

            if persist:
                self._persist()

    def persist(self):
        with self._lock:
            self._persist()

    def _persist(self):
        if self._enable_persistence:
            current = self.settings.as_dict()
            diff = deep_diff(current, self._default_settings)
            loaders.write(self.dynamic_config, DynaBox(diff), merge=True)

    def reload(self):
        with self._lock:
            self.settings.reload()

    def __getattr__(self, name):
        with self._lock:
            return getattr(self.settings, name)
