import argparse
import logging
import logging.config
import os
import pathlib
import platform
import sys

import aiodebug.log_slow_callbacks

from . import dist_name
from .config_persist import PersistConfig
from .config_template import config_template

log = logging.getLogger(__name__)


def init_configuration(is_service, loop=None, parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Data Agent")

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
    )
    parser.add_argument(
        "--verbose",
        "-debug",
        dest="verbose",
        action="store_true",
        help="print debugging messages",
    )

    # args = parser.parse_args()
    args, unknown_args = parser.parse_known_args()
    exec_dir = pathlib.Path(sys.executable).parent.resolve()

    if is_service:
        # Used internally by confuse package
        os.environ[f"{dist_name.upper()}DIR"] = str(exec_dir.joinpath("config"))

    config = PersistConfig(dist_name, f"data_agent.{sys.platform}")
    config.set_args(args, dots=True)

    log.debug(f"Configuration directory is {config.config_dir()}")
    # Initialize automatic values
    if not config["service"]["id"]:
        config["service"]["id"] = platform.node()

    # Use a boolean flag and the transient overlay.
    if config["verbose"]:
        log.info("Verbose mode")
        config["log"]["level"] = 2
    else:
        config["log"]["level"] = 0
    log.debug("Logging level is {}".format(config["log"]["level"].get(int)))

    # Configure log file path
    logs_dir = str(os.path.join(config.config_dir(), "logs"))

    # Create path if not exists
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    handlers = config["log"]["handlers"].get()
    handlers["file"]["filename"] = str(
        os.path.join(logs_dir, config["log"]["handlers"]["file"]["filename"].get(str))
    )
    handlers["err_file"]["filename"] = str(
        os.path.join(
            logs_dir, config["log"]["handlers"]["err_file"]["filename"].get(str)
        )
    )
    config["log"]["handlers"] = handlers

    # Validate configuration
    valid = config.get(config_template)

    log.debug(
        f"Logging file path {config['log']['handlers']['file']['filename'].get(str)}"
    )

    # # Persist the configuration
    # config_filename = os.path.join(config.config_dir(),
    #                                confuse.CONFIG_FILENAME)
    # log.debug(f'Persistence configuration file is {config_filename}')
    # with open(config_filename, 'w') as f:
    #     f.write(config.dump().strip())

    # Init logging

    # pp.pprint(valid['log'])
    logging.config.dictConfig(valid["log"])

    # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    # for l in loggers:
    #     print(l)

    # # Some validated/converted values.
    # print('library is', valid.library)
    # print('directory is', valid.paths.directory)
    # print('paths.default is', valid.paths.default)

    # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    # pp.pprint(loggers)

    # Apply extra configurations related to logs
    if config["trace"]["slow_callbacks"].get() > 0:
        aiodebug.log_slow_callbacks.enable(config["trace"]["slow_callbacks"].get())
        log.info("Slow callbacks tracing enabled!")

    # if config['trace.hang_inspection']:
    #     aiodebug.hang_inspection.enable(stack_output_dir=config.config_dir(),
    #                                     interval=config['trace.slow_callbacks'],
    #                                     loop=loop)
    if loop is not None and config["trace"]["asyncio_debug_mode"].get():
        loop.set_debug(enabled=True)
        log.info("Asyncio debug mode enabled!")

    return config, unknown_args


def component_config_view(config, component):
    return config[component].get(config_template[component])


class PersistentComponent:
    def __init__(self, config, component, enable_persistence=False):
        self._config = config
        self._component = component
        self._enable_persistence = enable_persistence

    def list_items(self):
        if self._enable_persistence:
            return component_config_view(self._config, self._component)

        return []

    def add_item(self, item_name, params):
        if self._enable_persistence:
            config_view = component_config_view(self._config, self._component)
            self._config.set({self._component: {**config_view, **{item_name: params}}})

    def update_item(self, item_name, params):
        if self._enable_persistence:
            config_view = component_config_view(self._config, self._component)
            if item_name not in config_view:
                self._config.set(
                    {self._component: {**config_view, **{item_name: params}}}
                )
            else:
                subitems = {**(config_view.get(item_name)), **params}
                self._config.set(
                    {self._component: {**config_view, **{item_name: subitems}}}
                )

    def remove_subitems(self, item_name, params):
        if self._enable_persistence:
            config_view = component_config_view(self._config, self._component)
            if item_name in config_view:
                subitems = config_view.get(item_name)
                for key in params:
                    subitems.pop(key)
                self._config.set(
                    {self._component: {**config_view, **{item_name: subitems}}}
                )

    def remove_item(self, item_name):
        if self._enable_persistence:
            config_view = component_config_view(self._config, self._component)
            del config_view[item_name]
            self._config.set({self._component: config_view})

    def update_subitem(self, item_name, subitem, params):
        if self._enable_persistence:
            config_view = component_config_view(self._config, self._component)
            if item_name not in config_view:
                self._config.set(
                    {self._component: {**config_view, **{item_name: {subitem: params}}}}
                )
            else:
                config_view[item_name][subitem] = params
                self._config.set({self._component: config_view})

    def remove_subitem(self, item_name, subitem):
        if self._enable_persistence:
            del self._config[self._component][item_name][subitem]
