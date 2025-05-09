import logging

from data_agent import __version__
from data_agent.api import ServiceApi
from data_agent.config_manager import ConfigManager
from data_agent.connection_manager import ConnectionManager
from data_agent.exchanger import DataExchanger
from data_agent.safe_manipulator import SafeManipulator

log = logging.getLogger(__name__)


class LocalAgent:
    _config = None
    _broker_conn = None
    _connection_manager = None
    _safe_manipulator = None
    _scheduler = None
    _exchanger = None
    _api = None
    _is_service = None

    def __init__(self, is_service=False, enable_persistance=True):
        self._is_service = is_service
        self._enable_persistance = enable_persistance

    def open(self):
        self._config = ConfigManager(
            loop=None, enable_persistence=self._enable_persistance
        )

        service_config = self._config.get("service")

        log.info("************ Initializing Data Agent Service ***********************")
        log.info(f" Version:              {__version__}")
        log.info(f" Service Id:           {service_config.id}")
        log.info(
            f" FQN:                  {service_config.domain}.{service_config.type}.{service_config.id}"
        )
        log.info(f" Config directory:     {self._config.base_path}")
        log.info(
            f" Logs path:            {self._config.get('log.handlers.file.filename')}"
        )
        log.info(
            "***********************************************************************"
        )

        self._connection_manager = ConnectionManager(self._config)
        self._safe_manipulator = SafeManipulator(
            self._connection_manager, config=self._config
        )

        self._exchanger = DataExchanger(self._connection_manager)

        self._api = ServiceApi(
            scheduler=None,
            connection_manager=self._connection_manager,
            data_exchanger=self._exchanger,
            safe_manipulator=self._safe_manipulator,
        )

        log.info("")
        log.info(
            "************ Data Agent Service Initialized *************************"
        )
        log.info(
            f" Supported connectors: {list(self._connection_manager.list_supported_connectors().keys())}"
        )
        log.info(
            "***********************************************************************"
        )

    @property
    def api(self):
        return self._api

    def close(self):
        if self._connection_manager:
            log.info(
                "************ Starting Agent Termination *************************"
            )
            self._connection_manager.close()
            self._config = None
            self._broker_conn = None
            self._connection_manager = None
            self._exchanger = None
            self._safe_manipulator = None
            self._scheduler = None
            log.info("")
            log.info(
                "************ Data Agent Service Gracefully Finished ********************"
            )

    def __enter__(self):
        try:
            self.open()
        except Exception as ex:
            self.close()
            raise ex
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
