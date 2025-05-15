import logging
import re

from amqp_fabric.amq_broker_connector import AmqBrokerConnector

from data_agent import __version__
from data_agent.api import ServiceApi
from data_agent.config_manager import ConfigManager
from data_agent.connection_manager import ConnectionManager
from data_agent.daq_scheduler import create_daq_scheduler
from data_agent.exchanger import DataExchanger
from data_agent.safe_manipulator import SafeManipulator

log = logging.getLogger(__name__)


class BrokerAgent:
    _config = None
    _broker_conn = None
    _connection_manager = None
    _safe_manipulator = None
    _data_exchanger = None
    _scheduler = None

    async def init(self, loop, is_service=False, enable_persistence=True):
        self._config = ConfigManager(loop=loop, enable_persistence=enable_persistence)

        service_config = self._config.get("service")
        broker_config = self._config.get("broker")

        uri_pattern = re.compile(r"(\b(?:[a-z]{,5})://.*:)(.*)(@[^ \b]+)", re.MULTILINE)
        broker_uri_wo_pass = re.sub(uri_pattern, r"\1**********\3", broker_config.uri)

        log.info("************ Initializing Data Agent Service ***********************")
        log.info(f" Version:              {__version__}")
        log.info(f" Service Id:           {service_config.id}")
        log.info(
            f" FQN:                  {service_config.domain}.{service_config.type}.{service_config.id}"
        )
        log.info(f" Broker URI:           {broker_uri_wo_pass}")
        log.info(f" Config directory:     {self._config.base_path}")
        log.info(
            f" Logs path:            {self._config.get('log.handlers.file.filename')}"
        )
        log.info(
            "***********************************************************************"
        )

        self._broker_conn = AmqBrokerConnector(
            amqp_uri=broker_config.uri,
            service_domain=service_config.domain,
            service_id=service_config.id,
            service_type=service_config.type,
            keep_alive_seconds=service_config.keep_alive_seconds,
        )
        await self._broker_conn.open(timeout=broker_config.timeout)

        # Init AMQP Log handler
        for handler in log.handlers:
            if handler.get_name() == "amqp":
                await self._broker_conn.init_logging_handler(handler)

        self._connection_manager = ConnectionManager(config=self._config)
        self._safe_manipulator = SafeManipulator(
            connection_manager=self._connection_manager,
            config=self._config,
        )
        self._scheduler = create_daq_scheduler(
            broker=self._broker_conn,
            conn_manager=self._connection_manager,
            config=self._config,
        )
        self._data_exchanger = DataExchanger(self._connection_manager)
        api = ServiceApi(
            self._scheduler,
            self._connection_manager,
            self._data_exchanger,
            self._safe_manipulator,
        )
        await self._broker_conn.rpc_register(api)

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

    async def close(self):
        log.info("************ Starting Agent Termination *************************")
        self._scheduler.shutdown()
        self._connection_manager.close()
        await self._broker_conn.close()
        self._config = None
        self._broker_conn = None
        self._connection_manager = None
        self._safe_manipulator = None
        self._data_exchanger = None
        self._scheduler = None
        log.info("")
        log.info(
            "************ Data Agent Service Gracefully Finished ********************"
        )
