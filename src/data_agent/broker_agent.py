import logging
import re

from amqp_fabric.amq_broker_connector import AmqBrokerConnector

from data_agent import __version__
from data_agent.api import ServiceApi
from data_agent.config_manager import (
    PersistentComponent,
    component_config_view,
    init_configuration,
)
from data_agent.config_template import (
    CONFIG_SECTION_BROKER,
    CONFIG_SECTION_CONNECTION_MANAGER,
    CONFIG_SECTION_DAQ_SCHEDULER,
    CONFIG_SECTION_LOG,
    CONFIG_SECTION_SAFE_MANIPULATOR,
    CONFIG_SECTION_SERVICE,
)
from data_agent.connection_manager import ConnectionManager
from data_agent.daq_scheduler import create_daq_scheduler
from data_agent.safe_manipulator import SafeManipulator

log = logging.getLogger(__name__)


class BrokerAgent:
    _config = None
    _broker_conn = None
    _connection_manager = None
    _safe_manipulator = None
    _scheduler = None

    async def init(self, loop, is_service=False, enable_persistance=True):
        self._config, _ = init_configuration(is_service, loop)
        service_config = component_config_view(self._config, CONFIG_SECTION_SERVICE)
        broker_config = component_config_view(self._config, CONFIG_SECTION_BROKER)
        log_config = component_config_view(self._config, CONFIG_SECTION_LOG)

        uri_pattern = re.compile(r"(\b(?:[a-z]{,5})://.*:)(.*)(@[^ \b]+)", re.MULTILINE)
        broker_uri_wo_pass = re.sub(uri_pattern, r"\1**********\3", broker_config.uri)

        log.info("************ Initializing Data Agent Service ***********************")
        log.info(f" Version:              {__version__}")
        log.info(f" Service Id:           {service_config.id}")
        log.info(
            f" FQN:                  {service_config.domain}.{service_config.type}.{service_config.id}"
        )
        log.info(f" Broker URI:           {broker_uri_wo_pass}")
        log.info(f" Config directory:     {self._config.config_dir()}")
        log.info(f' Logs path:            {log_config["handlers"]["file"]["filename"]}')
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

        self._connection_manager = ConnectionManager(
            PersistentComponent(
                self._config,
                CONFIG_SECTION_CONNECTION_MANAGER,
                enable_persistence=enable_persistance,
            )
        )
        self._safe_manipulator = SafeManipulator(
            self._connection_manager,
            PersistentComponent(
                self._config,
                CONFIG_SECTION_SAFE_MANIPULATOR,
                enable_persistence=enable_persistance,
            ),
        )
        self._scheduler = create_daq_scheduler(
            self._broker_conn,
            self._connection_manager,
            PersistentComponent(
                self._config,
                CONFIG_SECTION_DAQ_SCHEDULER,
                enable_persistence=enable_persistance,
            ),
        )
        api = ServiceApi(
            self._scheduler, self._connection_manager, self._safe_manipulator
        )
        await self._broker_conn.rpc_register(api)

        log.info("")
        log.info(
            "************ Data Agent Service Initialized *************************"
        )
        log.info(
            f" Supported connectors: {self._connection_manager.list_supported_connectors()}"
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
        self._scheduler = None
        log.info("")
        log.info(
            "************ Data Agent Service Gracefully Finished ********************"
        )
