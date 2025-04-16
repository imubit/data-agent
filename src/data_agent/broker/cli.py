import argparse
import asyncio
import logging

from amqp_fabric.amq_broker_connector import AmqBrokerConnector

from data_agent import __version__
from data_agent.broker.instr_exec import cli_exec
from data_agent.broker.instr_list_services import cli_list_services
from data_agent.config_manager import ConfigManager

log = logging.getLogger(__name__)


async def create_amq_broker_connector(config, keep_alive_listen):
    config_broker = config.get("broker")
    config_service = config.get("service")

    amq = AmqBrokerConnector(
        amqp_uri=config_broker.uri,
        service_domain=config_service.domain,
        service_id=config_service.id,
        service_type=config_service.type,
        keep_alive_listen=keep_alive_listen,
    )
    await amq.open(timeout=config_broker.timeout)
    return amq


def run():
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(description="Broker Connected Data Agent CLI")

    config = ConfigManager(loop=loop, parser=parser)

    # Initialize subparser after ConfigManager identified configurable settings
    subparsers = parser.add_subparsers(dest="instruction", help="Execute API call")
    subparsers.add_parser("exec")
    subparsers.add_parser("list_services")

    known_args, _ = parser.parse_known_args()
    print(known_args)

    config_service = config.get("service")

    log.info(
        f'***** Broker Connected Agent CLI: "{config_service.id}", '
        f'Instruction: "{known_args.instruction}", Ver: {__version__} ******'
    )

    broker = loop.run_until_complete(
        create_amq_broker_connector(
            config=config, keep_alive_listen=(known_args.instruction == "list_services")
        )
    )

    task = None

    if known_args.instruction == "exec":
        task = cli_exec(broker, config)
    elif known_args.instruction == "list_services":
        task = cli_list_services(broker, config)
    else:
        raise Exception("Unknown instruction")

    loop.run_until_complete(task)
    loop.run_until_complete(broker.close())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


if __name__ == "__main__":
    run()
