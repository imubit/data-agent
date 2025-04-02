import argparse
import asyncio
import logging

from amqp_fabric.amq_broker_connector import AmqBrokerConnector

from data_agent import __version__
from data_agent.broker.instr_exec import cli_exec
from data_agent.broker.instr_list_services import cli_list_services
from data_agent.config_manager import component_config_view, init_configuration
from data_agent.config_template import CONFIG_SECTION_BROKER, CONFIG_SECTION_SERVICE

# logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
# logging.getLogger().addHandler(logging.StreamHandler())

log = logging.getLogger(__name__)


async def create_amq_broker_connector(config, keep_alive_listen):
    config_broker = component_config_view(config, CONFIG_SECTION_BROKER)
    config_service = component_config_view(config, CONFIG_SECTION_SERVICE)

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
    subparsers = parser.add_subparsers(dest="instruction", help="Execute API call")
    subparsers.add_parser("exec")
    subparsers.add_parser("list_services")

    config, unrecognized_args = init_configuration(
        loop=loop, is_service=False, parser=parser
    )
    known_args, _ = parser.parse_known_args()

    config_service = component_config_view(config, CONFIG_SECTION_SERVICE)

    log.info(
        '***** Broker Connected Agent CLI: "{}", Instruction: "{}", Ver: {} ******'.format(
            config_service["id"], known_args.instruction, __version__
        )
    )

    broker = loop.run_until_complete(
        create_amq_broker_connector(
            config=config, keep_alive_listen=(known_args.instruction == "list_services")
        )
    )

    task = None

    if known_args.instruction == "exec":
        task = cli_exec(broker, config, unrecognized_args)
    elif known_args.instruction == "list_services":
        task = cli_list_services(broker, config, unrecognized_args)
    else:
        raise Exception("Unknown instruction")

    loop.run_until_complete(task)

    loop.run_until_complete(broker.close())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


if __name__ == "__main__":
    run()
