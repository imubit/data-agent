import argparse
import ast
import logging
import pprint
import time
import traceback

from data_agent import __version__
from data_agent.api import ServiceApi
from data_agent.config_manager import ConfigManager
from data_agent.connection_manager import ConnectionManager
from data_agent.exchanger import DataExchanger
from data_agent.safe_manipulator import SafeManipulator

log = logging.getLogger(__name__)

global_kwargs = []


def build_kwargs(args):
    args = {i.split("=")[0]: i.split("=")[1] for i in args}

    ret = {
        k[2:]: args[k]
        for k in args
        if k.startswith("--") and k[2:] not in global_kwargs and args[k] is not None
    }

    # Convert types
    for k in ret:
        if not isinstance(ret[k], str):
            continue

        try:
            ret[k] = ast.literal_eval(ret[k])

        except ValueError:
            pass
        except SyntaxError:
            pass

    return ret


def print_formatted_result(api_func, command, kwargs):
    print("------------------------------------------------")
    print(f"Executing command: {command}")
    # print('      args:{0}'.format(args['<args>']))
    print(f"    kwargs:{kwargs}")
    print("------------------------------------------------\n\r")

    try:
        start_time = time.time()
        result = api_func(**kwargs)
        elapsed_time = time.time() - start_time

        # if cm.expired:
        #     print('\n\r------------------------------------------------')
        #     print('Error: Timeout executing command."')
        #     return

        if isinstance(result, list):
            for n in result:
                print(n)
        else:
            pprint.pprint(result)

        print("\n\r------------------------------------------------")
        print(
            'Command "{0}" executed successfully ({1:.2f} sec)!'.format(
                command, elapsed_time
            )
        )

    except Exception:
        print("\n\r")
        traceback.print_exc()
        print("\n\r------------------------------------------------")


def exec_command(config, cmd, kwargs):
    connection_manager = ConnectionManager(config=config)

    safe_manipulator = SafeManipulator(connection_manager, config=config)

    exchanger = DataExchanger(connection_manager)

    api = ServiceApi(
        scheduler=None,
        connection_manager=connection_manager,
        data_exchanger=exchanger,
        safe_manipulator=safe_manipulator,
    )

    api_func = getattr(api, cmd)

    print_formatted_result(api_func, cmd, kwargs)


def run():
    parser = argparse.ArgumentParser(
        description="Local Data Agent CLI."
        "This tool is not connecting to any broker or "
        "external clients. It is used to execute API calls directly "
        "from the command lineda"
    )

    config = ConfigManager(loop=None, parser=parser, enable_persistence=True)

    subparsers = parser.add_subparsers(dest="instruction", help="Execute API call")
    subparsers.add_parser("exec")

    known_args, unknown = parser.parse_known_args()

    log.info(
        f'***** Local CLI: Instruction: "{known_args.instruction}", Ver: {__version__} ******'
    )

    if known_args.instruction == "exec":
        exec_command(
            config=config,
            cmd=config.unknown_args[1],
            kwargs=build_kwargs(config.unknown_args[2:]),
        )


if __name__ == "__main__":
    run()
