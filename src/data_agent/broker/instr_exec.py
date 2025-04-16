import ast
import pprint
import time

import six
from aio_pika.exceptions import DeliveryError
from async_timeout import timeout

global_kwargs = []


async def run_command(command, args, proxy):
    start_time = time.time()

    res = await proxy.func(method_name=command, kwargs=build_kwargs(args), expiration=3)

    elapsed_time = time.time() - start_time
    return res, elapsed_time


def build_kwargs(args):
    args = {i.split("=")[0]: i.split("=")[1] for i in args}

    ret = {
        k[2:]: args[k]
        for k in args
        if k.startswith("--") and k[2:] not in global_kwargs and args[k] is not None
    }

    # Convert types
    for k in ret:
        if not isinstance(ret[k], six.string_types):
            continue

        try:
            ret[k] = ast.literal_eval(ret[k])

        except ValueError:
            pass
        except SyntaxError:
            pass

    return ret


async def print_formatted_result(
    command, args, broker, service_domain, service_id, service_type
):
    print(f"Domain: {service_domain}")
    print(f"Service Type: {service_type}")
    print(f"Service Id: {service_id}")
    print(f"Executing command: {command}")
    # print('      args:{0}'.format(args['<args>']))
    print(f"    kwargs:{build_kwargs(args)}")
    print("------------------------------------------------\n\r")

    proxy = await broker.rpc_proxy(service_domain, service_id, service_type)

    try:
        async with timeout(2) as cm:
            result, elapsed_time = await run_command(command, args, proxy)

        if cm.expired:
            print("\n\r------------------------------------------------")
            print('Error: Timeout executing command."')
            return

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

    except DeliveryError:
        print(
            "Error: Unrecognized target domain / service_id / service_type / function."
        )
        print("\n\r------------------------------------------------")


async def cli_exec(broker, config):
    config_service = config.get("service")

    service_id = config_service.id
    service_domain = config_service.domain
    service_type = config_service.type

    await print_formatted_result(
        config.unknown_args[0],
        config.unknown_args[1:] if len(config.unknown_args) > 1 else [],
        broker,
        service_domain,
        service_id,
        service_type,
    )
