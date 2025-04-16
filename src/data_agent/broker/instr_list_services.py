import asyncio
from pprint import pprint


async def cli_list_services(broker, config):
    # Sleep to collect keep alives
    print("Waiting 5 seconds for keep alives...")
    await asyncio.sleep(5)

    res = broker.list_services(health_check=True)

    print("Detected services:")
    pprint(res)
