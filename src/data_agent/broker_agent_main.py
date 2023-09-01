import asyncio
import logging

from data_agent.broker_agent import BrokerAgent

log = logging.getLogger(__name__)


def run_event_loop():
    agent = BrokerAgent()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(agent.init(loop))

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        loop.run_until_complete(agent.close())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    run_event_loop()
