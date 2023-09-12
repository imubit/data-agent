import asyncio
import sys

import servicemanager
import win32serviceutil

from data_agent.broker_agent import BrokerAgent
from data_agent.connection_manager import ConnectionManager
from data_agent.win32.win_service import SMWinservice


class DataAgentService(SMWinservice):
    _svc_name_ = "data_agent"
    _svc_display_name_ = "Data Agent"
    _svc_description_ = f"Data Agent with {ConnectionManager.list_supported_connectors()} connectors supported."
    _loop = None

    def start(self):
        # self.isrunning = True
        # servicemanager.LogInfoMsg(f'{self._svc_name_} configuration directory is {config.config_dir()}')
        pass

    def stop(self):
        # self.isrunning = False
        self._loop.stop()

    def main(self):
        agent = BrokerAgent()
        self._loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(agent.init(self._loop, is_service=True))

        try:
            self._loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self._loop.run_until_complete(agent.close())
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Called by Windows shell. Handling arguments such as: Install, Remove, etc.
        win32serviceutil.HandleCommandLine(DataAgentService)
    else:
        # Called by Windows Service. Initialize the service to communicate with the system operator
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(DataAgentService)
        servicemanager.StartServiceCtrlDispatcher()
