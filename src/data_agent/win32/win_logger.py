from logging import StreamHandler

import servicemanager


class WinLogHandler(StreamHandler):
    def emit(self, record):
        msg = self.format(record)[:1024]
        if servicemanager.RunningAsService():
            servicemanager.LogInfoMsg(msg)
