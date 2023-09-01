"""
SMWinservice
by Davide Mastromatteo (http://thepythoncorner.com/dev/how-to-create-a-windows-service-in-python/)

Base class to create winservice in Python
-----------------------------------------

Instructions:

1. Just create a new class that inherits from this base class
2. Define into the new class the variables
   _svc_name_ = "nameOfWinservice"
   _svc_display_name_ = "name of the Winservice that will be displayed in scm"
   _svc_description_ = "description of the Winservice that will be displayed in scm"
3. Override the three main methods:
    def start(self) : if you need to do something at the service initialization.
                      A good idea is to put here the inizialization of the running condition
    def stop(self)  : if you need to do something just before the service is stopped.
                      A good idea is to put here the invalidation of the running condition
    def main(self)  : your actual run loop. Just create a loop based on your running condition
4. Define the entry point of your module calling the method "parse_command_line" of the new class
5. Enjoy
"""

import os
import socket
import sys
from contextlib import closing

import pywintypes
import servicemanager
import win32api
import win32con
import win32event
import win32profile
import win32service
import win32serviceutil


class SMWinservice(win32serviceutil.ServiceFramework):
    """Base class to create winservice in Python"""

    _svc_name_ = "pythonService"
    _svc_display_name_ = "Python Service"
    _svc_description_ = "Python Service Description"

    @classmethod
    def parse_command_line(cls):
        """
        ClassMethod to parse the command line
        """
        win32serviceutil.HandleCommandLine(
            cls, customOptionHandler=SMWinservice.post_service_update
        )

    # https://stackoverflow.com/questions/41200068/python-windows-service-error-starting-service-the-service-did-not-respond-to-t
    @classmethod
    def post_service_update(cls, *args):
        env_reg_key = "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment"
        hkey = win32api.RegOpenKeyEx(
            win32con.HKEY_LOCAL_MACHINE, env_reg_key, 0, win32con.KEY_ALL_ACCESS
        )

        with closing(hkey):
            system_path = win32api.RegQueryValueEx(hkey, "PATH")[0]
            # PATH may contain %SYSTEM_ROOT% or other env variables that must be expanded
            # ExpandEnvironmentStringsForUser(None) only expands System variables
            system_path = win32profile.ExpandEnvironmentStringsForUser(
                None, system_path
            )
            system_path_list = system_path.split(os.pathsep)

            core_dll_file = win32api.GetModuleFileName(sys.dllhandle)
            core_dll_name = os.path.basename(core_dll_file)

            for search_path_dir in system_path_list:
                try:
                    dll_path = win32api.SearchPath(search_path_dir, core_dll_name)[0]
                    print(f"System python DLL: {dll_path}")
                    break
                except pywintypes.error as ex:
                    if ex.args[1] != "SearchPath":
                        raise
                    continue
            else:
                print("*** WARNING ***")
                print(
                    f"Your current Python DLL ({core_dll_name}) is not in your SYSTEM PATH"
                )
                print("The service is likely to not launch correctly.")

        pythonservice_exe = win32serviceutil.LocatePythonServiceExe()
        pywintypes_dll_file = pywintypes.__spec__.origin

        pythonservice_path = os.path.dirname(pythonservice_exe)
        pywintypes_dll_name = os.path.basename(pywintypes_dll_file)

        try:
            return win32api.SearchPath(pythonservice_path, pywintypes_dll_name)[0]
        except pywintypes.error as ex:
            if ex.args[1] != "SearchPath":
                raise
            print("*** WARNING ***")
            print(
                f"{pywintypes_dll_name} is not is the same directory as pythonservice.exe"
            )
            print(f'Copy "{pywintypes_dll_file}" to "{pythonservice_path}"')
            print("The service is likely to not launch correctly.")

    def __init__(self, args):
        """
        Constructor of the winservice
        """
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        """
        Called when the service is asked to stop
        """
        self.stop()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        """
        Called when the service is asked to start
        """
        self.start()
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        self.main()

    def start(self):
        """
        Override to add logic before the start
        eg. running condition
        """
        pass

    def stop(self):
        """
        Override to add logic before the stop
        eg. invalidating running condition
        """
        pass

    def main(self):
        """
        Main class to be ovverridden to add logic
        """
        pass


# entry point of the module: copy and paste into the new module
# ensuring you are calling the "parse_command_line" of the new created class
if __name__ == "__main__":
    SMWinservice.parse_command_line()
