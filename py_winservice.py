import multiprocessing
import os

import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_watchdog import WatchDogObServer


class TestService(win32serviceutil.ServiceFramework):
    _svc_name_ = "WatchDogService"
    _svc_display_name_ = "WatchDog Service"
    _svc_description_ = "WatchDog for CCLAS "

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        if os.sys.platform.startswith('win'):
            multiprocessing.freeze_support()

        config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
        logger = LoggerFactory(config=config).getLogger()
        wObserver = WatchDogObServer(config=config, logger=logger)
        wObserver.start()
        rc = None
        while rc != win32event.WAIT_OBJECT_0:
            with open('C:\\TestService.log', 'a') as f:
                f.write('test service running...\n')
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TestService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TestService)