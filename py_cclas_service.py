import multiprocessing
import os
import socket
import sys

import servicemanager
import win32event
import win32service
import win32serviceutil

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_watchdog import WatchDogObServer


# Watchdog for CCLAS Windows Service 版本
class WatchDogService(win32serviceutil.ServiceFramework):
    _svc_name_ = "WatchDogService"
    _svc_display_name_ = "Data Grabbing Robot"
    _svc_description_ = "Data Grabbing Robot for CCLAS By sinomine.com.cn"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.wObserver.stop()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        if os.sys.platform.startswith('win'):
            multiprocessing.freeze_support()

        config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
        logger = LoggerFactory(config=config).getLogger()
        self.wObserver = WatchDogObServer(config=config, logger=logger)
        self.wObserver.start()

        rc = None
        while rc != win32event.WAIT_OBJECT_0:
            # with open('e:\\TestService.log', 'a') as f:
            #     f.write('test service running...\n')
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(WatchDogService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(WatchDogService)
