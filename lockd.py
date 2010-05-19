#!/usr/bin/env python

from twisted.internet import protocol, reactor
from struct import unpack, pack
from time import time
import sys
import logging
import logging.config

LOG_FILENAME = '/var/log/lockd.log'

# Timeout in milliseconds.
TIMEOUT = 250
logger = None

class LockdProtocol(protocol.Protocol):

    def dataReceived(self, line):
        (code, file) = unpack('i255sx', line)
        success = True
        if code == 0:
            success = self.factory.lock(file)
        elif code == 1:
            self.factory.unlock(file)
        
        if success:
            self.transport.write(pack('i', 0))
        else:
            self.transport.write(pack('i', 1))

class LockdFactory(protocol.ServerFactory):
    protocol = LockdProtocol

    def __init__(self):
        self.lockmanager = LockManager()

    def lock(self, file):
        return self.lockmanager.lock(file)

    def unlock(self, file):
        self.lockmanager.unlock(file)

class LockManager:

    def __init__(self):
        self.locks = {}

    def lock(self, file):
        """Request a lock on a file.

           @param file the file that should be locked.
           @returns True the file was locked.
                    False the file could not be locked.

           Create a lock manager.
           >>> l = LockManager()

           Request a lock on a file.
           >>> l.lock('my file')
           True

           Now request a lock on the same file.
           >>> l.lock('my file')
           False

           An expired lock should be released.
           >>> l.locks['expired'] = 1           
           >>> l.lock('expired')
           True
        """
        if self.locks.has_key(file):
            t = self.locks[file]
            diff = time() - t
            if diff * 1000 < TIMEOUT:
                return False
            else:
                if logger:
		  logger.warn("Lock expired for file %s,  the difference is %s ms." % (file, diff))
                return True
        self.locks[file] = time()
        return True

    def unlock(self, file):
        """Request to unlock a file.

           @param file the file to unlock
           
           Create a lock manager.
           >>> l = LockManager()
           >>> l.lock('lock')
           True
           >>> l.unlock('lock')
           >>> l.lock('lock')
           True

           Unlock a unknown file.
           >>> l.unlock('unknown')        
        """
        if self.locks.has_key(file):
            del self.locks[file]
        
if __name__ == '__main__':
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        import doctest
        doctest.testmod()
    else:
	logger = logging.getLogger('lockd')
	logger.setLevel(logging.WARN)
	handler = logging.handlers.RotatingFileHandler(
        	      LOG_FILENAME, maxBytes=20000000, backupCount=5)
	logger.addHandler(handler)

        reactor.listenTCP(1500, LockdFactory())
        reactor.run()
