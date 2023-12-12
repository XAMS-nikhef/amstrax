import datetime
import logging
import os
from git import Repo
from importlib import import_module

__all__ = ['get_daq_logger', 'DAQLogHandler', 'get_git_hash']

"""
This module contains the common logger for all DAQ software. It is
responsible for writing log messages to disk and to the console.  
Mainly copied from Joran's work on the DAQ of XENONnT.
"""

def get_daq_logger(name,
                   process_name=None,
                   level=logging.INFO,
                   opening_message=None,
                   **kwargs):
    """
    Common logger for DAQ-software
    :param name: name to be displayed in the logs, e.g. "main"
    :param process_name: name of the process, e.g. "bootstrax". Under
        this name the log will be written. If none is provided, use the 
        name argument (but dont do name="main" for obvious reasons).
    :param level: logging.DEBUG or logging.INFO etc, or "DEBUG" or "INFO" etc. Default INFO
    :param opening_message: Each time a new document is opened (because
        it's a new day, add this message to a log.info)
    :param **kwargs: directly passed to DAQLogHandler
    :return: logger
    """
    if process_name is None:
        if name == 'main':
            raise ValueError('Be a bit more informative, we don\'t want to be '
                             'logging to "main.log" whatever that means.')
        process_name = name
    logger = logging.getLogger(name)
    logger.addHandler(DAQLogHandler(process_name, opening_message=opening_message, **kwargs))
    if isinstance(level, str):
        level = getattr(logging, level)
    logger.setLevel(level)
    return logger


def get_git_hash(module: 'str') -> str:
    """
    Get the latest git hash from a module
    :param module: Name of the module to load, e.g. "daqnt"
    :return: the latest commit hash
    """
    mod = import_module(module)
    path = mod.__path__[0]
    repo = Repo(path, search_parent_directories=True)
    return repo.head.object.hexsha


class DAQLogHandler(logging.Handler):
    """Common logger logic for all DAQ software"""

    def __init__(self,
                 process_name: str,
                 mc=None,
                 opening_message=None,
                 logdir=os.path.join(os.environ['HOME'], 'daq', 'logs'),
                ):
        logging.Handler.__init__(self)
        self.opening_message=opening_message
        self.process_name = process_name
        now = datetime.datetime.utcnow()
        self.today = datetime.date(now.year, now.month, now.day)
        if not os.path.exists(logdir):
            raise OSError(f'Are you on the DAQ? {logdir} does not exist..')
        self.logdir = logdir
        self.Rotate(self.today)
        self.count = 0
        self.mc = mc

    def close(self):
        if hasattr(self, 'f') and not self.f.closed:
            self.f.flush()
            self.f.close()

    def __del__(self):
        self.close()

    def emit(self, record):
        """
        This function is responsible for sending log messages to their
        output destinations, in this case the console and disk

        :param record: logging.record, the log message
        :returns: None
        """
        msg_datetime = datetime.datetime.utcfromtimestamp(record.created)
        msg_today = datetime.date(msg_datetime.year, msg_datetime.month, msg_datetime.day)

        if msg_today != self.today:
            # if the log message is not from the same day as the logfile, rotate
            self.Rotate(msg_today)
        m = self.FormattedMessage(msg_datetime, record.levelname, record.funcName, record.lineno, record.getMessage())
        self.f.write(m)
        print(m[:-1])  # strip \n
        self.count += 1
        if self.count > 2:
            # a lot of implementations don't routinely flush data to disk
            # and we want to ensure that logs are readily available on disk, not sitting
            # around in a buffer somewhere. Redax does this with a timer because
            # logging happens from many threads, things logging via this module
            # generally don't
            self.f.flush()
            self.count = 0
        # if this is bad enough, push to the db/website
        if record.levelno >= logging.CRITICAL and self.mc is not None:
            try:
                self.mc.daq.log.insert_one(
                    {'user': self.process_name,
                     'message': record.getMessage(),
                     'priority': 4, 'runid': -1})
            except Exception as e:
                print(f'Database issue? Cannot log? {type(e)}, {e}')

    def Rotate(self, when):
        """
        This function makes sure that the currently-opened file has "today's" date. If "today"
        doesn't match the filename, open a new one

        :param when: datetime.date, the file-date that should be opened
        :returns: None
        """
        if hasattr(self, 'f'):
            self.f.close()

        self.f = open(self.FullFilename(when), 'a')
        self.today = datetime.date.today()
        m = self.FormattedMessage(datetime.datetime.utcnow(), "init", "logger", 0, "Opening a new file")
        self.f.write(m)
        if self.opening_message is not None:
            m = self.FormattedMessage(datetime.datetime.utcnow(),
                                      "init",
                                      "logger",
                                      0,
                                      self.opening_message
                                      )
            self.f.write(m)

    def FullFilename(self, when):
        """
        Returns the path and filename

        :param when: datetime.date, the file-date that should be opened
        :returns: os.path, the full path to the new file
        """
        day_dir = os.path.join(self.logdir, f"{when.year:04d}", f"{when.month:02d}.{when.day:02d}")
        os.makedirs(day_dir, exist_ok=True)
        return os.path.join(day_dir, self.Filename(when))

    def Filename(self, when):
        """
        The name of the file to log to

        :param when: datetime.date, unused
        :returns: str, name of the file (without any path information)
        """
        return f"{self.process_name}.log"

    def FormattedMessage(self, when, level, func_name, lineno, msg):
        """
        Formats the message for output: <timestamp> | <level> | <function> (L <lineno>) |  | <message>

        :param when: datetime.datetime, when the message was created
        :param level: str, the name of the level of this message
        :param msg: str, the actual message
        :param func_name: name of the calling function
        :param lineno: line number
        :returns: str, the formatted message
        """
        func_line = f'{func_name} (L{lineno})'
        return f"{when.isoformat(sep=' ')} | {str(level).upper():8} | {func_line:20} | {msg}\n"
    