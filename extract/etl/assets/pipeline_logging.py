"""
Provides logging capabilities
"""

import time
from loguru import logger

class PipelineLogging:
    """
    Provides logging capabilities

    Methods:
        log_to_file: write the log message to file and to the console
        get_logs: return a concatenated list of all the log files in the logs directory
    """

    def __init__(self, pipeline_name: str, log_path: str):
        """
        Args:
            pipeline_name: The pipeline name which is used as sub-folder within the logs root |
                folder to isolate the logs for this pipeline
            file_path: The root file path for logs
        """

        self.pipeline_name = pipeline_name
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        self.file_path = f"{log_path}/{pipeline_name}_{timestamp}.log"
        logger.add(self.file_path, level="INFO", rotation="50 MB", retention="10 days")

    # write the log message to file and to the console
    def log_to_file(self, message: str):
        """
        Logs a message to file and to the console

        Args:
            message: Message string to log 
        """

        logger.info(message)

    # return a concatenated list of all the log files in the logs directory
    def get_logs(self) -> str:
        """
        Returns a concatenated string of all current log files

        Returns:
            Concatenated string of all current log files
        """

        with open(self.file_path, "r", encoding='utf8') as file:
            return "".join(file.readlines())
