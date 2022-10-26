import logging

msg_format = "[%(asctime)s] %(levelname)s - %(message)s"
logger = logging
logger.basicConfig(format=msg_format, level=logging.INFO)