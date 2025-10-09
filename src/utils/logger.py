import logging


def get_logger(name: str, log_file: str = "project.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger
