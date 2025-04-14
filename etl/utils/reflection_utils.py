import importlib

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_class_from_string(path: str):
    logger.info(f"Inferring the class by name: {path}")
    module_path, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def load_class_from_yaml(config):
    cls = load_class_from_string(config["klass"])
    return cls(**{k: v for k, v in config.items() if k != "klass"})