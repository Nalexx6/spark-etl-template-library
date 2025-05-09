import yaml
import logging

from etl.readers.reader_factory import ReaderFactory
from etl.utils import reflection_utils as ru


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Registry:

    def __init__(self, registry_config_filepath: str = None):

        if registry_config_filepath:
            with open(registry_config_filepath, "r") as f:
                additional_registry_config: dict = yaml.safe_load(f)["registry"]
        else:
            additional_registry_config = {}

        additional_registry = {}

        for (entity_type, entity_conf) in additional_registry_config.items():

            tmp_dict = {}

            for (k, v) in entity_conf.items():
                tmp_dict.update(**{k: ru.load_class_from_string(v)})

            additional_registry.update(**{entity_type: tmp_dict})

        self.reader_factory = ReaderFactory(additional_registry.get("readers", None))

        def get_reader_factory() -> ReaderFactory:
            return self.reader_factory
