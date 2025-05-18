import logging

from etl.inputs.input_factory import InputFactory
from etl.outputs.output_factory import OutputFactory
from etl.readers.reader_factory import ReaderFactory
from etl.utils import reflection_utils as ru, load_yaml
from etl.writers.writer_factory import WriterFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Registry:

    def __init__(self, registry_config_filepath: str = None):

        if registry_config_filepath:
            additional_registry_config = load_yaml(registry_config_filepath)["registry"]
        else:
            additional_registry_config = {}

        additional_registry = {}

        for (entity_type, entity_conf) in additional_registry_config.items():

            tmp_dict = {}

            for (k, v) in entity_conf.items():
                tmp_dict.update(**{k: ru.load_class_from_string(v)})

            additional_registry.update(**{entity_type: tmp_dict})

        self.reader_factory = ReaderFactory(additional_registry.get("readers", None))
        self.writer_factory = WriterFactory(additional_registry.get("writers", None))
        self.input_factory = InputFactory(additional_registry.get("inputs", None))
        self.output_factory = OutputFactory(additional_registry.get("outputs", None))

        def get_reader_factory() -> ReaderFactory:
            return self.reader_factory

        def get_writer_factory() -> WriterFactory:
            return self.writer_factory

        def get_input_factory() -> InputFactory:
            return self.input_factory

        def get_output_factory() -> OutputFactory:
            return self.output_factory

