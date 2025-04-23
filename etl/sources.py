
class FileSourceMixin:
    def __init__(self, path: str, options: dict = None):
        self.path = path
        self.options = options or {}


class CSVSourceMixin(FileSourceMixin):
    def __init__(self, path: str, header: bool = True, options: dict = None):
        base_options = {
            "header": header,
        }
        if options:
            base_options.update(options)
        super().__init__(path, options=base_options)


class ParquetSourceMixin(FileSourceMixin):
    def __init__(self, path: str, options: dict = None):
        super().__init__(path, options=options)


class KafkaSourceMixin:
    def __init__(self, servers: str, topic: str,  options: dict = None):
        self.servers = servers
        self.topic = topic
        self.options = options or {}

        # TODO: add to reader: starting_offsets="earliest",
