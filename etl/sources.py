
class FileSourceMixin:
    def __init__(self, path: str, options: dict = None):
        self.path = path
        self.options = options or {}


class FileSinkMixin:
    def __init__(self, path: str, mode: str = "overwrite", options: dict = None):
        self.path = path
        self.mode = mode
        self.options = options or {}


class CSVSourceMixin(FileSourceMixin):
    def __init__(self, path: str, header: bool = True, infer_schema: bool = True, options: dict = None):
        base_options = {
            "header": header,
            "inferSchema": infer_schema,
        }
        if options:
            base_options.update(options)
        super().__init__(path, options=base_options)


class CSVSinkMixin(FileSinkMixin):
    def __init__(self, path: str, mode: str = "overwrite", header: bool = True, options: dict = None):
        base_options = {
            "header": str(header).lower()
        }
        if options:
            base_options.update(options)
        super().__init__(path, mode=mode, options=base_options)


class ParquetSourceMixin(FileSourceMixin):
    def __init__(self, path: str, options: dict = None):
        super().__init__(path, options=options)


class ParquetSinkMixin(FileSinkMixin):
    def __init__(self, path: str, mode: str = "overwrite", options: dict = None):
        super().__init__(path, mode=mode, options=options)

# TODO: S3 specific
# class S3PathMixin:


class KafkaSourceMixin:
    def __init__(self, servers: str, topic: str, starting_offsets="earliest", options: dict = None):
        self.servers = servers
        self.topic = topic
        self.starting_offsets = starting_offsets
        self.options = options or {}


class KafkaSinkMixin:
    def __init__(self, servers: str, topic: str, options: dict = None):
        self.servers = servers
        self.topic = topic
        self.options = options or {}
