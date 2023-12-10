import dagster


class ObservationConfig(dagster.PermissiveConfig):
    date: str
    raw_dir: str
    processed_dir: str
