import yaml

def load_config(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def validate_config(config):
    if "sources" not in config:
        raise ValueError("Missing sources config")