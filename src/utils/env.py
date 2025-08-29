from dotenv import load_dotenv
import os


def get_envs(dev: bool = False, keys: list[str] | None = None):
    if dev:
        load_dotenv()
    if keys is None:
        return tuple()
    return tuple(os.getenv(key) for key in keys)
