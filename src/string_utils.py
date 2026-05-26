import re

TO_SNAKE_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

def camel_to_snake(string: str) -> str:
    return TO_SNAKE_RE.sub("_", string).lower()