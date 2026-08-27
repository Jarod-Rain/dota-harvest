"""Command-line front end.

This layer parses arguments and dispatches to the pipeline; it holds no domain
logic of its own, so every command remains callable as a plain function.
"""

from dota_harvest.cli.main import main

__all__ = ["main"]
