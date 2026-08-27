"""Collect high-MMR Dota 2 match data from STRATZ and OpenDota.

The package is organised as four layers, each depending only on the ones above
it, so the dependency graph stays acyclic and any layer can be exercised alone:

``core``
    Configuration, path resolution, and the SQLite manifest that tracks every
    match id the project knows about.
``api``
    HTTP transport (retry, rate limiting, quota accounting) and the STRATZ
    GraphQL / OpenDota REST clients built on it.
``pipeline``
    The collection stages: ``discover`` builds a sampling frame, ``fetch``
    downloads match detail, ``transform`` turns the raw landing zone into
    Parquet, and ``reference`` snapshots the lookup tables.
``cli``
    Argument parsing and command dispatch. Contains no domain logic.

``diagnostics`` sits alongside these as a tool rather than a stage: it probes
the live APIs and checks the collection for schema drift.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
