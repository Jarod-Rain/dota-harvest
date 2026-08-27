"""The collection pipeline, in the order the stages run.

1. :mod:`~dota_harvest.pipeline.discover` walks OpenDota to build a sampling
   frame of match ids, recording them in the manifest.
2. :mod:`~dota_harvest.pipeline.fetch` pulls match detail from STRATZ into the
   raw landing zone as gzipped JSONL.
3. :mod:`~dota_harvest.pipeline.transform` converts that landing zone into
   patch-partitioned Parquet. Network-free and re-runnable.

:mod:`~dota_harvest.pipeline.reference` snapshots the lookup tables the match
data decodes against, and :mod:`~dota_harvest.pipeline.pro` is an alternative
front end to stage 1 for tournament and team collection.
"""

from dota_harvest.pipeline import discover, fetch, pro, reference, transform

__all__ = ["discover", "fetch", "pro", "reference", "transform"]
