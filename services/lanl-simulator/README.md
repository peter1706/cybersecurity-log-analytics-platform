# lanl-simulator

External producer. Replays the LANL source subset (`data/subset/*.txt.gz`) as
**daily raw batches**, writing one object per source per day to the MinIO
`landing` bucket, and records the landing checksum.

Replays a single day (`--day N`) or, with `--day-end M`, an inclusive range
`[N, M]` in one pass — one landing object per day — to seed the history a
Silver → Gold rolling-window backfill needs.