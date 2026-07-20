# lanl-simulator

External producer. Replays the LANL source subset (`data/subset/*.txt.gz`) as
**daily raw batches**, writing one object per source per day to the MinIO
`landing` bucket, and records the landing checksum.