### <ins>Instructions to Run<ins>

1. navigate to correct working directory
```
cd /<your-path-to-cloned-project-here>
```

2. build docker image:
```
docker build -t ingest_market_data . 
```

3. run ingestion:
```
docker run -v <your-path-to-cloned-project-here>/outputs:/app/outputs ingest_market_data
```

output file saves to current working directory in the folder called outputs in a file named outputs.py

### <ins>Assumptions<ins>
the assumption from reviewing the data was that 
- refdata joins to executions based on ISIN and currency and joining keys
- and marketdata joined to refdata based on marketdata.listing_id = refdata.id

- but I noticed there were a large number of null values generated in the bbo, and slippage calculations, this was due to the marketdata dataset not containing many of the listing_id's in the other data set.

### <ins>Performance Metrics<ins>
- run time averages around 1 second per run
- docker build time ~ 15.7s
- docker image size ~1.3GB
- memory usage peaks at around 700mb

