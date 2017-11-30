# cosmos-clients
C# and Python clients for interacting with CosmosDB

## cosmos-db-client

This contains a CSProject for interacting with cosmos-db in C#. The API used is the DocumentDB. A good quickstart example is [here](onsole.cloud.google.com/compute/instances?project=patient-net). 

The program currently executes 1000 reads or writes and then calculates the median and 90th percentile latency times.

## Python clients

The python clients use the [DocumentDB API](https://docs.microsoft.com/en-us/azure/cosmos-db/documentdb-sdk-python)

The Program.py program executes 500 writes and then reads the latest one. The data that is written is the timestamp at the time of the write operation. The point is to compare the read time with the write time.

The ReadProgram.py program continually reads the latest document in the table until the same document is read twice.
