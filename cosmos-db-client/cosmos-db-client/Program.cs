using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace cosmos_db_client
{
    class Program
    {
        private static readonly string[] EndpointUrls = { "https://tsaoa-centralus.documents.azure.com:443/"};
        private static readonly string[] PrimaryKeys = {  "QOHfnrEpD9JD54z3xAdwQ5Qa3ZeKUylkUPThFbtddwTaZuWGTqIBG8pag5Sy9qpjTAdrqg0lCoTWAi0jKzXJUQ=="};

        async Task createDatabaseAsync(DocumentClient client, string id)
        {
            try
            {
                await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(id));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await client.CreateDatabaseAsync(new Database { Id = id });
                }
                else
                {
                    throw;
                }
            }
        }

        async Task createCollectionAsync(DocumentClient client, string partitionKey, string dbId, string collId)
        {
            try
            {
                await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(dbId, collId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    DocumentCollection collection = new DocumentCollection();
                    collection.Id = collId;
                    collection.PartitionKey.Paths.Add(partitionKey);

                    await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(dbId), collection,
                        new RequestOptions { OfferThroughput = 500 }); //2500 request units per second
                }
                else
                {
                    throw;
                }
            }
        }

        // Object to represent a document that we're writing.
        public class DeviceReading
        {
            [JsonProperty("id")]
            public string Id;

            [JsonProperty("deviceId")]
            public string DeviceId;

            [JsonConverter(typeof(Newtonsoft.Json.Converters.IsoDateTimeConverter))]
            [JsonProperty("writeTime")]
            public DateTime WriteTime;
        }

        // Create a document. Here the partition key is extracted 
        // as "XMS-0001" based on the collection definition
        async Task<Document> createDocumentAsync(string db, string collection, string id, DocumentClient client)
        {
            return await client.CreateDocumentAsync(
                UriFactory.CreateDocumentCollectionUri(db, collection),
                new DeviceReading
                {
                    Id = id,
                    DeviceId = "XMS-0001",
                    WriteTime = DateTime.UtcNow
                });
        }
        
        async Task<DeviceReading> readDocumentAsync(DocumentClient client, string db, string collection, string partition, string id)
        {
            // Read document. Needs the partition key and the Id to be specified
            Document result = await client.ReadDocumentAsync(
              UriFactory.CreateDocumentUri(db, collection, id),
              new RequestOptions { PartitionKey = new PartitionKey(partition) });

            return (DeviceReading)(dynamic)result;
        }

        async Task deleteDocumentasync(DocumentClient client, string db, string collection, string partition, string id)
        {
            // Delete a document. The partition key is required.
            await client.DeleteDocumentAsync(
              UriFactory.CreateDocumentUri(db, collection, id),
              new RequestOptions { PartitionKey = new PartitionKey(partition) });
        }

        static void Main(string[] args)
        {
            Program program = new Program();

            string[] locations = new string[1] { LocationNames.CentralUS };
            for (int i = 0; i < locations.Length; i++)
            {
                ConnectionPolicy usConnectionPolicy = new ConnectionPolicy();
                usConnectionPolicy.PreferredLocations.Add(locations[i]);
                
                DocumentClient client = new DocumentClient(new Uri(EndpointUrls[i]), PrimaryKeys[i], usConnectionPolicy);

                program.createDatabaseAsync(client, "db").Wait();

                //Create collection
                program.createCollectionAsync(client, "/deviceId", "db", "coll").Wait();

                List<long> reads = new List<long>();
                List<long> writes = new List<long>();
                for (int j = 0; j < 1000; j++)
                {
                    
                    string id = "XMS-001-" + DateTime.Now.ToString("yyyyMMddHHmmssfff");

                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    //Write document
                    program.createDocumentAsync("db", "coll", id, client).Wait();
                    watch.Stop();
                    var elapsedMs = watch.ElapsedMilliseconds;
                    writes.Add(elapsedMs);
                    
                    //watch = System.Diagnostics.Stopwatch.StartNew();
                    //Read document
                    /*var task = client.ReadDocumentFeedAsync(UriFactory.CreateDocumentCollectionUri("db", collectionId: "coll"), new FeedOptions { MaxItemCount = 10 });
                    task.Wait();
                    var docs = task.Result;
                    if(docs.Count > 0)
                    {
                        Console.WriteLine("ReadTime: " + DateTime.UtcNow.ToString("hh.mm.ss.ffffff") + ". Read: " + docs.ElementAt(docs.Count - 1).writeTime.ToString("hh.mm.ss.ffffff"));
                    }*/
                    /*
                    var task = program.readDocumentAsync(client, "db", "coll", "XMS-0001", id);
                    task.Wait();
                    Console.WriteLine("ReadTime: " + DateTime.UtcNow.ToString("hh.mm.ss.ffffff") + ". Document read was written at: " + task.Result.WriteTime.ToString("hh.mm.ss.ffffff"));
                    watch.Stop();
                    elapsedMs = watch.ElapsedMilliseconds;
                    
                    reads.Add(elapsedMs);
                    */
                }
                //writes.Sort();
                //reads.Sort();
                //Console.WriteLine(locations[i] + " median write ms: " + writes.Median());
                //Console.WriteLine(locations[i] + " 90th percentile write ms: " + writes[89]);
                //Console.WriteLine(locations[i] + " median read ms: " + reads.Median());
                //Console.WriteLine(locations[i] + " 90th percentile read ms: " + reads[89]);
            }
            //Delete document
            //program.deleteDocumentasync(usClient, "db", "coll", "XMS-0001", "XMS-001-FE24C");
        }
    }
    public static class MyListExtensions
    {
        public static double Median(this List<long> values)
        {
            return values.Median(0, values.Count);
        }

        public static double Median(this List<long> values, int start, int end)
        {
            if (start - end == 0)
            {
                return values[start];
            }
            if (values.Count == 0)
            {
                return 0;
            }
            double median = 0.0;
            int count = end - start;
            if((end-start) % 2 == 0)
            {
                long middleElement1 = values[(count / 2) - 1];
                long middleElement2 = values[(count / 2)];
                median = (middleElement1 + middleElement2) / 2.0;
            } else
            {
                median = values[(count / 2)];
            }
            return median;
        }

        //Taken from http://www.martijnkooij.nl/2013/04/csharp-math-mean-variance-and-standard-deviation/
        public static double Mean(this List<long> values)
        {
            return values.Count == 0 ? 0 : values.Mean(0, values.Count);
        }

        public static double Mean(this List<long> values, int start, int end)
        {
            double s = 0;

            for (int i = start; i < end; i++)
            {
                s += values[i];
            }

            return s / (end - start);
        }

        public static double Variance(this List<long> values)
        {
            return values.Variance(values.Mean(), 0, values.Count);
        }

        public static double Variance(this List<long> values, double mean)
        {
            return values.Variance(mean, 0, values.Count);
        }

        public static double Variance(this List<long> values, double mean, int start, int end)
        {
            double variance = 0;

            for (int i = start; i < end; i++)
            {
                variance += Math.Pow((values[i] - mean), 2);
            }

            int n = end - start;
            if (start > 0) n -= 1;

            return variance / (n-1);
        }

        public static double StandardDeviation(this List<long> values)
        {
            return values.Count == 0 ? 0 : values.StandardDeviation(0, values.Count);
        }

        public static double StandardDeviation(this List<long> values, int start, int end)
        {
            double mean = values.Mean(start, end);
            double variance = values.Variance(mean, start, end);

            return Math.Sqrt(variance);
        }
    }
}
