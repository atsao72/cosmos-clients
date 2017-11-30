import pydocumentdb;
import pydocumentdb.document_client as document_client
import time;

config = {
        'ENDPOINT': 'https://tsaoa-westeu.documents.azure.com:443/',
        'MASTERKEY': 'QOHfnrEpD9JD54z3xAdwQ5Qa3ZeKUylkUPThFbtddwTaZuWGTqIBG8pag5Sy9qpjTAdrqg0lCoTWAi0jKzXJUQ==',
        'DOCUMENTDB_DATABASE': 'db',
        'DOCUMENTDB_COLLECTION': 'coll'
};

# Initialize the Python DocumentDB client
client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})

# Create a database
#db = client.CreateDatabase({ 'id': config['DOCUMENTDB_DATABASE'] })
dblink = "dbs/" + config["DOCUMENTDB_DATABASE"];
# Create collection options
options = {
        'offerEnableRUPerMinuteThroughput': True,
        'offerVersion': "V2",
        'offerThroughput': 400
}

# Create a collection
collection = client.CreateCollection(dblink, { 'id': config['DOCUMENTDB_COLLECTION'] }, options)

for i in range(500):
    # Create some documents
    document1 = client.CreateDocument(collection['_self'],
            {
                'id': 'doc{}'.format(i),
                'writeTime': time.time(),
            })

# Query them in SQL
query = { 'query': 'SELECT TOP 1 * FROM server s ORDER BY s.writeTime DESC' }

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 2
result_iterable = client.QueryDocuments(collection['_self'], query, options)
results = list(result_iterable);
for r in results:
    print("Read time: {}, write time: {}".format(time.time(), r['writeTime']));
