import pydocumentdb;
import pydocumentdb.document_client as document_client
import time;
import numpy as np;

config = {
        'ENDPOINT': 'https://tsaoa-centralus.documents.azure.com:443/',
        'MASTERKEY': 'QOHfnrEpD9JD54z3xAdwQ5Qa3ZeKUylkUPThFbtddwTaZuWGTqIBG8pag5Sy9qpjTAdrqg0lCoTWAi0jKzXJUQ==',
        'DOCUMENTDB_DATABASE': 'db',
        'DOCUMENTDB_COLLECTION': 'coll'
}

# Initialize the Python DocumentDB client
client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})
dblink = 'dbs/' + config['DOCUMENTDB_DATABASE'];
"""
query = { 'query': 'SELECT * FROM server s' }
options = {}
options['enableCrossPartitionQuery'] = True
coll_iterable = client.QueryCollections(dblink, query, options)
results = list(coll_iterable)
print(results)
"""

# Query them in SQL
query = { 'query': 'SELECT TOP 1 * FROM server s ORDER BY s.writeTime DESC' }

options = {}
options['enableCrossPartitionQuery'] = True
collectionlink = dblink + '/colls/{0}'.format(config['DOCUMENTDB_COLLECTION'])
prev = 0;
cont = True;
differences = [];
while cont:
    result_iterable = client.QueryDocuments(collectionlink, query, options)
    results = list(result_iterable);
    for r in results:
        print("Read time: {}, write time: {}".format(time.time(), r['writeTime']));
        differences.append(time.time() - r['writeTime'])
        if prev == r['writeTime']:
            cont = False;
            prev = r['writeTime'];

differences.sort();
arr = np.array(differences);
print("Median difference: {}, 90th percentile difference: {}".format(np.percentile(arr, 50), np.percentile(arr, 90)))
