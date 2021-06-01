import datetime
import json

import pydgraph


# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


client_stub = create_client_stub()
client = create_client(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all():
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema():
    schema = """
    id: string @index(exact) .
    commentNum: int .
    type: string .
    thumbNum: int .
    title: string .
    content: string .
    time: string .
    author: string .
    url: string .
    type News {
        id
        commentNum
        type
        thumbNum
        title
        content
        time
        author
        url
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


# Create data using JSON.
def create_data(news):
    # Create a new transaction.
    txn = client.txn()
    try:
        # Create data.
        p = {
            'uid': '_:' + news['id'],
            'dgraph.type': 'News',
            'id': news['id'],
            'commentNum': news['commentNum'],
            'type': news['type'],
            'thumbNum': news['thumbNum'],
            'title': news['title'],
            'content': news['content'],
            'time': news['time'],
            'author': news['author'],
            'url': news['url']
        }

        # Run mutation.
        response = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object (person named "Alice").
        # response.uids returns a map from blank node names to uids.

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


# Deleting a data
def delete_data():
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query all($a: string) {
            all(func: eq(name, $a)) {
               uid
            }
        }"""
        variables1 = {'$a': 'Bob'}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("Bob's UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print('Bob deleted')
        txn.commit()

    finally:
        txn.discard()


# Query for data.
def query_alice():
    # Run query.
    query = """query all($a: string) {
        all(func: eq(name, $a)) {
            uid
            name
            age
            married
            loc
            dob
            friend {
                name
                age
            }
            school {
                name
            }
        }
    }"""

    variables = {'$a': 'Alice'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print('Number of people named "Alice": {}'.format(len(ppl['all'])))


# Query to check for deleted node
def query_bob():
    query = """query all($b: string) {
            all(func: eq(name, $b)) {
                uid
                name
                age
                friend {
                    uid
                    name
                    age
                }
                ~friend {
                    uid
                    name
                    age
                }
            }
        }"""

    variables = {'$b': 'Bob'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print('Number of people named "Bob": {}'.format(len(ppl['all'])))


# if __name__ == '__main__':
#     try:
#         main()
#         print('DONE!')
#     except Exception as e:
#         print('Error: {}'.format(e))
