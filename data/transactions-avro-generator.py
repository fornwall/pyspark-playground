from fastavro import writer, parse_schema

# pip3 install fastavro

schema = {
    'doc': 'Transactions in avro format',
    'name': 'transactions',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'userid', 'type': 'string'},
        {'name': 'amount', 'type': 'double'},
    ],
}
parsed_schema = parse_schema(schema)

records = [
    {'userid': 'user_1', 'amount': 10 },
    {'userid': 'user_1', 'amount': -20 },
    {'userid': 'user_2', 'amount': 30 },
    {'userid': 'user_1', 'amount': 2 },
    {'userid': 'user_1', 'amount': -20 },
    {'userid': 'user_2', 'amount': -40 },
    {'userid': 'user_3', 'amount': -10 },
    {'userid': 'user_4', 'amount': -30 },
    {'userid': 'user_5', 'amount': -40 },
    {'userid': 'user_5', 'amount': -20 },
    {'userid': 'user_1', 'amount': 4 },
    {'userid': 'user_2', 'amount': 10 },
    {'userid': 'user_3', 'amount': 1 },
    {'userid': 'user_4', 'amount': 2 },
    {'userid': 'user_5', 'amount': 3 },
]

with open('transactions.avro', 'wb') as out:
    writer(out, parsed_schema, records)
