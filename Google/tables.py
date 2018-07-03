from workbench.core.tables import Table


class GoogleTrendsCache(Table):
    table_name = 'google_trends_cache'
    ttl = True
    key_schema = [
        {
            'AttributeName': 'name',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'location',
            'KeyType': 'RANGE'
        }
    ]
    attribute_definitions = [
        {
            'AttributeName': 'name',
            'AttributeType': 'S',
        },
        {
            'AttributeName': 'location',
            'AttributeType': 'S'
        }
    ]

