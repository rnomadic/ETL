from workbench.settings.base import FACEBOOK_ACCESS_TOKEN

from workbench.core.cache import Cache
from workbench.core.connections import get_redshift_cursor

from facebook import GraphAPI, GraphAPIError

import time

graph = GraphAPI(access_token=FACEBOOK_ACCESS_TOKEN, )
curs = get_redshift_cursor()

cache_key = 'facebook_artist'
cache = Cache()

MADE_ARTISTS_QUERY = """SELECT DISTINCT a.name FROM music_artist AS a
                        INNER JOIN m2t_promotionuserrequest AS m ON a.id = m.artist_id;"""

curs.execute(MADE_ARTISTS_QUERY)

artists = curs.fetchall()

found = set()
missed = set()

for a in artists:
    a = a[0]
    if cache.get(':::'.join([cache_key, a])):
        continue

    try_count = 5
    try:
        res = graph.search(type='page', q=' '.join([a, 'official']))
    except GraphAPIError as e:
        print('1', e)
        res = None

    if res:
        data = res['data']
        if len(data) == 0:
            try:
                res = graph.search(type='page', q=a)
                data = res['data']
            except GraphAPIError as e:
                print('2', e)
                continue
        has_insights = False
        while try_count > 0 and len(data) > 0:
            page = data.pop(0)
            try:
                res2 = graph.get_connections(page['id'], 'insights', metric='page_fans_country')
            except GraphAPIError as e:
                print('3', e)
                res2 = None

            if res2 and len(res2['data']) > 0:
                has_insights = True
                break

            try_count -= 1
        if has_insights:
            found.add(a)
            cache.set(':::'.join([cache_key, a]), 1)
        else:
            missed.add(a)

    time.sleep(5)

print('found {} of {}'.format(len(found), len(artists)))
