from workbench.core.connections import get_redshift_cursor
from workbench.core.utils import err, ProcessError

TRENDS_SEPERATOR = '    '

FILTER_STMT = """
              SELECT
                DISTINCT dates.artist_name as name, dates.location
              FROM
                (
                    (SELECT artist_id, artist_name,  max(date) as date, location
                     FROM mmt_artist_facebook
                     WHERE location != 'WW'
                      GROUP BY artist_id, artist_name, location) AS dates
                    INNER JOIN
                    (SELECT DISTINCT artist_id, date, location
                     FROM mmt_artist_facebook
                     WHERE likes >= %s
                    ) AS likes
                      ON dates.artist_id = likes.artist_id
                         AND dates.date = likes.date
                         AND dates.location = likes.location
                         AND dates.location IN %s
                )
              WHERE LOWER(dates.artist_name) IN %s
              """

# NATIONS_STMT = """SELECT nation_iso FROM v_whitelist_bizdev_locations"""
NATIONS_STMT = """SELECT nation_iso FROM v_focus_bizdev_locations"""

INVALID_CHARS = ['&', '$', '@', '=', ';', ':', '+', ',', '?',
                 '\\', '{', '}', '^', '`', '[', ']', '<', '>', '#', '%', '"', '\'', '~', '|']


def filter_by_facebook_likes(artists_and_locations, min_likes_threshold=5000, curs=None):
    """
    Args:
        artists_and_locations: [{'name': <artist name>, 'location': <nation iso>, ...}, ...]
        min_likes_threshold: minimum facebook likes to filter by
        curs: a Redshift cursor to use

    Returns: filtered by limit, [{'name': <artist name>, 'location': <nation iso>, ...}, ...]
    """
    curs = curs or get_redshift_cursor()
    try:
        artist_names = set(map(lambda a_l: a_l['name'].lower(), artists_and_locations))
        locations = set(map(lambda a_l: a_l['location'], artists_and_locations))
    except KeyError as e:
        err(e)
        raise ProcessError('artists_and_locations must be: '
                           '[{\'name\': <artist name>, \'location\': <nation iso>, ...}, ...]')

    try:
        curs.execute(FILTER_STMT, (min_likes_threshold, tuple(locations), tuple(artist_names)))
    except Exception as e:
        err(e)
        raise ProcessError('filter query failed')

    filter_by = set('_'.join([str(row[0].lower()), str(row[1])]) for row in curs.fetchall())
    return list(filter(lambda a_l:
                       '_'.join([a_l['name'].lower(), a_l['location']]) in filter_by,
                       artists_and_locations))


def process_trends_model_input(fn, curs=None):
    """
    Processes flat file input into list of artist X locations

    Input is line-separated artist names, or 4-space delimited/line-separated artist names + nation isos
    Args:
        fn: full path to input file
        curs: a Redshift cursor to use (if needed)

    Returns: [{'name': <artist name>, 'location': <nation iso>, ...}, ...]

    """
    try:
        with open(fn, 'r') as f:
            raw = [l.rstrip().replace('!', '') for l in f.readlines()]
        assert len(raw) > 0 and all(len(raw_inp) > 0 for raw_inp in raw)
    except FileNotFoundError:
        raise ProcessError('no such file {} (hint: must be absolute path)'.format(fn))
    except AssertionError:
        raise ProcessError('file {} was empty or contained empty lines'.format(fn))

    if len(raw[0].split(TRENDS_SEPERATOR)) > 1:
        return [dict(zip(('name', 'location'), l.split(TRENDS_SEPERATOR))) for l in raw]
    else:
        curs = curs or get_redshift_cursor()
        try:
            curs.execute(NATIONS_STMT)
        except Exception as e:
            err(e)
            raise ProcessError('failed to fetch whitelist biz dev nations')
        locs = [loc for loc in curs.fetchall()]
        return [dict(name=raw_artist, location=loc[0]) for raw_artist in raw for loc in locs]


def filter_by_blacklist(artists_and_locations, cache):
    """
    Args:
        artists_and_locations: [{'name': <artist name>, 'location': <nation iso>, ...}, ...]
        cache: CacheTTL to use

    Returns: filtered by cache, [{'name': <artist name>, 'location': <nation iso>, ...}, ...]
    """
    return [x for x in artists_and_locations if not cache.get(x)]
    # return artists_and_locations


def get_artists_locations(fn, fl, cache):
    """
    Args:
        fn: absolute path to input file
        fl: True/False filter by facebook likes
        cache: CacheTTL to use

    Returns: [{'name': <artist name>, 'location': <nation iso>, ...}, ...] filtered by all criteria
    """
    artists_locations = process_trends_model_input(fn)
    print('found {} AxL pairs in file {}'.format(len(artists_locations), fn))
    if fl:
        artists_locations = filter_by_facebook_likes(artists_locations)
        print('{} after fb filter'.format(len(artists_locations)))
    if cache:
        artists_locations = filter_by_blacklist(artists_locations, cache)
        print('{} after cache filter'.format(len(artists_locations)))
    return artists_locations
