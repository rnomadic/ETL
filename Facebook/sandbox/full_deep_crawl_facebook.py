# from workbench.settings.base import PROJECT_ROOT

# from workbench.core.connections import get_redshift_cursor

import subprocess
import shlex
# import datetime
import sys
import time

TIMESTMPS = [# '2018-01-12T21:57:21',
             # '2018-01-13T21:52:18',
             # '2018-01-14T21:47:14',
             # '2018-01-15T21:42:11',
             # '2018-01-16T21:37:08',
             # '2018-01-17T21:32:05',
             '2018-01-18T21:27:02',
             '2018-01-19T21:21:59',
             '2018-01-20T21:16:57',
             '2018-01-21T21:11:54',
             '2018-01-22T21:06:51',
             '2018-01-23T21:01:48',
             '2018-01-24T20:56:44',
             '2018-01-25T20:51:42',
             '2018-01-26T20:46:39',
             '2018-01-27T20:41:36',
             '2018-01-28T20:36:32',
             '2018-01-29T20:31:29',
             '2018-01-30T20:26:26',
             '2018-01-31T20:21:23',
             '2018-02-01T20:16:20',
             '2018-02-02T20:11:17',
             '2018-02-03T20:06:14',
             '2018-02-04T20:01:11',
             '2018-02-05T19:56:08',
             '2018-02-06T19:51:05',
             '2018-02-07T19:46:02']


if __name__ == "__main__":
    # curs = get_redshift_cursor()
    # curs.execute('select id from mmt_artist where is_crawl_facebook = %s and last_updated::date = %s',
    #              (1, '2018-01-12'))
    # aids = [a[0] for a in curs.fetchall()]
    #
    # index = 0
    # while True:
    #     print('running {} to {}'.format(index, index+100))
    #     aids_chunk = aids[index:index+100]
    #     if not aids:
    #         print('DONENENONEDLFNSDFLKJ')
    #         break
    #     ts = datetime.datetime.now().isoformat()
    #     cf_name = PROJECT_ROOT + '/workbench/data/artists/fb-deep.{}'.format(ts)
    #     with open(cf_name, 'w') as cf:
    #         cf.write('\n'.join(str(aid) for aid in aids_chunk))
    for ts in TIMESTMPS:
        print('--------------------------')
        print('running ' + ts)
        # callstring = 'airflow trigger_dag warehouse_deep_artist_likes -c ' \
        #              '\'{"artist_list_fn": "%s", "crawl_all": 1}\'' % cf_name

        # callstring = 'airflow clear -dc -s {} -e {} warehouse_deep_artist_likes'.format(ts, ts)
        # print(callstring)
        # proc = subprocess.run(shlex.split(callstring), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # print(proc.stdout.decode('utf-8'))
        # if proc.stderr and 'Error' in proc.stderr.decode('utf-8'):
        #     print(proc.stderr.decode('utf-8'), file=sys.stderr)
        #     _ = input('[[[[[continue?]]]]')
        callstring = 'airflow run -iIf warehouse_deep_artist_likes warehouse_export_deep_artist_likes {}'.format(ts)
        print(callstring)
        proc = subprocess.run(shlex.split(callstring), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(proc.stdout.decode('utf-8'))
        if proc.stderr and 'Error' in proc.stderr.decode('utf-8'):
            print(proc.stderr.decode('utf-8'), file=sys.stderr)
            _ = input('[[[[[continue?]]]]')

        time.sleep(60 * 5)
        # index += 100
