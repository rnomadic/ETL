import datetime
import time
import io
import pandas as pd

from workbench.core.task import BaseTask
from workbench.core.cache import CacheTTL
from workbench.core.utils import ProcessError
from workbench.Google.tables import GoogleTrendsCache
from workbench.Google.models import trending, trending_jay, trending_paul
from workbench.Google.utils import get_artists_locations, TRENDS_SEPERATOR
from workbench.core.boto_utils import export_to_s3, serialize_s3_fn
from workbench.settings.aws import S3_DATA_BUCKET_NAME
from workbench.settings.base import TIME_SERIES_FORMAT

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError


TRENDING_MODELS = {'trending': trending,
                   'trending_jay': trending_jay,
                   'trending_paul': trending_paul}

EXPORT_COLS = ['name', 'location', 'trending', 'trending_jay', 'trending_paul']

S3_TRENDS_KEY = 'google_trends/trends_df/{label}/{name}_{location}_{date}.csv'
S3_RESULTS_KEY = 'google_trends/results_df/{label}/{ts}.csv'


class GoogleTrendsAnalysisTask(BaseTask):
    description = 'Performs Google Trends analyses to determine if artist x location pairs are trending'
    usage = BaseTask.update_usage('GoogleTrendsAnalysisTask '
                                  '[-c/--cache -l/--filter-likes] [-f <filename> -s <s3 label> -g <gprop>]')
    kwargs = BaseTask.update_kwargs({
        'cache': {'flags': ('-c', '--cache'), 'help': 'Recreate the table', 'default': True,
                  'action': 'store_true', 'required': False},
        'filter_likes': {'flags': ('-l', '--filter-likes'), 'help': 'Filter list by Facebook Likes', 'default': True,
                         'action': 'store_true', 'required': False},
        'artist_list_fn': {'flags': ('-f', '--artist-list-fn'), 'help': 'Filename of artists (x locations) to check',
                           'action': 'store', 'default': None, 'required': True},
        's3_label': {'flags': ('-s', '--s3-label'), 'help': 'S3 label',
                     'action': 'store', 'default': None, 'required': False},
        'gprop': {'flags': ('-g', '--gprop'), 'help': 'Lens (if you dont want web search): youtube/images/news/froogle',
                  'action': 'store', 'default': None, 'required': False}
    })

    def __init__(self, **kwargs):
        super(GoogleTrendsAnalysisTask, self).__init__(**kwargs)

        self.date = self.get_kwarg('date', kwargs)
        self.cache = CacheTTL(table=GoogleTrendsCache) if self.get_kwarg('cache', kwargs) else None
        self.label = self.get_kwarg('s3_label', kwargs) or self.date
        self.pytrends = TrendReq(hl='en-US', tz=360)

    def run(self, *args, **kwargs):
        """ Pull Google Trends data and check if trending according to our predefined models.
        Write results to S3 and update cache (if applicable).
        """
        data_to_process = get_artists_locations(fn=self.get_kwarg('artist_list_fn', kwargs),
                                                fl=self.get_kwarg('filter_likes', kwargs),
                                                cache=self.cache)
        results = self.run_trends(data_to_process)

        if results:
            try:
                s3key = serialize_s3_fn(S3_RESULTS_KEY.format(label=self.label, ts=int(time.time())))
                self.df_to_s3(key=s3key, df=pd.DataFrame(results, columns=[col for col in EXPORT_COLS]))
                self.xcom_push(key='results_s3_key', val=s3key)
            except ProcessError as e:
                raise ProcessError('Error writing results data to s3: {}'.format(e))

        if self.cache:
            for r in results:
                self.cache.set(key={'name': r['name'], 'location': r['location']})

        self.xcom_push(key='checked', val='Successfully checked {} Google Trends.'.format(len(results)))

    def run_trends(self, data_to_process):
        """ Check trending and xcom_push any artist x location pairs that went unchecked.
        Args:
            data_to_process: <list> of artist name + location dicts.

        Returns:
            <list> of artist name + location + trending dicts.
        """
        self.log('Checking Google Trends for {} Artist x Location pairs'.format(len(data_to_process)))
        results = []

        for r in self.check_trending(data_to_process):
            results.append(r)

        if len(data_to_process) != len(results):
            self.err('Unable to retrieve trends for the following: \n {}'.format(
                '\n'.join(TRENDS_SEPERATOR.join([dp['name'], dp['location']]) for dp in data_to_process if
                          (dp['name'], dp['location']) not in {(res['name'], res['location']) for res in results})))
            self.xcom_push(key='unchecked', val='Unable to retrieve complete trends data. Please check log.')

        self.xcom_push(key='empty', val='Empty trends results for the following: \n {}'.format(
            '\n'.join('{} @ {}'.format(res['name'], res['location']) for res in results if
                      not all(model in res.keys() for model in TRENDING_MODELS.keys()))))

        return results

    @staticmethod
    def df_to_s3(df, key):
        """ Writes a DataFrame to S3 as a csv file.
        Args:
            df: <DataFrame>
            key: s3 key
        """
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer)
        export_to_s3(bucket_name=S3_DATA_BUCKET_NAME,
                     key=key,
                     data=csv_buffer.getvalue().encode('utf8'))

    def check_trending(self, data_to_process, **kwargs):
        """ Retrieve Google Trends data, determine whether trending, and return results.
        Args:
            data_to_process: <list> of artist name + location dicts.
            **kwargs: 'gprop': Used to specify trends type. ie YouTube, images, or <None> for web search

        Returns:
            <Iterable> of <dict> items of artist, location, and trending results data.
        """
        for i, dp in enumerate(data_to_process, 1):
            if i % 50 == 0:
                self.log('Processing item {} of {}.'.format(i, len(data_to_process)))

            name = dp['name']
            loc = dp['location']

            try:
                self.pytrends.build_payload(
                    [name],
                    cat=35,
                    timeframe='{} {}'.format(
                        (datetime.datetime.now() - datetime.timedelta(days=730)).strftime(TIME_SERIES_FORMAT),
                        self.date),
                    geo=loc,
                    gprop='{}'.format(self.get_kwarg('gprop', kwargs)))
                df = self.pytrends.interest_over_time()
            except ResponseError as e:
                self.err('Error retrieving trends data: {}'.format(e))
                return

            if not df.empty:
                df.drop('isPartial', axis=1, inplace=True)
                for key, val in TRENDING_MODELS.items():
                    try:
                        dp[key] = val(df, name)
                    except IndexError as e:
                        self.err('Error processing trends data: {}'.format(e))
                        return

                try:
                    self.df_to_s3(df=df, key=serialize_s3_fn(
                        S3_TRENDS_KEY.format(label=self.label, name=name, location=loc, date=self.date)))
                except ProcessError as e:
                    self.err("Error writing trends data to s3: {}".format(e))
                    return

            yield dp
