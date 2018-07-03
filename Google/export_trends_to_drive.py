from workbench.settings.base import S3_DATA_BUCKET_NAME

from workbench.core.utils import ProcessError
from workbench.core.google import BaseExportToDriveTask
from workbench.core.boto_utils import get_s3_files

import time


class ExportTrendsCSVTask(BaseExportToDriveTask):
    name = 'ExportTrendsCSVTask'
    description = 'Exports results of Trends analysis (trending artist and locations) to Drive'

    from_disk = True
    data_folder_id = '1Ulhg6eT7R9QOP3DcrwnZGA4avUQUyiHZ'
    data_folder_name = 'Trending Artist Suggestions'
    filetype = 'csv'
    permissions = [
        {
            'type': 'user',
            'role': 'writer',
            'value': 'jason@mymusictaste.com'
        },
        {
            'type': 'user',
            'role': 'writer',
            'value': 'evan@mymusictaste.com'
        },
    ]

    results_s3_prefix = 'google_trends/results_df/'

    def generate_filename(self, **kwargs):
        date = self.get_kwarg('date', kwargs)
        return 'Trending Artists - {}'.format(date)

    def generate_file_contents(self, filename, **kwargs):
        s3_key = self.xcom_pull(key='results_s3_key')

        s3_files = get_s3_files(S3_DATA_BUCKET_NAME, s3_prefix=self.results_s3_prefix,
                                key_ext='.csv', filters=[lambda s3_file: s3_file.key == s3_key])

        if not s3_files:
            raise ProcessError('could not find any matching files in {} for {}'.format(S3_DATA_BUCKET_NAME, s3_key))

        contents = None
        for f in s3_files:
            if not contents:  # PE first file (if multiple) contains headers
                contents = f.get()['Body'].read().decode().rstrip()
            else:  # PE strip headers from remaining files before concat
                stripped_content = f.get()['Body'].read().decode().rstrip()
                contents = '\n'.join([contents, '\n'.join(stripped_content.split('\n')[1:])])

        tmp_fn = '/tmp/{}.csv'.format(time.time())
        with open(tmp_fn, 'w') as tmpfile:
            tmpfile.write(contents)

        return tmp_fn
