"""
MMTDATA Google Trends Models
"""


def trending(df, colname):
    df['avg6mo'] = df[colname].rolling(window=26).mean()
    df['avg12mo'] = df[colname].rolling(window=52).mean()

    stats = {}
    for n in range(4):
        stats['H{}'.format(n + 1)] = df.iloc[25 + n * 26]['avg6mo']
    for n in range(2):
        stats['Y{}'.format(n + 1)] = df.iloc[51 + n * 52]['avg12mo']

    return ((stats['Y2'] / (stats['Y1'] or 1)) > 1.75) and \
           (stats['H4'] > stats['H3']) and \
           (stats['H3'] > stats['H2']) and \
           (stats['H4'] > 35)


def trending_jay(df, colname):
    df['avg6mo'] = df[colname].rolling(window=26).mean()

    stats = {}
    for n in range(4):
        stats['H{}'.format(n + 1)] = df.iloc[25 + n * 26]['avg6mo']

    return (stats['H4'] / (stats['H3'] or 1) > 1.75) and \
           (stats['H4'] > 20)


def trending_paul(df, colname):
    df['avg6mo'] = df[colname].rolling(window=26).mean()

    stats = {}
    for n in range(4):
        stats['H{}'.format(n + 1)] = df.iloc[25 + n * 26]['avg6mo']

    return (stats['H4'] > stats['H3']) and \
           (stats['H3'] > stats['H2']) and \
           (stats['H4'] > 20)
