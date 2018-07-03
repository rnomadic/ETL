from workbench.settings.base import TIME_SERIES_FORMAT, FACEBOOK_APP_ID, \
    FACEBOOK_APP_SECRET, FACEBOOK_MARKETING_ACCOUNT_ID, FACEBOOK_ACCESS_TOKEN, WORLDWIDE_ISO

from workbench.core.utils import err

from facebookads.api import FacebookAdsApi
from facebookads.adobjects.ad import Ad
from facebookads.adobjects.adcreative import AdCreative
from facebookads.adobjects.adcreativeobjectstoryspec import AdCreativeObjectStorySpec
from facebookads.adobjects.adcreativelinkdata import AdCreativeLinkData
from facebookads.adobjects.adcreativevideodata import AdCreativeVideoData
from facebookads.adobjects.adcreativelinkdatacalltoaction import AdCreativeLinkDataCallToAction
from facebookads.adobjects.adcreativelinkdatacalltoactionvalue import AdCreativeLinkDataCallToActionValue
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.exceptions import FacebookRequestError

from facebook import GraphAPI, GraphAPIError
import json
import requests
import pytz
import datetime
import time
import logging

LOCAL_TIME_ZONE = pytz.timezone('Asia/Seoul')

ACTION_KEY_CONVERTER = {  # hash table lookup for conversion stats
    'offsite_conversion.fb_pixel_add_to_wishlist': ('add_to_wishlist', 'cost_per_add_to_wishlist'),
    'offsite_conversion.fb_pixel_complete_registration': (
        'complete_registrations', 'cost_per_complete_registration'),
    'offsite_conversion.fb_pixel_lead': ('leads', 'cost_per_lead'),
    'link_click': ('link_clicks', 'cost_per_link_click'),
    'post_engagement': ('post_engagement', 'cost_per_post_engagement'),
    'post_reaction': ('post_reactions', 'cost_per_post_reaction'),
    'like': ('likes', 'cost_per_like'),
    'comment': ('comments', 'cost_per_comment')
}


class AccessTokenError(Exception):
    pass


# convert yyyy-mm-ddT00:00:00 to yyyy-mm-dd
def tsify_datetime(dt):
    return dt.split('T')[0]


# convert ts to python datetime object
def get_datetime(ts):
    try:
        return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        return LOCAL_TIME_ZONE.localize(datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S'))


# divide helper with default
def div(numerator, denominator, default=None):
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return default if default is not None else numerator


def get_facebook_token():
    host = "https://graph.facebook.com/oauth/"
    redirect_uri = "https://www.mymusictaste.com/"
    payload = {
        "client_id": FACEBOOK_APP_ID,
        "client_secret": FACEBOOK_APP_SECRET,
        "redirect_uri": redirect_uri,
        "access_token": FACEBOOK_ACCESS_TOKEN}

    res = json.loads(requests.get(host + "client_code", params=payload).content.decode('utf8'))

    code = res.get("code", None)
    machine_id = res.get("machine_id", None)

    payload.pop('client_secret')
    payload.pop('access_token')

    payload['code'] = code

    if machine_id is not None:
        payload['machine_id'] = machine_id

    res = json.loads(requests.get(host + 'access_token', params=payload).content.decode('utf8'))

    new_token = res.get("access_token", None)
    expires_in = res.get('expires_in', 0)

    if new_token != FACEBOOK_ACCESS_TOKEN and expires_in < 60 * 60 * 48:  # PE for now, warn 2 days before expire
        raise AccessTokenError(new_token)

    return new_token


def get_ad_acct(token):
    FacebookAdsApi.init(FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, token, {})
    return AdAccount(FACEBOOK_MARKETING_ACCOUNT_ID)


# retrieve Ad objects associated with MMT AdAccount where status = active|paused
def get_ads(mmt_ads_acct):
    ad_params = {'fields': [Ad.Field.adset_id, Ad.Field.id, 'creative{object_story_spec}'],
                 'limit': 500,
                 'filtering': [{'field': 'ad.effective_status', 'operator': AdsInsights.Operator.in_,
                                'value': [Ad.EffectiveStatus.active,
                                          # Ad.EffectiveStatus.adset_paused, Ad.EffectiveStatus.campaign_paused,
                                          Ad.EffectiveStatus.paused]}]}
    try:
        ads_generator = mmt_ads_acct.get_ads(params=ad_params)
        return {ad[AdsInsights.Field.adset_id]: ad for ad in ads_generator}
    except FacebookRequestError as e:
        if e.api_error_code() == 17:
            err('A: Hit api limit..backing off')
            time.sleep(60)
            return get_ads(mmt_ads_acct)
        else:
            raise e


# retrieve AdSet objects and associated stats for adset_ids with start_date < ts < end_date
def get_adsets(mmt_ads_acct, adset_ids, ts):
    adset_params = {
        'fields': [AdSet.Field.name, AdSet.Field.created_time, AdSet.Field.start_time, AdSet.Field.end_time,
                   AdSet.Field.effective_status, AdSet.Field.budget_remaining, AdSet.Field.daily_budget,
                   AdSet.Field.lifetime_budget, AdSet.Field.id],
        'filtering': [{'field': AdSet.Field.id, 'operator': 'IN', 'value': list(adset_ids)}], 'limit': 500}

    try:
        adsets_generator = mmt_ads_acct.get_ad_sets(params=adset_params)
        adsets = {adset[AdSet.Field.id]: adset for adset in adsets_generator}
    except FacebookRequestError as e:
        if e.api_error_code() == 17:
            err('AS: Hit api limit..backing off')
            time.sleep(60)
            return get_adsets(mmt_ads_acct, adset_ids, ts)
        else:
            raise e

    remove_adsets = []
    for adset_id in adsets:
        start_date = adsets[adset_id][AdSet.Field.start_time]
        end_date = adsets[adset_id][AdSet.Field.end_time]
        if get_datetime(ts) > get_datetime(end_date):
            print('{} already ended on {}'.format(adset_id, end_date))
            remove_adsets.append(adset_id)
        elif get_datetime(ts) < get_datetime(start_date):
            print('{} has not started yet, starts on {}'.format(adset_id, start_date))
            remove_adsets.append(adset_id)
    for adset_id in remove_adsets:
        adsets.pop(adset_id)

    return adsets


# retrieve AdInsights objects with associated stats for adset_ids and bounded by ts (1 day)
def get_adset_insights(mmt_ads_acct, adset_ids, ts):

    insight_params = {'level': AdsInsights.Level.adset,
                      'fields': [AdsInsights.Field.adset_id, AdsInsights.Field.reach, AdsInsights.Field.cpp,
                                 AdsInsights.Field.frequency, AdsInsights.Field.spend, AdsInsights.Field.actions,
                                 AdsInsights.Field.cost_per_action_type],
                      'filtering': [
                          {'field': 'adset.id', 'operator': AdsInsights.Operator.in_, 'value': list(adset_ids)}],
                      'time_ranges': [{'since': tsify_datetime(ts), 'until': tsify_datetime(ts)}],
                      'limit': 500}

    try:
        insight_generator = mmt_ads_acct.get_insights(params=insight_params)
        return {insight[AdsInsights.Field.adset_id]: insight for insight in insight_generator}
    except FacebookRequestError as e:
        if e.api_error_code() == 17:
            err('ASI: Hit api limit..backing off')
            time.sleep(60)
            return get_adset_insights(mmt_ads_acct, adset_ids, ts)
        else:
            raise e


# retrieve AdInsights object with associated period stats for adset bounded by start_date -> end_date
def get_adset_period_insight(adset, start_date, end_date):
    period_insight_parameters = {'fields': [AdsInsights.Field.reach, AdsInsights.Field.frequency,
                                            AdsInsights.Field.actions,
                                            AdsInsights.Field.date_start, AdsInsights.Field.date_stop],
                                 'time_ranges': [{'since': tsify_datetime(start_date),
                                                  'until': tsify_datetime(end_date)}]}
    try:
        return adset.get_insights(params=period_insight_parameters)
    except FacebookRequestError as e:
        if e.api_error_code() == 17:
            err('ASPI: Hit api limit..backing off')
            time.sleep(60)
            return get_adset_period_insight(adset, start_date, end_date)
        else:
            raise e


# extract link from ad, if exists
def get_adlink(ad):
    try:
        object_story_spec = ad[Ad.Field.creative][AdCreative.Field.object_story_spec]
    except KeyError:
        return ''

    try:
        return object_story_spec[AdCreativeObjectStorySpec.Field.link_data][AdCreativeLinkData.Field.link]
    except KeyError:
        pass

    try:
        video = object_story_spec[AdCreativeObjectStorySpec.Field.video_data]
        return video[AdCreativeVideoData.Field.call_to_action][AdCreativeLinkDataCallToAction.Field.value][
            AdCreativeLinkDataCallToActionValue.Field.link]
    except KeyError:
        return ''


# retrieve AdSet[delivery_estimate] for potential_audience_size
def get_reach_estimate(adset):
    try:
        adset.remote_read(fields=['delivery_estimate'])
    except FacebookRequestError as e:
        if e.api_error_code() == 17:
            err('RE: reached api limit')
            time.sleep(60)
            return get_reach_estimate(adset)
        else:
            raise e

    reach_estimate = -1
    try:
        data = adset['delivery_estimate']['data'] if 'data' in adset['delivery_estimate'] else []
        reach_estimate = data[0]['estimate_mau']
    except (KeyError, IndexError):
        pass

    return reach_estimate


# annotate record with stats from insight
def append_insights(record, insight):
    record['frequency'] = float(insight[AdsInsights.Field.frequency])
    record['reach'] = int(insight[AdsInsights.Field.reach])
    record['cost_per_1000_people_reach'] = float(insight[AdsInsights.Field.cpp])
    record['amount_spent'] = float(insight[AdsInsights.Field.spend])

    try:
        actions = insight[AdsInsights.Field.actions]
    except KeyError:
        actions = []
    try:
        action_costs = insight[AdsInsights.Field.cost_per_action_type]
    except KeyError:
        action_costs = []

    record = append_action_insights(record, actions)
    record = append_action_cost_insights(record, action_costs)

    return record


# annotate record with stats from actions
def append_action_insights(record, actions):
    # ['comment', 'like', 'link_click', 'offsite_conversion.fb_pixel_add_to_wishlist',
    #  'offsite_conversion.fb_pixel_complete_registration', 'offsite_conversion.fb_pixel_lead',
    #  'post', 'post_reaction', 'unlike', 'page_engagement', 'post_engagement', 'offsite_conversion']
    for a in actions:
        try:
            record[ACTION_KEY_CONVERTER[a['action_type']][0]] = int(a['value'])
        except KeyError:
            pass

    return record


# annotate record with stats from action_costs
def append_action_cost_insights(record, action_costs):
    for a in action_costs:
        try:
            record[ACTION_KEY_CONVERTER[a['action_type']][1]] = float(a['value'])
        except KeyError:
            pass

    return record


# annotate record with stats from period_insight
def append_period_insights(record, period_insight):
    try:
        pi = period_insight[0]
    except IndexError:
        return record

    record['period_start'] = pi[AdsInsights.Field.date_start]
    record['period_end'] = pi[AdsInsights.Field.date_stop]
    record['period_frequency'] = float(pi[AdsInsights.Field.frequency])
    if AdsInsights.Field.actions in pi:
        record['period_lead'] = int(next((item for item in pi[AdsInsights.Field.actions]
                                          if item['action_type'] == 'offsite_conversion.fb_pixel_lead'),
                                         {'value': 0})['value'])
    else:
        record['period_lead'] = -1

    period_reach = int(pi[AdsInsights.Field.reach])
    record['period_reach'] = period_reach
    record['potential_audience_size_progress'] = period_reach / float(record['potential_audience_size'])

    return record


# annotate record with calculated stats using ts (1 day)
def append_calculated_insights(record, ts):
    days_left = get_datetime(record['ends']) - get_datetime(ts)

    record['days_left'] = 0 if days_left.days < 0 else days_left.days

    days_occurred = float(record['duration'] - record['days_left'])
    record['makes_per_day'] = div(record['makes'], days_occurred)
    record['leads_per_day'] = div(record['leads'], days_occurred)
    record['makes_per_lead'] = div(record['makes'], record['leads'], default=0)
    record['link_clicks_per_reach'] = div(record['link_clicks'], float(record['reach']), default=0)
    record['link_clicks_per_reaction'] = div(record['link_clicks'], float(record['post_reactions']), default=0)
    record['leads_per_signup'] = div(record['leads'], float(record['complete_registrations']), default=0)
    record['leads_per_link_clicks'] = div(record['leads'], float(record['link_clicks']), default=0)

    if record['daily_budget'] == 0.0:
        record['daily_budget'] = div(record['amount_left'], record['days_left'])
    record['daily_budget_spent'] = div(record['amount_spent'], days_occurred)

    return record


# annotate record with ad_link
def append_ad_link(record, ad_link):
    record['link'] = ad_link
    if 'mymusictaste.com' not in ad_link:
        return record

    try:
        components = ad_link.split('/')[-2].split(',')
    except IndexError:
        return record

    record['promotion_name'] = components[0]
    if len(components) < 3:
        record['promotion_id'] = components[-1]
    else:
        record['artist_id'] = components[-2]
        record['city_id'] = components[-1]

    return record


def get_artist_ww_likes(graph, facebook_page_id):
    res = None
    while True:
        try:
            res = graph.get_object(str(facebook_page_id), fields='fan_count')
            assert 'fan_count' in res
            break
        except AssertionError:
            logging.log(msg='no ww count for {}'.format(facebook_page_id), level=logging.WARNING)
            break
        except GraphAPIError as e:
            if e.code == 17:
                logging.log(msg='backing off', level=logging.INFO)
                time.sleep(300)
                continue
            else:
                raise e

    return {'location': WORLDWIDE_ISO, 'likes': res['fan_count']} if res else None


# get worldwide and by country likes for a fb page, with optional deep crawl
def get_artist_likes_by_country(facebook_page_id, ts, is_crawl_all=False, window_size=7):
    graph = GraphAPI(access_token=FACEBOOK_ACCESS_TOKEN, )
    artist_data = {}

    ww_likes = get_artist_ww_likes(graph, facebook_page_id)
    if ww_likes:
        artist_data[ts.strftime(TIME_SERIES_FORMAT)] = [ww_likes]

    time.sleep(1)

    kwargs = {'metric': 'page_fans_country',
              'until': int(ts.timestamp()),
              'since': int((ts - datetime.timedelta(days=window_size)).timestamp())}

    while True:
        try:
            res = graph.get_connections(str(facebook_page_id), 'insights', **kwargs)
            assert len(res['data']) > 0
        except AssertionError:
            logging.info(msg='no more data')
            break
        except GraphAPIError as e:
            if e.code == 17:
                logging.log(msg='backing off', level=logging.INFO)
                time.sleep(300)
                continue
            else:
                raise e

        for dp in res['data'][0]['values']:  # get each country's views for artist, for each ts in values
            if 'value' in dp and dp['value']:
                value = dp['value']
                date = dp['end_time'].split('T')[0]
                try:
                    artist_data[date].extend([{'location': iso, 'likes': value[iso]} for iso in value])
                except KeyError:
                    artist_data[date] = [{'location': iso, 'likes': value[iso]} for iso in value]

        time.sleep(1)

        if is_crawl_all:
            previous = res['paging']['previous'] if 'previous' in res['paging'] else None
            if not previous:
                break

            since = int(previous.split('since=')[-1].split('&')[0])
            until = int(previous.split('until=')[-1].split('&')[0])
            if kwargs['since'] <= since:
                break

            kwargs['since'] = since
            kwargs['until'] = until
        else:
            break

    return artist_data


def find_my_fbid(search_term, quiet=True):
    """
    Tries to find a valid facebook page id for a search term (ie url)
    Args:
        search_term: url, canonical name, etc to search for fbid
        quiet: don't emit error

    Returns: valid fbid for search_term, or None
    """
    try:
        resp = requests.post('https://findmyfbid.com', data={'url': search_term})
        resp.raise_for_status()
        fbid = json.loads(resp.text)['id']
    except Exception as e:
        if not quiet:
            from workbench.core.utils import err
            err(e)
        return None

    return fbid
