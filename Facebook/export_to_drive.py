from workbench.core.google import ExportPivotToDriveTask


class ExportMarketingCSVTask(ExportPivotToDriveTask):
    name = 'ExportMarketingCSVTask'
    description = 'Exports marketing file'

    data_folder_id = '0B_Y0lB2QTTMaUy1XNUVoYnRtSGc'
    data_folder_name = 'marketing'
    filetype = 'csv'
    permissions = [{
        'type': 'user',
        'role': 'writer',
        'value': 'sarah@mymusictaste.com'
    }]
    query = """SELECT fb.ad_set as Ad_Set, SPLIT_PART(fb.ad_set, '-', 1) as Artist,
            fb.starts as Ad_Set_Starts, fb.ends as Ad_Set_Ends, fb.duration as Duration,
            sub1.daily_make_count as Makes, sub2.cumulative_make_count as Cumulative_Makes,
            sub3.total_make_count as Total_Makes, fb.leads as Leads,
            fb.add_to_wishlist as Add_To_Wishlist, fb.complete_registrations as Complete_Registrations,
            fb.link_clicks as Link_Clicks,  fb.period_frequency as Period_Frequency,
            fb.reach as Daily_Reach, fb.period_reach as Period_Reach, fb.period_lead as Period_Leads,
            fb.potential_audience_size as Potential_Audience_Size,
            fb.potential_audience_size_progress as Potential_Audience_Size_Progress,
            (fb.daily_budget_spent / sub1.daily_make_count) as Cost_Per_Make, fb.cost_per_lead as Cost_Per_Lead,
            fb.cost_per_add_to_wishlist as Cost_Per_Add_To_Wishlist,
            fb.cost_per_1000_people_reach as Cost_Per_1000_People_Reach, fb.amount_spent as Amount_Spent,
            fb.budget as Budget, fb.daily_budget as Daily_Budget, fb.daily_budget_spent as Daily_Budget_Spent,
            fb.period_start as Period_Start, fb.period_end as Period_End
            FROM mmt_facebook_ads AS fb LEFT JOIN

            (SELECT fb.ad_set, fb.ts, count(fb.ad_set) AS daily_make_count FROM mmt_facebook_ads AS fb
             LEFT JOIN m2t_promotionuserrequest AS m
             ON (fb.promotion_id = m.promotion_id OR (fb.artist_id = m.artist_id AND fb.city_id = m.city_id))
             AND m.make_time::date = ts::date
             WHERE fb.promotion_id IS NOT NULL OR (fb.artist_id IS NOT NULL AND fb.city_id IS NOT NULL)
             GROUP BY fb.ad_set, fb.ts) AS sub1
            ON fb.ad_set = sub1.ad_set and fb.ts = sub1.ts

            INNER JOIN (SELECT fb.ad_set, fb.ts, count(fb.ad_set) AS cumulative_make_count FROM mmt_facebook_ads AS fb
             LEFT JOIN m2t_promotionuserrequest AS m
             ON (fb.promotion_id = m.promotion_id OR (fb.artist_id = m.artist_id AND fb.city_id = m.city_id))
             AND m.make_time::date <= ts::date AND m.make_time::date >= fb.starts
             WHERE fb.promotion_id IS NOT NULL OR (fb.artist_id IS NOT NULL AND fb.city_id IS NOT NULL)
             GROUP BY fb.ad_set, fb.ts) AS sub2
            ON sub1.ad_set = sub2.ad_set AND sub1.ts = sub2.ts

            INNER JOIN (SELECT fb.ad_set, fb.ts, count(fb.ad_set) AS total_make_count FROM mmt_facebook_ads AS fb
             LEFT JOIN m2t_promotionuserrequest AS m
             ON fb.promotion_id = m.promotion_id OR (fb.artist_id = m.artist_id AND fb.city_id = m.city_id)
             AND m.make_time::date <= ts::date
             WHERE fb.promotion_id IS NOT NULL OR (fb.artist_id IS NOT NULL AND fb.city_id IS NOT NULL)
             GROUP BY fb.ad_set, fb.ts) AS sub3
            ON sub2.ad_set = sub3.ad_set AND sub2.ts = sub3.ts
            WHERE fb.ts = %(timestamp)s;"""

    def generate_file_header(self):
        return 'Ad_Set,Artist,Ad_Set_Starts,Ad_Set_Ends,Duration,Makes,Cumulative_Makes,Total_Makes,Leads,' \
               'Add_To_Wishlist,Complete_Registrations,Link_Clicks,Period_Frequency,Daily_Reach,Period_Reach,' \
               'Period_Leads,Potential_Audience_Size,Potential_Audience_Size_Progress,' \
               'Cost_Per_Make,Cost_Per_Lead,Cost_Per_Add_To_Wishlist,Cost_Per_1000_People_Reach,Amount_Spent,' \
               'Budget,Daily_Budget,Daily_Budget_Spent,Period_Start,Period_End'
