# Author: Chris Harland
# Dota 2 Data Parsing and Scraping from dotabuff.com
#
#

from __future__ import division
import re
import json
import pandas as pd
import numpy as np
import bs4
import requests


stat_fix = re.compile('[k]')
dig_fix = re.compile('(\d)')
match_id_parse = re.compile('\d+')
match_href = re.compile('^\/matches\/')
pick_ban = re.compile('(ban|pick)')
hero_finder = re.compile('\/heroes\/\w+')
faction_find = re.compile('faction\-')

stat_col_names = ['Team','Hero','Player','Level','Kills','Deaths','Assists','Gold','Last_Hits','Denies','XPM','GPM','Hero_Dmg','Hero_Healed','Tower_Damage','Item_1','Item_2','Item_3','Item_4','Item_5','Item_6']
ability_col_names = ['Hero'] + ['Level_%d' % x for x in range(1,26)]
details_col_names = ['Tournment','Mode','Region','Duration']
sub_pages = ['','builds','kills','farm','objectives','runes','vision','chat','log']
sides = ['radiant','dire']

def get_match_id(soup):
    match_id = match_id_parse.findall(soup.find('div',{'class':'content-header-title'}).text)[0]
    return match_id

def parse_match_section(sec):
    title = sec.find('header').text
    # Match table
    sec_stats = []
    for tr in sec.find('table').find_all('tr')[1:6]:
        tds = tr.find_all('td')
        hero = tds[0].find('img')['title']
        player = tds[1].text
        stats = [int(''.join(dig_fix.findall(stat_fix.sub('00',tds[i].text)))) for i in range(3,15)]
        items = [x['alt'] for x in tds[15].find_all('img')]
        if len(items) < 6:
            items = items + ['none']*(6-len(items))
        parsed_row = [hero] + [player] + stats + items
        sec_stats = np.append(sec_stats,np.array(parsed_row))
    team_stats = np.array([title]*5).reshape((5,1))
    team_stats = np.hstack((team_stats,np.reshape(sec_stats,(5,20))))
    return team_stats

def parse_game_stats(soup):
    match_id = get_match_id(soup)
    teams = np.array(['radiant']*5 + ['dire']*5)
    team_stats = []
    for side in sides:
        sec = soup.find('section',{'class':side})
        team_stats = np.append(team_stats, parse_match_section(sec))
    team_stats = np.reshape(team_stats, (10,21))
    df = pd.DataFrame(team_stats, columns = stat_col_names)
    df['Side'] = teams
    df['match_id'] = match_id
    return df

def parse_diff_xp(soup):
    match_id = get_match_id(soup)
    plot_json = json.loads(soup.find('div',{'data-flot':'chart-value-minute'})['data-json'])
    df = pd.DataFrame(plot_json[1]['data'], columns = ['time','xp'])
    df['match_id'] = match_id
    return df

def parse_chart(soup):
    plot_json = json.loads(soup['data-json'])
    df = pd.DataFrame(plot_json[1]['data'], columns = ['time','value'])
    return df

def parse_hero_chart(soup):
    df = pd.DataFrame()
    for s in json.loads(soup['data-json']):
        hero = s['label']
        ts = s['data']
        df_hero = pd.DataFrame(ts)
        df_hero.columns = ['time','value']
        df_hero['hero'] = hero
        df = df.append(df_hero)
    return df[['time','hero','value']]

def parse_diff_gold(soup):
    match_id = get_match_id(soup)
    plot_json = json.loads(soup.find('div',{'data-flot':'chart-value-minute'})['data-json'])
    df = pd.DataFrame(plot_json[1]['data'], columns = ['time','gold'])
    df['match_id'] = match_id
    return df

def parse_ability_builds(soup):
    match_id = get_match_id(soup)
    ability_arr = []
    for team in soup.find_all('article',{'class':'ability-builds'}):
        for tr in team.find_all('tr')[1::]:
            skill_arr = []
            for td in tr.find_all('td'):
                if td.find('img') is not None:
                    skill_arr.append(td.find('img')['alt'])
                else:
                    skill_arr.append('none')
            ability_arr = ability_arr + skill_arr
    ability_arr = np.reshape(ability_arr, (10,26))
    df = pd.DataFrame(ability_arr, columns = ability_col_names)
    df['match_id'] = match_id
    return df

def get_match_details(soup):
    match_id = get_match_id(soup)
    dds = soup.find('div',{'id':'content-header-secondary'}).find_all('dd')
    details = [x.text for x in dds[:-1]]
    details[-1] = duration_to_sec(details[-1])
    time_stamp = pd.to_datetime(dds[-1].find('time')['datetime'])
    match_details = np.append(match_id, details)
    match_details = np.append(match_details, time_stamp)
    match_details[0] = int(match_details[0])
    return match_details
    
def duration_to_sec(dur_str):
    if ':' not in dur_str:
        return 0
    vals = [int(x) for x in dur_str.split(':')]
    if vals[0] < 0:
        return vals[0]*60 - vals[1]
    else:
        return vals[0]*60 + vals[1]

def get_match_ids(soup):
    return [int(x.text) for x in soup.find_all('a', href = match_href, text = match_id_parse)]

def pull_all_match_pages(ids):
    for m_id in ids:
        base_url = 'http://www.dotabuff.com/matches/%d' % m_id
        for page in sub_pages:
            fetch_url = base_url + '/' + page
            fName = os.path.join('D:/dota2_matches/%s/' % page, '%d_%s.txt' % (m_id,page))
            with open(fName, 'wb') as txt_file:
                txt_file.write(requests.get(fetch_url).text.encode('UTF-8'))   
                
def get_picks_and_bans(soup):
    match_id = get_match_id(soup)
    pb = soup.findAll('div',{'class':pick_ban})
    pb_phase = []
    for hero in pb[1::]:
        sel = hero['class'][0]
        name = hero.find('img')['alt']
        seq = int(hero.find('div',{'class':'seq'}).text)
        pb_phase = pb_phase + [seq,sel,name]
    df = pd.DataFrame(np.reshape(pb_phase, (-1,3)), columns = ['sequence', 'action','hero'])
    df = df[df.action != 'picks-inline']
    df['sequence'] = df.sequence.astype(int)
    df['match_id'] = match_id
    return df.sort('sequence').reset_index(drop = True)

def parse_item_sequence(soup):
    match_id = get_match_id(soup)
    players = soup.findAll('section',{'class':'performance-artifact'})
    item_builds = pd.DataFrame()
    for player in players:
        hero = player.find('a', {'href':hero_finder}).find('img')['alt']
        for buy in player.findAll('div',{'class':'segment expanded'}):
            time_stamp = duration_to_sec(buy.find('div',{'class':'time'}).text)
            items = [x['alt'] for x in buy.findAll('img')]
            df_user = pd.DataFrame(items)
            df_user['time_stamp'] = time_stamp
            df_user['hero'] = hero
            item_builds = item_builds.append(df_user)
    #df = pd.DataFrame(item_builds)
    #df['match_id'] = match_id
    item_builds.columns = ['item','time_stamp','hero']
    item_builds['match_id'] = match_id
    return item_builds[['match_id','time_stamp','hero','item']].sort(['hero','time_stamp']).reset_index(drop=True)

def parse_farm_charts(soup):
    match_id = get_match_id(soup)
    charts = soup.findAll('div',{'data-flot':'chart-value-minute'})
    df_xp = parse_chart(charts[0])
    df_xp.columns = ['time','team_xp']
    df_gold = parse_chart(charts[1])
    df_gold.columns = ['time','team_gold']
    df_last_hits = parse_hero_chart(charts[2])
    df_last_hits.columns = ['time','hero','last_hits']
    df_hero_gold = parse_hero_chart(charts[3])
    df_hero_gold.columns = ['time','hero','gold']
    df_team = df_xp.merge(df_gold)
    df_hero = df_last_hits.merge(df_hero_gold)
    df_team['match_id'] = match_id
    df_hero['match_id'] = match_id
    return df_team,df_hero

def extract_amounts(txt):
    t = ''.join(pull_nums.findall(txt))
    if t == '':
        return 0
    else:
        return int(t)
    
def extract_wards(txt):
    nums = [extract_amounts(x) for x in txt.split('/')]
    return nums
    
def parse_performace(soup):
    match_id = get_match_id(soup)
    hero_perf = []
    for row in soup.findAll('tr',{'class':faction_find}):
        hero = row.find('img')['alt']
        tds = row.findAll('td')
        values = [extract_amounts(x.text) for x in tds[2::]]
        hero_perf = hero_perf + [hero] + values
    df = pd.DataFrame(np.reshape(hero_perf,(-1,12)),
                      columns = ['hero','tower_kills','barracks_kills',
                                 'roshan_kills','tower_dmg','structure_dmg',
                                 'aegis_pickup','aegis_use','cheese_pickup',
                                 'cheese_use','rune_pickup','rune_use'])
    df['match_id'] = match_id
    return df

def parse_runes(soup):
    match_id = get_match_id(soup)
    hero_perf = []
    for row in soup.findAll('tr',{'class':faction_find}):
        hero = row.find('img')['alt']
        tds = row.findAll('td')
        values = [extract_amounts(x.text) for x in tds[2::]]
        hero_perf = hero_perf + [hero] + values
    df = pd.DataFrame(np.reshape(hero_perf,(-1,11)),
                      columns = ['hero','activated','bottled',
                                 'top','bottom','double_damage',
                                 'haste','regen','invis',
                                 'illusion','bounty'])
    df['match_id'] = match_id
    return df

def parse_vision(soup):
    match_id = get_match_id(soup)
    hero_perf = []
    for row in soup.findAll('tr',{'class':faction_find}):
        hero = row.find('img')['alt']
        tds = row.findAll('td')
        values = [extract_wards(x.text) for x in tds[2:12]]
        hero_perf = hero_perf + [hero] + [item for sublist in values for item in sublist] + [duration_to_sec(tds[12])]
    df = pd.DataFrame(np.reshape(hero_perf,(-1,17)),
                      columns = ['hero','obs_owned','sent_owned',
                                 'obs_placed','sent_placed','obs_killed',
                                 'sent_killed','dust_owned','dust_used',
                                 'dust_hits','dust_acc','smoke_owned',
                                 'smoke_used','smoke_hits','gem_owned',
                                 'gem_dropped','gem_time_carried'])
    df['match_id'] = match_id
    return df

def update_match_ids(current_ids = None):
	captured_match_ids = []
	for i in range(1,51):
	    match_soup = bs4.BeautifulSoup(requests.get('http://www.dotabuff.com/esports/matches?page=%d' % i).text)
	    captured_match_ids = captured_match_ids + get_match_ids(match_soup)

	return np.unique(captured_match_ids + current_ids)

