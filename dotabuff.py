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
import time
import os


stat_fix = re.compile('[k]')
dig_fix = re.compile('(\d)')
match_id_parse = re.compile('\d+')
match_href = re.compile('^\/matches\/')
pick_ban = re.compile('(ban|pick)')
hero_finder = re.compile('\/heroes\/\w+')
faction_find = re.compile('faction\-')

stat_col_names = ['Team','Hero', 'player_id','Player','Level','Kills','Deaths','Assists','Gold','Last_Hits','Denies','XPM','GPM','Hero_Dmg','Hero_Healed','Tower_Damage','Item_1','Item_2','Item_3','Item_4','Item_5','Item_6']
ability_col_names = ['Level_%d' % x for x in range(1,26)]
details_col_names = ['Tournment','Mode','Region','Duration']
sub_pages = ['','builds','kills','farm','objectives','runes','vision','chat','log']
sides = ['radiant','dire']
detail_cols = ['match_id', 'tournament_name','game_mode','location','game_length','start_time']

def get_match_id(soup):
    match_id = match_id_parse.findall(soup.find('div',{'class':'content-header-title'}).text)[0]
    return match_id

def build_player_ids(soup):
    build_ids = []
    for user in soup.find_all('section',{'class':'performance-artifact'}):
        build_ids.append(match_id_parse.findall(user.find('a',href=re.compile('player'))['href'])[0])
    return build_ids

def build_hero_list(soup):
    hero_list = []
    for user in soup.find_all('img',{'class':'image-hero image-avatar'}):
        hero_list.append(user['alt'])
    return hero_list

def parse_match_section(sec):
    title = sec.find('header').text
    # Match table
    sec_stats = []
    for tr in sec.find('table').find_all('tr')[1:6]:
        tds = tr.find_all('td')
        hero = tds[0].find('img')['title']
        player = tds[1].text
        player_id = match_id_parse.findall(tds[1].find('a')['href'])[0]
        stats = [int(''.join(dig_fix.findall(stat_fix.sub('00',tds[i].text)))) for i in range(3,15)]
        items = [x['alt'] for x in tds[15].find_all('img')]
        if len(items) < 6:
            items = items + ['none']*(6-len(items))
        parsed_row = [hero] + [player_id] + [player] + stats + items
        sec_stats = np.append(sec_stats,np.array(parsed_row))
    team_stats = np.array([title]*5).reshape((5,1))
    team_stats = np.hstack((team_stats,np.reshape(sec_stats,(5,21))))
    return team_stats

def parse_game_stats(soup):
    match_id = get_match_id(soup)
    teams = np.array(['radiant']*5 + ['dire']*5)
    team_stats = []
    for side in sides:
        sec = soup.find('section',{'class':side})
        team_stats = np.append(team_stats, parse_match_section(sec))
    team_stats = np.reshape(team_stats, (10,22))
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
    player_ids = build_player_ids(soup)
    hero_list = build_hero_list(soup)
    for player in soup.find_all('section',{'class':'performance-artifact'}):
        skill_arr = ['']*25
        for skill in player.find_all('div',{'class':'skill'}):
            name = skill.find('img')['alt']
            for entry in skill.find_all('div',{'class':'entry choice'}):
                level = int(entry.text)
                skill_arr[level - 1] = name
        ability_arr = ability_arr + skill_arr
    ability_arr = np.reshape(ability_arr, (10,25))
    df = pd.DataFrame(ability_arr, columns = ability_col_names)
    df['match_id'] = match_id
    df['hero'] = hero_list
    df['player_id'] = player_ids
    return df

def get_match_details(soup):
    match_id = get_match_id(soup)
    dds = soup.find('div',{id:'content-header-secondary'}).find_all('dd')
    details = [x.text for x in dds[:-1]]
    details[-1] = duration_to_sec(details[-1])
    time_stamp = pd.to_datetime(dds[-1].find('time')['datetime'])

    if len(dds) < 6:
        match_id = np.append(match_id, ['Practice'])

    match_details = np.append(match_id, details)
    match_details = np.append(match_details, time_stamp)
    match_details[0] = int(match_details[0])
    df = pd.DataFrame(np.reshape(match_details, (1,6)), columns=['match_id', 'tournament_name','game_mode','location','game_length','start_time'])
    return df
    
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

def pull_all_match_pages(ids, path = './match_parse/'):
    for m_id in ids:
        base_url = 'http://www.dotabuff.com/matches/%d' % m_id
        for page in sub_pages:
            fetch_url = base_url + '/' + page
            fName = os.path.join(path, '%d_%s.txt' % (m_id,page))
            with open(fName, 'wb') as txt_file:
                txt_file.write(requests.get(fetch_url).text.encode('UTF-8'))
                time.sleep(1)
                
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
            time_txt = buy.find('div',{'class':'time'})
            if time_txt is None:
                continue
            else:
                time_stamp = duration_to_sec(time_txt.text)
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
    t = ''.join(match_id_parse.findall(txt))
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
        time.sleep(1)

	return np.unique(captured_match_ids + current_ids)


def get_latest_matches():
    # Get new matches
    new_matches = update_match_ids()

    # Keep only match ids that haven't been parsed yet
    # TODO: pull current matches from DB
    #sql_q = 'SELECT DISTINCT match_id FROM match_stats;'
    # db.cursor()

    matches_to_pull = [x for x in new_matches if x not in parsed_matches]

    # DL all the files needed for the matches
    pull_all_match_pages(matches_to_pull)

    print "Found %d matches, pulled %d new matches" % (len(new_matches), len(matches_to_pull))


def parse_recent_pull(path = './match_parse/'):
    # find all of the match ids to parse
    ids_to_parse = [f[:-5] for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]

    for id in ids_to_parse:
        # read in raw files
        fName_match = id + '_.txt'
        fName_builds = id + '_builds.txt'
        fName_farm = id + '_farm.txt'
        fName_obj = id + '_objectives.txt'
        fName_runes = id + '_runes.txt'
        fName_vision = id + '_vision.txt'
        soup_match = bs4.BeautifulSoup(open(path % fName_match).read())
        soup_build = bs4.BeautifulSoup(open(path % fName_builds).read())
        soup_farm = bs4.BeautifulSoup(open(path % fName_farm).read())
        soup_obj = bs4.BeautifulSoup(open(path % fName_obj).read())
        soup_runes = bs4.BeautifulSoup(open(path % fName_runes).read())
        soup_vision = bs4.BeautifulSoup(open(path % fName_vision).read())

        # parse info
        
        # From match file
        df_details = get_match_details(soup_match)
        df_stats = parse_game_stats(soup_match)
        df_pickban = get_picks_and_bans(soup_match)
        df_xp = parse_diff_xp(soup_match)

        # From build file
        df_build = parse_ability_builds(soup_build)
        df_item = parse_item_sequence(soup_build)

        # From farm file
        df_farm = parse_farm_charts(soup_farm)

        # From obj file
        df_perf = parse_performace(soup_obj)

        # From runes file
        df_runes = parse_runes(soup_runes)

        # From vision file
        df_vision = parse_vision(soup_vision)

        # write to SQL tbls
