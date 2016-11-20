import pandas as pd, numpy as np, math, requests

class ffball_data():
    def __init__(self,position):
        self.scoring_type = 'ppr' #we have a ppr league, but most the stats i collect are not for ppr...
        self.season=2016 #season for data
        self.position=position # position that we care about. i've only really tried with QB...
        self.TEMPdata_frame = pd.DataFrame() #create a temp data place
        self.data_frame = pd.DataFrame() #this will be the primary data site


    def get_urls(self,week,pred=False): #create dictionary of urls to get data from
        urls = dict()
        
        urls['fpro'] = "http://www.fantasypros.com/nfl/projections/%s.php?week=%d?scoring=%s"\
            % (self.position.lower(),week,self.scoring_type.upper)

        pos_dict = {'QB':10, 'RB':20, 'WR':30, 'TE':40, 'K':80}
        league_dict = {'fftoday':1,'fft_ppr':107644,'yahoo':17,'FFPC':107437,'NFFC':5}
        urls['fftoday'] = "http://www.fftoday.com/rankings/playerwkproj.php?Season=%d&GameWeek=%d&PosID=%d&LeagueID=%d"\
            % (self.season,week,pos_dict[self.position],league_dict['fft_ppr'])
        urls['yahoo'] = "http://www.fftoday.com/rankings/playerwkproj.php?Season=%d&GameWeek=%d&PosID=%d&LeagueID=%d"\
            % (self.season,week,pos_dict[self.position],league_dict['yahoo'])

        urls['cbs_predict'] = "http://www.cbssports.com/fantasy/football/stats/weeklyprojections/%s/%d/avg/%s?&_1:col_1=1&print_rows=9999"\
            % (self.position,week,self.scoring_type)
        urls['cbs_actual1'] = "http://www.cbssports.com/fantasy/football/stats/sortable/points/%s/standard/week-%d?&_1:col_1=1&print_rows=9999"\
            % (self.position,week-1)
        if pred==False:
            urls['cbs_actual2'] = "http://www.cbssports.com/fantasy/football/stats/sortable/points/%s/standard/week-%d?&_1:col_1=1&print_rows=9999"\
                % (self.position,week)

        urls['fsharks'] = "http://www.fantasysharks.com/apps/Projections/WeeklyProjections.php?pos=%s&format=json&week=%d"\
            % (self.position,week)
        
        urls['espn'] = 'hold'
        urls['nfl'] = 'hold'
        return urls
    
    def create_actualData_column(self,urls): 
        #get actual performance data from the previous week. To include this data I don't collect predicted data for week 1 (no week 0 performance data). 
        #not sure how much i like this...
        name_column = 'Player'
        df = pd.read_html(urls['cbs_actual1'],header=2)[0]
        df = df[0:-1] #remove final row
        data = []
        for i,x in enumerate(self.TEMPdata_frame.index): #loop through players
            searchfor = [self.TEMPdata_frame.LastName[x],self.TEMPdata_frame.FirstName[x]] #get player name (hope no players have same name)
            str_search = '.*'+searchfor[0]+'.*'+searchfor[1]+'.*|.*'+searchfor[1]+'.*'+searchfor[0]+'.*' #find name match
            points = df[df[name_column].str.contains(str_search)]

            if i ==0: column_names = list(points.columns[1:])

            if len(points) == 0:
                data.append(np.zeros((1,17))[0])
            else:    
                points = points.iloc[0,1:].values
                data.append(points)

        self.TEMPdata_frame = pd.concat([self.TEMPdata_frame, pd.DataFrame(data,columns=column_names)],axis=1)
        return self.TEMPdata_frame
        
    def create_df_column(self,df,column_names,column_stats): 
        #find specific player's data and make a list of it thats in the same order as the reference list. 
        data = []
        for x in self.TEMPdata_frame.index:
            searchfor = [self.TEMPdata_frame.LastName[x],self.TEMPdata_frame.FirstName[x]]
            str_search = '.*'+searchfor[0]+'.*'+searchfor[1]+'.*|.*'+searchfor[1]+'.*'+searchfor[0]+'.*'
            points = df[df[column_names].str.contains(str_search)][column_stats]
            if len(points) == 0:
                data.append(0.0)
            elif len(points) == 1:
                try:
                    if math.isnan(points):
                        data.append(0.0)
                    else:
                        data.append(float(points))
                except:    
                    data.append(0.0)
            else:
                print('whoops!!!')
        return data
    
    def create_opp_column(self,df): 
        #get data about opponent, home or away, and bye week (or what i assume to be info about bye).  
        df.columns = ['Name','Opp']
        opp, home_away = [],[]
        for x in self.TEMPdata_frame.index:
            searchfor = [self.TEMPdata_frame.LastName[x],self.TEMPdata_frame.FirstName[x]]
            str_search = '.*'+searchfor[0]+'.*'+searchfor[1]+'.*|.*'+searchfor[1]+'.*'+searchfor[0]+'.*'
            points = df[df['Name'].str.contains(str_search)]['Opp'].values
            if len(points) == 0:
                opp.append(0)
                home_away.append(0)
            elif len(points) > 0:
                opp.append(points[0][-2:])
                if points[0][0] == '@':
                    home_away.append(1)
                else:
                    home_away.append(2)
        return opp,home_away
    
    def input_df(self,df):
        self.data_frame = df
        return self.data_frame
    
    def save_df(self):
        self.data_frame.to_pickle('%ss_%d.pkl' % (self.position,self.season))
    
    def load_df(self):
        self.data_frame = pd.read_pickle('%ss_%d.pkl' % (self.position,self.season))
        return self.data_frame
    
    def update(self,week,pred=False):
        #update the data with a new week's data
        
        urls = self.get_urls(week,pred=pred) #update urls
        
        #------------------------------------------------------------------#
        #create the first dataframe and reference list
        df = pd.read_html(urls['fpro'])[0]
        df = df[['Player','FPTS']]
        self.TEMPdata_frame = pd.DataFrame(list(df['Player'].str.split()),columns=['FirstName','LastName','Team'])
        
        if week==2: #assign player ID#s
            self.TEMPdata_frame['player_num'] = np.arange(0,len(self.TEMPdata_frame))
        else:
            player_num = ['']*(np.max(self.data_frame['player_num'])+1) #what about players that enter half way through the season??
            for players in np.unique(self.data_frame['player_num']): #make sure this works with more than one set in the df
                search_for = self.data_frame[self.data_frame['player_num']==players][['FirstName','LastName']].values[0]
                temp_df_loc = np.where((self.TEMPdata_frame['FirstName']==search_for[0]) & (self.TEMPdata_frame['LastName']==search_for[1]))[0][0]
                player_num[temp_df_loc] = players
            self.TEMPdata_frame['player_num'] = player_num
                                
        self.TEMPdata_frame['week'] = np.ones((np.max(self.TEMPdata_frame['player_num'])+1,1))*week
        urls.pop('fpro')
        
        #-----------------------------------------------------------------#
        #get actual performance data from the previous week
        self.TEMPdata_frame = self.create_actualData_column(urls)
        urls.pop('cbs_actual1')
        
        
        #-----------------------------------------------------------------#
        #get predicted data from difference sources
        for keys in urls.keys():
            if keys=='fftoday' or keys=='yahoo':
            #notice that yahoo is not yahoo predictions but fftoday predictions for yahoo's scoring system
                df = pd.read_html(urls[keys],match='FFPts',header=16)[0]
                if keys == 'fftoday':
                    [opp,home_away] = self.create_opp_column(df[['Player  Sort First: Last:','Opp']])
                    self.TEMPdata_frame['opp'] = opp
                    self.TEMPdata_frame['home_away'] = home_away
                    
                df = df[['Player  Sort First: Last:','FFPts']]
                
            
            elif keys=='cbs_predict' or keys=='cbs_actual2': #cbs predict doesnt have data for first 4 weeks...
                df = pd.read_html(urls[keys],header=2)[0]
                df = df[0:-1] #remove final row
                df = df[['Player','FPTS']]
            
            elif keys=='espn' or keys=='nfl':
            #espn and nfl are a pain to get data from so this is a little messy. I have to go page by page...
                if keys=='espn':
                    pages = np.arange(0,np.max(self.TEMPdata_frame['player_num'])+41,40)
                    pos_dict = {'QB':0, 'RB':2, 'WR':4, 'TE':6, 'K':17, 'DST':16}
                elif keys=='nfl':
                    pos_dict = {'QB':1, 'RB':2, 'WR':3, 'TE':4, 'K':7, 'DST':8}
                    pages = np.arange(1,np.max(self.TEMPdata_frame['player_num'])+27,25)
                    
                for page_num in pages:
                    if keys=='espn':
                        urls[keys] = "http://games.espn.com/ffl/tools/projections?&scoringPeriodId=%d&seasonId=%d&slotCategoryId=%d&startIndex=%d"\
                            % (week,self.season,pos_dict[self.position],int(page_num))
                        pre_df = pd.read_html(urls[keys],header=2)[0]
                        pre_df = pre_df[['PLAYER, TEAM POS', 'PTS']]
                        df_index = 14
                    elif keys=='nfl':
                        urls[keys] = "http://fantasy.nfl.com/research/projections?offset=%d&position=%d&sort=projectedPts&statCategory=projectedStats&statSeason=%d&statType=weekProjectedStats&statWeek=%d"\
                            % (int(page_num),pos_dict[self.position],self.season,week)
                        pre_df = pd.read_html(urls[keys])[0]
                        pre_df = pre_df.ix[:,[0, 12]]
                        
                        
                    if page_num==pages[0]:
                        df = pre_df
                    else:
                        df = df.append(pre_df)
                    del pre_df
                df.index = np.arange(0,len(df.index))
                
            elif keys=='fsharks':
            # have to use requests for this site
                header_data = {
                        'Accept-Encoding': 'gzip, deflate, sdch',
                        'Accept-Language': 'en-US,en;q=0.8',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64)'\
                        ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 '\
                        'Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9'\
                        ',image/webp,*/*;q=0.8',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive'
                    }

                response = requests.get(urls['fsharks'],headers=header_data)
                df = pd.DataFrame(response.json())
                df = df[['Name','FantasyPoints']]
            
            df.columns = ['Player','FPTS']
            try: #update the temporary dataframe
                self.TEMPdata_frame[keys] = self.create_df_column(df,'Player','FPTS')
            except: #let me know where the error was if this goes wrong
                print(keys)
                print(week)
            
        #------------------------------------------------------------------#
        #append temp df to main df
        if pred == True:
            return self.TEMPdata_frame
            
        elif pred == False:
            self.data_frame = self.data_frame.append(self.TEMPdata_frame)
            self.TEMPdata_frame = pd.DataFrame()
            return self.data_frame
