import argparse
import numpy as np
import pandas as pd
separator = '\t'
from timeit import default_timer as timer

start = timer()
#The score is calculated as follows:
# Sum [for each id individual]:
#      Sum [for each POI]:
#          If times_POI_oriFile> times_POI_anonymFile:
#               times_POI_anonymFile / times_POI_oriFile
#          Otherwise :
#               times_POI_oriFile / times_POI_anonymFile
def calcul_time(df):
    # How to calculate times_POI:
    # For each id:
    #    For each (latitude, longitude):
    #         For each type_POI (NIGHT,WORK,WEEKEND):
    #             for WORK and WEEKEND: time_POI = sum(for each day: date.max() - date.min())
    #             for NIGHT : time1 = sum(for each day: datetime_1.max() - datetime_1.min()) with night_start < datetime_1 < 24h
    #                         time2 = sum(for each day: datetime_2.max() - datetime_2.min()) with 00h < datetime_2 < night_end
    #                         time3 = datetime_2.min() - datetime_1.max() if datetime_1 and datetime_2 belong 2 consecutive days
    #                         time_POI = time1+time2+time3

    #Calcul times_POI for WORK,WEEKEND and time1 + time2 of NIGHT
    grp = df.groupby(['id','latitude','longitude','day','poi'])['date']
    grp = (grp.max() - grp.min()).reset_index()
    grp.date = grp.date.dt.total_seconds()
    grp.loc[(grp.poi == "NIGHT_S") | (grp.poi == "NIGHT_E"), 'poi'] = "NIGHT"
    grp = grp.groupby(['id','latitude','longitude','poi'])['date'].sum().reset_index()
    grp.rename(columns={'date':'time'}, inplace=True)

    #Calcul time3 for NIGHT
    night_s = df.loc[df.poi == "NIGHT_S"].groupby(['id','latitude','longitude','day'])['date'].max().reset_index() # max of datetime with time > night_start
    night_e = df.loc[df.poi == "NIGHT_E"].groupby(['id','latitude','longitude','day'])['date'].min().reset_index() # min of datetime with time < night_end
    night = pd.merge(night_s, night_e, how='left', on=['id', 'latitude', 'longitude'])  #merge 2 dataframe night_s and night_e
    night = night.dropna(subset=['day_x','day_y'])  #day_x, day_y is created from pd.merge; _x, _y is suffixes default
    night = night.drop(night[(night.day_y - night.day_x) / np.timedelta64(1, 'D') != 1].index)
    night['time'] = (night.date_y - night.date_x).dt.total_seconds()
    night.drop(['day_x','day_y','date_x','date_y'], inplace=True, axis=1)
    night = night.groupby(['id','latitude','longitude'])['time'].sum().reset_index()

    # Calcul total_time for NIGHT
    time_POI = pd.merge(grp, night, how='left', on=['id', 'latitude', 'longitude']) # Merge 2 dataframe grp and night 
    time_POI.loc[np.isnan(time_POI.time_y), "time_y"] = 0  # set values = 0 for NaN values
    time_POI['total'] = time_POI.time_x + time_POI.time_y   # time_POI = time1+time2+time3
    time_POI.drop(['time_x','time_y'], inplace=True, axis=1)

    #Find 3 max values in columns 'total' - 3 most frequented POIs in terms of attendance time
    max_value = time_POI.groupby(['id','poi'])['total'].apply(lambda x: x.nlargest(nbPOI)).reset_index() 
    max_value['latitude'] = max_value.level_2.apply(lambda x: time_POI.latitude[x]) # 'level_2' is a column which is generated automatically by function nlargest
    max_value['longitude'] = max_value.level_2.apply(lambda x: time_POI.longitude[x])
    max_value.drop(["level_2"], inplace = True, axis = 1) 

    return max_value

def calcul_score(row):
    if row['total_x'] == 0 and row['total_y'] == 0 :
        score = 1
    elif row['total_y'] > row['total_x']:
        score = row['total_x'] / row['total_y']
    else:
        score = row['total_y'] / row['total_x']
    return score

def main(originalFile, anonymisedFile, parameters={"size":2,"nbPOI":3,"night_start":22,"night_end":6,"work_start":9,"work_end":16,"weekend_start":10,"weekend_end":18}):
    size = parameters['size']
    global nbPOI
    nbPOI = parameters['nbPOI']

    # pois = {'night_start': '22:00', 'night_end': '06:00', 'work_start': '09:00', 'work_end': '16:00', 'weekend_start': '10:00', 'weekend_end': '18:00'}
    timeline = {i : pd.to_datetime(parameters[i], format = "%H").strftime("%H:%M") for i in [j for j in parameters.keys() if j not in ["size", "nbPOI"]]}
    pois = ['NIGHT_S', 'NIGHT_E', 'WORK', 'WEEKEND'] 

    def set_poi(df):
        return [
            (df['date'].dt.dayofweek < 5) & (df.index.isin(df.between_time(timeline['night_start'], '23:59:59', include_start=False, include_end=False).index)),
            (df['date'].dt.dayofweek < 5) & (df.index.isin(df.between_time('00:00', timeline['night_end'], include_start=False, include_end=False).index)),
            (df['date'].dt.dayofweek < 5) & (df.index.isin(df.between_time(timeline['work_start'], timeline['work_end'], include_start=False, include_end=False).index )),
            (df['date'].dt.dayofweek >= 5) & (df.index.isin(df.between_time(timeline['weekend_start'], timeline['weekend_end'], include_start=False, include_end=False).index ))
        ]
    s = timer()
    #OPEN FILES
    columns_name = ['id', 'date', 'latitude', 'longitude' ]
    df_ori = pd.read_csv(originalFile, names = columns_name, sep = '\t')
    df_anon = pd.read_csv(anonymisedFile, names = columns_name, sep = '\t')
    e = timer()
    print(e-s, "load file")

    # PRE_PROCESSING
    df_anon['true_id'] = df_ori.id
    #convert to datetime-type
    df_ori.date = pd.to_datetime(df_ori.date)
    df_anon.date = pd.to_datetime(df_anon.date)
    s = timer()
    print(s-e, "To datetime")
    ###############################################
    df_ori.index = pd.DatetimeIndex(df_ori['date'])
    df_anon.index = pd.DatetimeIndex(df_anon['date']) 
    #Add columns POI (NIGHT-WORK-WEEKEND)
    df_ori['poi'] = np.select(set_poi(df_ori), pois, None)
    df_anon['poi'] = np.select(set_poi(df_anon), pois, None)
    e = timer()
    print(e-s, "column poi")
    df_ori.reset_index(drop=True,inplace = True)
    df_anon.reset_index(drop=True,inplace = True)
    
    df_ori.dropna(subset=['poi'],inplace=True)
    df_anon.dropna(subset=['poi'],inplace=True)
    s = timer()
    print(s-e,"drop nan value")
    df_anon.drop(df_anon[df_anon.id == "DEL"].index, inplace=True)
    e= timer()
    print(e-s, "drop DEL row")
    #Add column 'day'
    df_ori['day'] = df_ori.date.dt.date
    df_anon['day'] = df_anon.date.dt.date
    s = timer()
    print(s-e, "add day column")
    
    # round latitude and longitude with 'size' number decimal
    df_ori.latitude = df_ori.latitude.round(size)
    df_ori.longitude = df_ori.longitude.round(size)
    df_anon.latitude = df_anon.latitude.round(size)
    df_anon.longitude = df_anon.longitude.round(size)
    e=timer()
    print(e-s, "round latitude and longitude")
    df_anon.id = df_anon.true_id   #set true_id of file anonym 

    # GET TABLE OF TIME OF ALL POSITIONS
    times_ori = calcul_time(df_ori)
    times_ano = calcul_time(df_anon)
    #Change type of id to string for function merge
    times_ori.id = times_ori.id.astype(str)
    times_ano.id = times_ano.id.astype(str)

    # GET SCORE
    score = pd.merge(times_ori, times_ano, how='left', on=['id', 'poi','latitude', 'longitude'])
    score.loc[np.isnan(score.total_y), "total_y"] = 0  # set values = 0 for NaN values
    score['score'] = score.apply(calcul_score, axis =1)
    total_size = score.shape[0]
    poi_utility = score['score'].sum() / total_size 
    s = timer()
    print(s-e, "calcul score")
    return poi_utility

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.original, args.anonymized))

end = timer()
print(end-start, "total")