import os
import pandas as pd
import numpy as np

class FileGateway: 

    def __init__(self, metadata, data, output_dir):
        super().__init__()
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.base_directory = os.path.join(dir_path,  './')
        self.metadata = metadata
        self.data = data
        self.output_dir = output_dir

    def read_data(self):
        headers = ['Source', 'start', 'end', 'total', 'ava', 'exp', 'lat']
        data = pd.read_csv(self.data, sep=";", names=headers, parse_dates=['start', 'end']) # nrows = 10000

        data['year'] = data['start'].dt.strftime('%Y') 
        data['weekday'] = data['start'].dt.strftime('%a')
        data['month'] = data['start'].dt.strftime('%m')
        data['month_name'] = data['start'].dt.strftime('%b')
        data['week'] = data['start'].dt.strftime('%V')
        data['day'] = data['start'].dt.strftime('%e')
        data['weekday'] = data['start'].dt.strftime('%a')
        data['hour'] = data['start'].dt.strftime('%H')
        data = data.astype({"day"
        : int, 'hour': int})
        data['lat'] = data['lat'] / 1000 
        data['fortnight'] = np.where(data["day"] <= 15, 0, 1)
        return data
    
    def read_metadata(self):
        users = pd.read_excel(self.metadata, sheet_name='Users')
        squads = pd.read_excel(self.metadata, sheet_name='Squads')
        members = pd.read_excel(self.metadata, sheet_name='Members')
        sources = pd.read_excel(self.metadata, sheet_name='Sources')
        
        indicators = pd.read_excel(self.metadata, sheet_name='Indicators')
        features = pd.read_excel(self.metadata, sheet_name='Features')
        journeyMaps = pd.read_excel(self.metadata, sheet_name='JourneyMaps')
        journeys = pd.read_excel(self.metadata, sheet_name='Journeys')
        
        journeys = journeys.drop(columns=['Organization', 'Product', 'Avatar', 'Description', 'Leaders'])
        journeys.rename(columns = {'Name':'Journey'}, inplace = True) 
        journeys['LatencySlo'] = journeys['LatencySlo']/ 1000 

        journeyMaps = journeyMaps.drop(columns=['Organization', 'Product'])
        journeyMaps.rename(columns = {'Service':'Journey'}, inplace = True) 
       
        sources = sources.drop(columns=['Organization', 'Product', 'Description', 'Avatar',
                                        'GoodDefinitionAvailability',
                                        'TotalDefinitionAvailability', 'GoodDefinitionLatency',
                                        'TotalDefinitionLatency', 'GoodDefinitionExperience',
                                        'TotalDefinitionExperience', 'Percentile', 'Tags'])
        sources.rename(columns = {'Name':'Source'}, inplace = True) 
        
        features = features.drop(columns=['Organization', 'Product', 'Avatar', 'Description'])
        features.rename(columns = {'Name':'Feature'}, inplace = True) 
        indicators = indicators.drop(columns=['Organization', 'Product'])
        return journeys, journeyMaps, features, indicators, sources

    def __write_file(self, frame, target):
        target = os.path.join(self.base_directory, self.output_dir, target)
        frame.to_csv(target, sep=";", index=False)
    
    def write_hourly(self, frame):
        self.__write_file(frame, 'hourly_data.csv')

    def write_daily(self, frame):        
        self.__write_file(frame, 'daily_data.csv')
    
    def write_fortnight(self, frame):        
        self.__write_file(frame, 'fortnight_data.csv')
    
    def write_fortnight_summary(self, frame):        
        self.__write_file(frame, 'fortnight_summary_data.csv')
    
    def write_month(self, frame):
        self.__write_file(frame, 'month_data.csv')        
    
    def write_slo_group(self, frame):
        self.__write_file(frame, 'slo_montly_data.csv')                

    
