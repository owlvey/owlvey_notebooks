import pandas as pd 
import numpy as np
from FileGateway import FileGateway


def generate_hourly(data): 
    output_hourly = data.groupby(
        ['Source', 'year', 'month', 'month_name', 'week', 'weekday', 'day', 'hour']).agg({
        'total': 'sum',
        'ava': 'sum',
        'exp': 'sum',
        'lat': 'mean'
    }).reset_index()
    output_hourly['ava_prop'] = output_hourly['ava'].divide(output_hourly['total'])
    output_hourly['exp_prop'] = output_hourly['exp'].divide(output_hourly['total'])
    output_hourly.replace([np.inf, -np.inf], 0)
    return output_hourly

def generate_daily(data):
    output_daily = data.groupby(
        ['Source', 'year', 'month', 'month_name', 'week', 'fortnight', 'weekday', 'day']).agg({
        'total': 'sum',
        'ava': 'sum',
        'exp': 'sum',
        'lat': 'mean'
    }).reset_index()
    output_daily['ava_prop'] = output_daily['ava'].divide(output_daily['total'])
    output_daily['exp_prop'] = output_daily['exp'].divide(output_daily['total'])
    output_daily.replace([np.inf, -np.inf], 0)
    return output_daily

def generate_month(output_daily):
    output_month = output_daily.groupby(
        ['Source', 'year', 'month', 'month_name']).agg({
        'total': 'sum',
        'ava': 'sum',
        'exp': 'sum',
        'lat': 'mean',
        'ava_prop': 'mean', 
        'exp_prop': 'mean'
    }).reset_index()
    output_month.replace([np.inf, -np.inf], 0)
    return output_month


def generate_fortnight(output_daily):
    output_fortnight = output_daily.groupby(
        ['Source', 'year', 'month', 'month_name', 'fortnight']).agg({
        'total': 'sum',
        'ava': 'sum',
        'exp': 'sum',
        'lat': 'mean'
    }).reset_index()
    output_fortnight['ava_prop'] = output_fortnight['ava'].divide(output_fortnight['total'])
    output_fortnight['exp_prop'] = output_fortnight['exp'].divide(output_fortnight['total'])
    output_fortnight.replace([np.inf, -np.inf], 0)

    output_fortnight_summary = output_fortnight.groupby(
        ['year', 'month', 'month_name', 'fortnight']
    ).agg({
        'total': 'sum',
        'ava': 'sum',
        'exp': 'sum',
        'lat': 'mean'
    }).reset_index()
    output_fortnight_summary['ava_prop'] = output_fortnight_summary['ava'].divide(output_fortnight_summary['total'])
    output_fortnight_summary['exp_prop'] = output_fortnight_summary['exp'].divide(output_fortnight_summary['total'])
    output_fortnight_summary.replace([np.inf, -np.inf], 0)
    return output_fortnight, output_fortnight_summary

def generate_slo(file_gateway, output_month):
    journeys, journeyMaps, features, indicators, sources = file_gateway.read_metadata()
    
    merged = journeys.merge(journeyMaps, left_on='Journey', right_on='Journey', how='left')
    
    merged = merged.merge(features, left_on='Feature', right_on='Feature', how='left')
    
    merged = merged.merge(indicators, left_on='Feature', right_on='Feature', how='left')
    
    merged = merged.merge(sources, left_on='Source', right_on='Source', how='left')

    output_slo = merged.merge(output_month, left_on='Source', right_on='Source', how='left')

    output_slo["ava_debt"] = output_slo["ava_prop"] - output_slo["AvailabilitySlo"]
    output_slo["ava_debt"] = abs(np.where(output_slo["ava_debt"] < 0, -1 * output_slo["ava_debt"], 0))

    output_slo["exp_debt"] = output_slo["exp_prop"] - output_slo["ExperienceSlo"]
    output_slo["exp_debt"] = abs(np.where(output_slo["exp_debt"] < 0, -1 * output_slo["exp_debt"], 0))

    output_slo["lat_debt"] = output_slo["lat"] - output_slo["LatencySlo"]
    output_slo["lat_debt"] = abs(np.where(output_slo["lat_debt"] < 0, -1 * output_slo["lat_debt"], 0))

    output_slo = output_slo[ ["Group", "Journey", "AvailabilitySLA", "AvailabilitySlo" , "ExperienceSlo",  "LatencySLA",  "LatencySlo", "Feature", "Source", "year" , "month", "month_name", "total", "ava", "ava_prop", "ava_debt", "exp", "exp_prop", "exp_debt", "lat", "lat_debt"]]

    return output_slo
    

if __name__ == "__main__":
    file_gateway = FileGateway('./input/eshopping.xlsx', './input/data.csv', './output')

    data = file_gateway.read_data()
    hourly = generate_hourly(data)
    daily = generate_daily(data)
    monthly = generate_month(daily)
    fortnight, fortnight_summary = generate_fortnight(daily)
    slo = generate_slo(file_gateway, monthly)
    
    #file_gateway.write_hourly(hourly)
    #file_gateway.write_daily(daily)
    #file_gateway.write_month(monthly)
    #file_gateway.write_fortnight(fortnight)
    #file_gateway.write_fortnight_summary(fortnight_summary)
    file_gateway.write_slo_group(slo)


