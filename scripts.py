import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

region_list = ["dolnoslaskie","kujawsko-pomorskie","lubelskie","lubuskie","lodzkie","malopolskie","mazowieckie",
"opolskie","podkarpackie","podlaskie","pomorskie","slaskie","swietokrzyskie","war-maz","wielkopolskie","zachodniopomorskie"]

def prepare_cases(df):
    # drop empty columns
    df.dropna(axis=1,how="all",inplace=True)
    # change date to datetime format
    df["Data"][212:243] = [str(x)+"0" for x in df["Data"][212:243]]
    df["Data"] = [str(df["Data"].iloc[x])+".2020" for x in (df.index[:304])] + [str(df["Data"].iloc[x])+".2021" for x in (df.index[304:])]
    
    df["Data"] = [datetime.strptime(x, "%d.%m.%Y") for x in df["Data"]]
    
    # rename columns
    column_renames = {df.columns[0]:"date",
                      df.columns[1]:"new_cases",
                      df.columns[2]:"misreported_cases",
                      df.columns[3]:"cases_rise_pct",
                      df.columns[4]:"cases_rise_week_to_week",
                      df.columns[5]:"new_cases_pct_from_active",
                      df.columns[6]:"active_cases_rise_in_pct",
                      df.columns[7]:"new_deaths",
                      df.columns[8]:"new_recoveries",
                      df.columns[9]:"inactive_cases_rise",
                      df.columns[10]:"active_cases_rise",
                      df.columns[11]:"sum_cases",
                      df.columns[12]:"sum_deaths",
                      df.columns[13]:"sum_recoveries",
                      df.columns[14]:"inactive_cases",
                      df.columns[15]:"active_cases",
                      df.columns[16]:"pct_of_deaths_inactive_cases",
                      df.columns[17]:"pct_of_recoveries_inactive_cases",
                      df.columns[18]:"pct_of_deaths",
                      df.columns[19]:"pct_of_recoveries",
                      df.columns[20]:"pct_of_inactive_cases",
                      df.columns[21]:"pct_of_active_cases"
        
    }
    df = df.rename(column_renames,axis=1)
    
    df = df.fillna(0)
    # make columns numeric
    for column in df.columns[1:]:
        df[column] = df[column].replace("","0")
        df[column] = make_numeric(df[column])
        
    return df
    
def make_numeric(column):


    column = column.apply(lambda x:float(str(x).replace("+ ","").replace("- ","-").
                        replace("%","").replace(",",".").replace("↓","0").replace("→","0").replace("n/a","0").replace("\xa0","").strip()))
    return column

def get_cases_report(spreadsheet):
    spread = spreadsheet.get_worksheet(0)
    data = spread.get_all_records(head=2)
    df = pd.DataFrame.from_dict(data)
    df.drop("",axis=1,inplace=True)
    df = prepare_cases(df)
    return df
def regional_clean(df):
    df.drop("",axis=1,inplace=True)
    
    column_renames = {
        "Województwo":"region",
        "Suma potwierdzonych przypadków":"sum_cases",
        "Suma zgonów *":"sum_deaths",
        "Suma Wyzdrowień *":"sum_recoveries",
        "Nieaktywne przypadki":"inactive_cases",
        "Aktywne przypadki *":"active_cases",
        "dzisiaj +":"rise_today",
        "wczoraj +":"rise_yesterday",
        "Średnia liczba zachorowań z 7 dni na 100 tys. mieszkańców":"7_day_mean_per_100k",
        "Populacja (GUS 2019)":"population",
        "Zapadalność na 1 tys. osób":"cases_per_1000",
        "Zgony na 1 tys. osób *":"deaths_per_1000",
        "Aktywne przypadki na 1 tys. osób  *":"active_per_1000",
        "Liczba przypadków na jeden zgon":"cases_per_death",
        "% zgonów *":"pct_of_deaths",
        "% zdrowych *":"pct_healthy",
        "% aktywnych przypadków *":"pct_active"
        
    }
    df = df.rename(column_renames,axis=1)

    for column in df.columns[1:]:
        df[column] = df[column].replace("","0")
        df[column] = make_numeric(df[column])
        
    return df

def get_regional_case_report(spreadsheet):
    spread = spreadsheet.get_worksheet(1)
    data = spread.get_all_records(head=2)
    df = pd.DataFrame.from_dict(data)
    df = df.iloc[:16,:]
    df = regional_clean(df)
    
    return df

def tests_clean(df):
    df=df.iloc[:,:-2]
    # drop empty columns
    df.dropna(axis=1,how="all",inplace=True)
    df.drop("",axis=1,inplace=True)
    # change date to datetime format
    df["Data"][213:244] = [str(x)+"0" for x in df["Data"][213:244]]
    df["Data"] = [str(df["Data"].iloc[x])+".2020" for x in (df.index[:305])] + [str(df["Data"].iloc[x])+".2021" for x in (df.index[305:])]
    
    df["Data"] = [datetime.strptime(x, "%d.%m.%Y") for x in df["Data"]]
    
    column_renames = {
        df.columns[0]:"date",
        df.columns[1]:"sum_tested_people",
        df.columns[2]:"new_tested_people",
        df.columns[3]:"sum_tests",
        df.columns[4]:"tests_in_24h",
        df.columns[5]:"antibody_tests",
        df.columns[6]:"pct of antibody tests",
        df.columns[7]:"sum_POZ_orders",
        df.columns[8]:"POZ_orders_24h",
        df.columns[9]:"POZ_orders_week_to_week_pct",
        df.columns[10]:"sum_positive_tests",
        df.columns[11]:"positive_tests_24h",
        df.columns[12]:"pct_positive_in_samples",
        df.columns[13]:"pct_positive_in_people",
        df.columns[14]:"pct_24h_positivity_rate",
        df.columns[15]:"pct_24h_positivity_rate_people",
        df.columns[16]:"sum_negatives_and_second_positives",
        df.columns[17]:"negatives_and_second_positives",
        df.columns[18]:"7_day_mean_test_nr",
        df.columns[19]:"7_day_mean_positivity_rate"
    }
    df = df.rename(column_renames,axis=1)
    
    df = df.fillna(0)
    # make columns numeric
    for column in df.columns[1:]:
        df[column] = df[column].replace("","0")
        df[column] = make_numeric(df[column])
    
    return df
def get_testing_report(spreadsheet):
    spread = spreadsheet.get_worksheet(2)
    data = spread.get_all_records(head=2)
    df = pd.DataFrame.from_dict(data)
    df = tests_clean(df)
    
    return df
def get_regional_testing_data(spread,rng,percents=False):
    df = pd.DataFrame(spread.get(rng)).iloc[1:,2:]
    df.columns = pd.DatetimeIndex(pd.date_range(pd.to_datetime("2020/11/24",format="%Y/%m/%d"),periods=len(df.columns+1)),name="date")
    
    df.index = region_list
    if percents == True:
        for column in df.columns:
            df[column] = df[column].replace("","0")
            df[column] = make_numeric(df[column])
    return df
def get_regional_testing_reports(spreadsheet):
    spread = spreadsheet.get_worksheet(3)
    regional_testing_data = {
    "daily_testing":get_regional_testing_data(spread,"4:20").transpose(),
    "daily_positive":get_regional_testing_data(spread,"24:40").transpose(),
    "daily_positive_pct":get_regional_testing_data(spread,"44:60",percents=True).transpose(),
    "rolling_mean":get_regional_testing_data(spread,"66:82",percents = True).transpose(),
    "tests_per_100k" : get_regional_testing_data(spread,"88:104").transpose(),
    "rolling_mean_per_100k":get_regional_testing_data(spread,"110:126").transpose()
    }

    return regional_testing_data
    
def get_regional_cases_data(spread,rng):
    df = pd.DataFrame(spread.get(rng)).iloc[1:,1:]
    df.columns = pd.DatetimeIndex(pd.date_range(pd.to_datetime("2020/03/04",format="%Y/%m/%d"),periods=len(df.columns+1)),name="date")

   
    df.index = region_list
    return df
def get_regional_cases_reports(spreadsheet):
    spread = spreadsheet.get_worksheet(4)
    
    regional_cases_data = {
        "new_cases_regional" : get_regional_cases_data(spread,"8:24").transpose(),
        "sum_cases_regional": get_regional_cases_data(spread,"31:47").transpose(),
        "new_deaths_regional":get_regional_cases_data(spread,"51:67").transpose(),
        "sum_deaths_regional":get_regional_cases_data(spread,"71:87").transpose(),
        "recoveries_regional":get_regional_cases_data(spread,"91:107").transpose(),
        "sum_recoveries_regional":get_regional_cases_data(spread,"111:127").transpose(),
        "active_cases_regional":get_regional_cases_data(spread,"131:147").transpose(),
        "active_cases_change":get_regional_cases_data(spread,"151:167").transpose(),
        "pct_daily_rise_regional":get_regional_cases_data(spread,"171:187").transpose(),
        "pct_active_daily_rise":get_regional_cases_data(spread,"193:209").transpose(),
        "rolling_mean_regional":get_regional_cases_data(spread,"215:231").transpose(),
        
        
    }
    return regional_cases_data
def hospitals_clean(df):
    df=df.iloc[:,:-2]
    # drop empty columns
    df.dropna(axis=1,how="all",inplace=True)
    # change date to datetime format
    df["Data"][212:243] = [str(x)+"0" for x in df["Data"][212:243]]
    df["Data"] = [str(df["Data"].iloc[x])+".2020" for x in (df.index[:304])] + [str(df["Data"].iloc[x])+".2021" for x in (df.index[304:])]
    
    df["Data"] = [datetime.strptime(x, "%d.%m.%Y") for x in df["Data"]]
    
    column_renames = {
        df.columns[0]:"date",
        df.columns[1]:"hospitalized",
        df.columns[2]:"change_d/d",
        df.columns[3]:"pct_hospitalized_in_active",
        df.columns[4]:"beds",
        df.columns[5]:"pct_taken_beds",
        df.columns[6]:"icu",
        df.columns[7]:"pct_icu_in_hospitalized",
        df.columns[8]:"icu_beds",
        df.columns[9]:"quarantined_locally",
        df.columns[10]:"from_abroad",
        df.columns[11]:"quarantined",
        df.columns[12]:"monitored_til_25.01",
    }
    df = df.rename(column_renames,axis=1)
    
    df = df.fillna(0)
    # make columns numeric
    for column in df.columns[1:]:
        df[column] = df[column].replace("","0")
        df[column] = make_numeric(df[column])
    
    return df
def get_hospital_load_report(spreadsheet):
    spread = spreadsheet.get_worksheet(5)
    data = spread.get_all_records(head=2)
    df = pd.DataFrame.from_dict(data)
    
    df = hospitals_clean(df)
    return df
def get_regional_epidemic_data(spread,rng):
    df = pd.DataFrame(spread.get(rng))
    df.fillna(0,inplace=True)
    df.columns = ["hospitalized",
                  "change_d/d",
                  "beds",
                  "pct_taken_beds",
                  "icu",
                  "icu_change_d/d",
                  "icu_beds",
                  "pct_taken_icu"]
    df = df.iloc[3:,:]
    df.index = pd.DatetimeIndex(pd.date_range(pd.to_datetime("2020/10/23",format="%Y/%m/%d"),periods=len(df.index+1)),name="date")
    
    
    for column in df.columns:
        df[column] = make_numeric(df[column])
    return df
def get_regional_hospitalization_data(spreadsheet):
    spread = spreadsheet.get_worksheet(6)

    regional_ranges ={
        "dolnoslaskie":"B:I",
        "kujawsko_pomorskie":"J:Q",
        "lubelskie":"R:Y",
        "lubuskie":"Z:AG",
        "lodzkie":"AH:AO",
        "malopolskie":"AP:AW",
        "mazowieckie":"AX:BE",
        "opolskie":"BF:BM",
        "podkarpackie":"BN:BU",
        "podlaskie":"BV:CC",
        "pomorskie":"CD:CK",
        "slaskie":"CL:CS",
        "swietokrzyskie":"CT:DA",
        "warminsko_mazurskie":"DB:DI",
        "wielkopolskie":"DJ:DQ",
        "zachodniopomorskie":"DR:DY"
    }
    regional_dfs = {}
    for key in regional_ranges.keys():
        regional_dfs[key] = get_regional_epidemic_data(spread,regional_ranges[key])
    return regional_dfs
def vacc_clean(spread):
    data = spread.get_all_records(head=2)
    df = pd.DataFrame.from_dict(data)
    df = df.iloc[1:,1:]
    df.drop("#",axis=1,inplace=True)
    df.drop("?",axis=1,inplace=True)
    # drop empty columns
    df.dropna(axis=1,how="all",inplace=True)
    # change date to datetime format
    df.index = pd.DatetimeIndex(pd.date_range(pd.to_datetime("2020/12/28",format="%Y/%m/%d"),periods=len(df.index+1)),name="date")

    
    column_renames = {
        df.columns[0]:"sum_vacc",
        df.columns[1]:"daily_vacc",
        df.columns[2]:"people_vacc",
        df.columns[3]:"pct_vacc",
        df.columns[4]:"1_dose",
        df.columns[5]:"2_doses",
        df.columns[6]:"nop_light",
        df.columns[7]:"nop_serious",
        df.columns[8]:"nop_severe",
        df.columns[9]:"nop_death",
        df.columns[10]:"f",
        df.columns[11]:"m",
        df.columns[12]:"doses_delivered",
        df.columns[13]:"doses_to_points",
        df.columns[14]:"utilized",
        df.columns[15]:"used",
    }
    df = df.rename(column_renames,axis=1)
    df.columns = [x for x in df.columns[:23]] + [x for x in region_list]
    df = df.fillna(0)
    # make columns numeric
    for column in df.columns[1:]:
        df[column] = df[column].replace("","0")
        df[column] = make_numeric(df[column])
    
    return df

def get_vaccination_report(sheet):
    spread = sheet.get_worksheet(0)
    df = vacc_clean(spread)
    return df
