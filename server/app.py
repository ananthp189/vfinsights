
import requests
import pandas as pd
import json
import pyodbc
from flask import Flask
from flask import request
import datetime


#create api   
app = Flask(__name__)
@app.route('/',methods = ["GET","POST"])
def insights():
    #set api calling parameters 
    domain= request.args.get('url') #get domain value from request  
    API_Key="d957b713f04241f39622786961f902df"
    API_URL='https://api.similarweb.com/v1/website/'   
    API_URL2='https://api.similarweb.com/v2/website/'
    #set date params
    x = datetime.datetime.now()- datetime.timedelta(days=30)
    y = datetime.datetime.now() - datetime.timedelta(days=2*365)
    end= x.strftime("%Y"+"-"+"%m")
    start= y.strftime("%Y"+"-"+"%m")
    paramTwoYears=API_Key+"&start_date="+start+"&end_date="+end+"&country=gb&granularity=monthly&main_domain_only=false&format=json"
    param=API_Key+"&start_date="+start+"&end_date="+end+"&country=gb&granularity=monthly&main_domain_only=false&format=json"
    paramLastMonth=API_Key+"&start_date="+end+"&end_date="+end+"&country=gb&granularity=monthly&main_domain_only=false&format=json"
    paramLastMonth2=API_Key+"&start_date="+end+"&end_date="+end+"&country=gb&main_domain_only=false&format=json"
    payload={}
    headers = {}



    # get data for Devices Table


    desktopVisitsURL=API_URL+domain+'/traffic-and-engagement/visits?api_key='+paramLastMonth
    mobileVisitsURL=API_URL2+domain+'/mobile-web/visits?api_key='+paramLastMonth
    deskVisit = requests.request("GET", desktopVisitsURL, headers=headers, data=payload)
    deskVisit= json.loads(deskVisit.text)
    print(deskVisit)
    deskData=deskVisit['visits'][0]
    mobVisit = requests.request("GET", mobileVisitsURL, headers=headers, data=payload)
    mobVisit= json.loads(mobVisit.text)
    mobData=mobVisit['visits'][0]
    totmobtraffic=mobData['visits']
    totdesktraffic=deskData['visits']
    #Demographics Table
    demo_age_desk=API_URL+domain+"/demographics/age?api_key=" + paramLastMonth2
    demo_gender_desk=API_URL+domain+"/demographics/gender?api_key=" + paramLastMonth2
    demo_age_mobile=API_URL+domain+"/demographics-mobile/age?api_key="+ paramLastMonth2
    demo_gender_mobile=API_URL+domain+"/demographics-mobile/gender?api_key="+ paramLastMonth2
    desk_age= requests.request("GET", demo_age_desk, headers=headers, data=payload)
    deskAge= json.loads(desk_age.text)
    mob_age= requests.request("GET", demo_age_mobile, headers=headers, data=payload)
    mobAge= json.loads(mob_age.text)
    desk_gender= requests.request("GET", demo_gender_desk, headers=headers, data=payload)
    deskgender= json.loads(desk_gender.text)
    mob_gender= requests.request("GET", demo_gender_mobile, headers=headers, data=payload)
    mobGender= json.loads(mob_gender.text)


    #Keyword Table

    orgkeyurl =API_URL+domain+"/traffic-sources/organic-search?api_key="+paramLastMonth2
    org_key = requests.request("GET", orgkeyurl, headers=headers, data=payload)
    orgkey=json.loads(org_key.text)
    orgkeydata=orgkey['search']
    paidkeyurl=API_URL+domain+"/traffic-sources/paid-search?api_key="+ paramLastMonth2
    paid_key = requests.request("GET", paidkeyurl, headers=headers, data=payload)
    paidkey=json.loads(paid_key.text)
    try:
        paidkeydata=paidkey['search']
    except KeyError:
        paidkeydata=orgkeydata

    #Traffic Table
    #get top 4 similar sites first

    getsimilarsites=API_URL+domain+"/similar-sites/similarsites?api_key="+API_Key+"&format=json"
    similarsites=requests.request("GET", getsimilarsites, headers=headers, data=payload)
    similarsite=json.loads(similarsites.text)
    competitor=[]
    score=[]


    for i in range(4):
        competitor.append(similarsite['similar_sites'][i]['url'])
        score.append(similarsite['similar_sites'][i]['score'])

    #make calls subsequently to get traffic of the 5 sites

    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
    trafficurl2=API_URL+competitor[0]+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
    trafficurl3=API_URL+competitor[1]+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
    trafficurl4=API_URL+competitor[2]+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
    trafficurl5=API_URL+competitor[3]+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth

    totalvisits=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    totalvisits.append(res1['visits'][0]['visits'])
    response2 = requests.request("GET", trafficurl2, headers=headers, data=payload)
    res2 = json.loads(response2.text)
    totalvisits.append(res2['visits'][0]['visits'])
    response3 = requests.request("GET", trafficurl3, headers=headers, data=payload)
    res3 = json.loads(response3.text)
    totalvisits.append(res3['visits'][0]['visits'])
    response4 = requests.request("GET", trafficurl4, headers=headers, data=payload)
    res4 = json.loads(response4.text)
    totalvisits.append(res4['visits'][0]['visits'])
    response5 = requests.request("GET", trafficurl5, headers=headers, data=payload)
    res5 = json.loads(response5.text)
    totalvisits.append(res5['visits'][0]['visits'])

    #make calls subsequently to get traffic of the 5 sites
    #pages_per_visit

    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
    trafficurl2=API_URL+competitor[0]+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
    trafficurl3=API_URL+competitor[1]+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
    trafficurl4=API_URL+competitor[2]+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
    trafficurl5=API_URL+competitor[3]+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth

    pagespervisit=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    pagespervisit.append(res1['pages_per_visit'][0]['pages_per_visit'])
    response2 = requests.request("GET", trafficurl2, headers=headers, data=payload)
    res2 = json.loads(response2.text)
    pagespervisit.append(res2['pages_per_visit'][0]['pages_per_visit'])
    response3 = requests.request("GET", trafficurl3, headers=headers, data=payload)
    res3 = json.loads(response3.text)
    pagespervisit.append(res3['pages_per_visit'][0]['pages_per_visit'])
    response4 = requests.request("GET", trafficurl4, headers=headers, data=payload)
    res4 = json.loads(response4.text)
    pagespervisit.append(res4['pages_per_visit'][0]['pages_per_visit'])
    response5 = requests.request("GET", trafficurl5, headers=headers, data=payload)
    res5 = json.loads(response5.text)
    pagespervisit.append(res5['pages_per_visit'][0]['pages_per_visit'])


    ##make calls subsequently to get traffic of the 5 sites
    ##averagevisitduration 


    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
    trafficurl2=API_URL+competitor[0]+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
    trafficurl3=API_URL+competitor[1]+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
    trafficurl4=API_URL+competitor[2]+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
    trafficurl5=API_URL+competitor[3]+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth

    averagevisitduration=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    averagevisitduration.append(res1['average_visit_duration'][0]['average_visit_duration'])
    response2 = requests.request("GET", trafficurl2, headers=headers, data=payload)
    res2 = json.loads(response2.text)
    averagevisitduration.append(res2['average_visit_duration'][0]['average_visit_duration'])
    response3 = requests.request("GET", trafficurl3, headers=headers, data=payload)
    res3 = json.loads(response3.text)
    averagevisitduration.append(res3['average_visit_duration'][0]['average_visit_duration'])
    response4 = requests.request("GET", trafficurl4, headers=headers, data=payload)
    res4 = json.loads(response4.text)
    averagevisitduration.append(res4['average_visit_duration'][0]['average_visit_duration'])
    response5 = requests.request("GET", trafficurl5, headers=headers, data=payload)
    res5 = json.loads(response5.text)
    averagevisitduration.append(res5['average_visit_duration'][0]['average_visit_duration'])


    ##make calls subsequently to get traffic of the 5 sites
    ##bounce-rate

    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
    trafficurl2=API_URL+competitor[0]+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
    trafficurl3=API_URL+competitor[1]+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
    trafficurl4=API_URL+competitor[2]+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
    trafficurl5=API_URL+competitor[3]+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth

    bouncerate=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    bouncerate.append(res1['bounce_rate'][0]['bounce_rate'])
    response2 = requests.request("GET", trafficurl2, headers=headers, data=payload)
    res2 = json.loads(response2.text)
    bouncerate.append(res2['bounce_rate'][0]['bounce_rate'])
    response3 = requests.request("GET", trafficurl3, headers=headers, data=payload)
    res3 = json.loads(response3.text)
    bouncerate.append(res3['bounce_rate'][0]['bounce_rate'])
    response4 = requests.request("GET", trafficurl4, headers=headers, data=payload)
    res4 = json.loads(response4.text)
    bouncerate.append(res4['bounce_rate'][0]['bounce_rate'])
    response5 = requests.request("GET", trafficurl5, headers=headers, data=payload)
    res5 = json.loads(response5.text)
    bouncerate.append(res5['bounce_rate'][0]['bounce_rate'])


    ##make calls subsequently to get traffic of the 5 sites
    ##unique

    trafficurl1=API_URL+domain+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
    trafficurl2=API_URL+competitor[0]+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
    trafficurl3=API_URL+competitor[1]+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
    trafficurl4=API_URL+competitor[2]+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
    trafficurl5=API_URL+competitor[3]+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth

    unique=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    unique.append(res1['data'][0]['dedup_data']['total_deduplicated_audience'])
    response2 = requests.request("GET", trafficurl2, headers=headers, data=payload)
    res2 = json.loads(response2.text)
    unique.append(res2['data'][0]['dedup_data']['total_deduplicated_audience'])
    response3 = requests.request("GET", trafficurl3, headers=headers, data=payload)
    res3 = json.loads(response3.text)
    unique.append(res3['data'][0]['dedup_data']['total_deduplicated_audience'])
    response4 = requests.request("GET", trafficurl4, headers=headers, data=payload)
    res4 = json.loads(response4.text)
    unique.append(res4['data'][0]['dedup_data']['total_deduplicated_audience'])
    response5 = requests.request("GET", trafficurl5, headers=headers, data=payload)
    res5 = json.loads(response5.text)
    unique.append(res5['data'][0]['dedup_data']['total_deduplicated_audience'])


    #Channel Table

    channelsurl=API_URL+domain+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    channelsurl1=API_URL+competitor[0]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    channelsurl2=API_URL+competitor[1]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    channelsurl3=API_URL+competitor[2]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    channelsurl4=API_URL+competitor[3]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    response1 = requests.request("get", channelsurl, headers=headers, data=payload)

    res1 = json.loads(response1.text)
    data=res1['visits'][domain][0]['visits']
    df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df1['total']=df1["organic"]+df1["paid"]
    del df1['organic']
    del df1['paid']
    df1['channel']=res1['visits'][domain][0]['source_type']

    data2=res1['visits'][domain][1]['visits']
    df2 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df2['total']=df2["organic"]+df2["paid"]
    del df2['organic']
    del df2['paid']
    df2['channel']=res1['visits'][domain][1]['source_type']

    data=res1['visits'][domain][2]['visits']
    df3 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df3['total']=df3["organic"]+df3["paid"]
    del df3['organic']
    del df3['paid']
    df3['channel']=res1['visits'][domain][2]['source_type']

    data=res1['visits'][domain][3]['visits']
    df4 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df4['total']=df4["organic"]+df4["paid"]
    del df4['organic']
    del df4['paid']
    df4['channel']=res1['visits'][domain][3]['source_type']

    data=res1['visits'][domain][4]['visits']
    df5 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df5['total']=df5["organic"]+df5["paid"]
    del df5['organic']
    del df5['paid']
    df5['channel']=res1['visits'][domain][4]['source_type']


    maindf= pd.concat([df1, df2, df3,df4,df5])
    maindf['Url']=domain
    maindf['Website']=domain
    maindf['order']="1_Main_URL"

    #for comp1
    response1 = requests.request("get", channelsurl1, headers=headers, data=payload)

    res1 = json.loads(response1.text)
    data=res1['visits'][competitor[0]][0]['visits']
    df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df1['total']=df1["organic"]+df1["paid"]
    del df1['organic']
    del df1['paid']
    df1['channel']=res1['visits'][competitor[0]][0]['source_type']

    data2=res1['visits'][competitor[0]][1]['visits']
    df2 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df2['total']=df2["organic"]+df2["paid"]
    del df2['organic']
    del df2['paid']
    df2['channel']=res1['visits'][competitor[0]][1]['source_type']

    data=res1['visits'][competitor[0]][2]['visits']
    df3 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df3['total']=df3["organic"]+df3["paid"]
    del df3['organic']
    del df3['paid']
    df3['channel']=res1['visits'][competitor[0]][2]['source_type']

    data=res1['visits'][competitor[0]][3]['visits']
    df4 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df4['total']=df4["organic"]+df4["paid"]
    del df4['organic']
    del df4['paid']
    df4['channel']=res1['visits'][competitor[0]][3]['source_type']

    data=res1['visits'][competitor[0]][4]['visits']
    df5 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df5['total']=df5["organic"]+df5["paid"]
    del df5['organic']
    del df5['paid']
    df5['channel']=res1['visits'][competitor[0]][4]['source_type']


    comp0df= pd.concat([df1, df2, df3,df4,df5])
    comp0df['Url']=domain
    comp0df['Website']=competitor[0]
    comp0df['order']="2_Comp_Url"


    #for comp1
    response1 = requests.request("get", channelsurl2, headers=headers, data=payload)

    res1 = json.loads(response1.text)
    data=res1['visits'][competitor[1]][0]['visits']
    df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df1['total']=df1["organic"]+df1["paid"]
    del df1['organic']
    del df1['paid']
    df1['channel']=res1['visits'][competitor[1]][0]['source_type']

    data=res1['visits'][competitor[1]][1]['visits']
    df2 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df2['total']=df2["organic"]+df2["paid"]
    del df2['organic']
    del df2['paid']
    df2['channel']=res1['visits'][competitor[1]][1]['source_type']

    data=res1['visits'][competitor[1]][2]['visits']
    df3 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df3['total']=df3["organic"]+df3["paid"]
    del df3['organic']
    del df3['paid']
    df3['channel']=res1['visits'][competitor[1]][2]['source_type']

    data=res1['visits'][competitor[1]][3]['visits']
    df4 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df4['total']=df4["organic"]+df4["paid"]
    del df4['organic']
    del df4['paid']
    df4['channel']=res1['visits'][competitor[1]][3]['source_type']

    data=res1['visits'][competitor[1]][4]['visits']
    df5 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df5['total']=df5["organic"]+df5["paid"]
    del df5['organic']
    del df5['paid']
    df5['channel']=res1['visits'][competitor[1]][4]['source_type']


    comp1df= pd.concat([df1, df2, df3,df4,df5])
    comp1df['Url']=domain
    comp1df['Website']=competitor[1]
    comp1df['order']="3_Comp_Url"

    #for comp2
    response1 = requests.request("get", channelsurl3, headers=headers, data=payload)

    res1 = json.loads(response1.text)
    data=res1['visits'][competitor[2]][0]['visits']
    df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df1['total']=df1["organic"]+df1["paid"]
    del df1['organic']
    del df1['paid']
    df1['channel']=res1['visits'][competitor[2]][0]['source_type']

    data=res1['visits'][competitor[2]][1]['visits']
    df2 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df2['total']=df2["organic"]+df2["paid"]
    del df2['organic']
    del df2['paid']
    df2['channel']=res1['visits'][competitor[2]][1]['source_type']

    data=res1['visits'][competitor[2]][2]['visits']
    df3 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df3['total']=df3["organic"]+df3["paid"]
    del df3['organic']
    del df3['paid']
    df3['channel']=res1['visits'][competitor[2]][2]['source_type']

    data=res1['visits'][competitor[2]][3]['visits']
    df4 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df4['total']=df4["organic"]+df4["paid"]
    del df4['organic']
    del df4['paid']
    df4['channel']=res1['visits'][competitor[2]][3]['source_type']

    data=res1['visits'][competitor[2]][4]['visits']
    df5 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df5['total']=df5["organic"]+df5["paid"]
    del df5['organic']
    del df5['paid']
    df5['channel']=res1['visits'][competitor[2]][4]['source_type']


    comp2df= pd.concat([df1, df2, df3,df4,df5])
    comp2df['Url']=domain
    comp2df['Website']=competitor[2]
    comp2df['order']="4_Comp_Url"

    #for Comp3
    response1 = requests.request("get", channelsurl4, headers=headers, data=payload)

    res1 = json.loads(response1.text)
    data=res1['visits'][competitor[3]][0]['visits']
    df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df1['total']=df1["organic"]+df1["paid"]
    del df1['organic']
    del df1['paid']
    df1['channel']=res1['visits'][competitor[3]][0]['source_type']

    data=res1['visits'][competitor[3]][1]['visits']
    df2 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df2['total']=df2["organic"]+df2["paid"]
    del df2['organic']
    del df2['paid']
    df2['channel']=res1['visits'][competitor[3]][1]['source_type']

    data=res1['visits'][competitor[3]][2]['visits']
    df3 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df3['total']=df3["organic"]+df3["paid"]
    del df3['organic']
    del df3['paid']
    df3['channel']=res1['visits'][competitor[3]][2]['source_type']

    data=res1['visits'][competitor[3]][3]['visits']
    df4 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df4['total']=df4["organic"]+df4["paid"]
    del df4['organic']
    del df4['paid']
    df4['channel']=res1['visits'][competitor[3]][3]['source_type']

    data=res1['visits'][competitor[3]][4]['visits']
    df5 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
    df5['total']=df5["organic"]+df5["paid"]
    del df5['organic']
    del df5['paid']
    df5['channel']=res1['visits'][competitor[3]][4]['source_type']


    comp3df= pd.concat([df1, df2, df3,df4,df5])
    comp3df['Url']=domain
    comp3df['Website']=competitor[3]
    comp3df['order']="5_Comp_Url"


    neworder = ['Url','Website','order','date','channel','total']
    final_df=pd.concat([maindf,comp0df,comp1df,comp2df,comp3df])
    final_df=final_df.reindex(columns=neworder)

    #response2 = requests.request("get", channelsurl1, headers=headers, data=payload)
    #res2 = json.loads(response2.text)
    #unique.append(res2['data'][0]['dedup_data']['total_deduplicated_audience'])
    #response3 = requests.request("get", channelsurl2, headers=headers, data=payload)
    #res3 = json.loads(response3.text)
    #unique.append(res3['data'][0]['dedup_data']['total_deduplicated_audience'])
    #response4 = requests.request("get", channelsurl3, headers=headers, data=payload)
    #res4 = json.loads(response4.text)
    #unique.append(res4['data'][0]['dedup_data']['total_deduplicated_audience'])
    #response5 = requests.request("get", channelsurl4, headers=headers, data=payload)
    #res5 = json.loads(response5.text)
    #unique.append(res5['data'][0]['dedup_data']['total_deduplicated_audience'])



    server = 'vfpoc.database.windows.net'
    database = 'insights'
    username = 'dbadmin'
    password = 'Password123!'   
    driver= '{ODBC Driver 18 for SQL Server}'
    #Truncate tables first.

    truncate_tables="""EXEC truncate_ALL_tables"""
    #Insert queries
    insert_device="""INSERT INTO Device values (?,?,?);"""
    insert_demographic="""INSERT INTO Demographic values (?,?,?,?,?,?,?,?,?,?);"""
    insert_channel="""INSERT INTO Channel values (?,?,?,?,?,?);"""
    insert_traffic="""INSERT INTO Traffic values (?,?,?,?,?,?,?,?);"""
    insert_keyword="""INSERT INTO Keyword_New values (?,?,?,?,?,?);"""
    device=(domain,mobData['visits'],deskData['visits'])
    demo1=(domain,deskAge['age_18_to_24']*totdesktraffic,deskAge['age_25_to_34']*totdesktraffic,deskAge['age_35_to_44']*totdesktraffic,deskAge['age_45_to_54']*totdesktraffic, deskAge['age_55_to_64']*totdesktraffic ,deskAge['age_65_plus']*totdesktraffic,deskgender['male']*totdesktraffic,deskgender['female']*totdesktraffic,'T')
    demo2=(domain,mobAge['age_18_to_24']*totmobtraffic,mobAge['age_25_to_34']*totmobtraffic,mobAge['age_35_to_44']*totmobtraffic,mobAge['age_45_to_54']*totmobtraffic, mobAge['age_55_to_64']*totmobtraffic ,mobAge['age_65_plus']*totmobtraffic,mobGender['male']*totmobtraffic,mobGender['female']*totmobtraffic,'F')

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.fast_executemany = True 
            #cursor.execute(truncate_tables)
            cursor.execute(insert_device,device)   
            cursor.execute(insert_demographic,demo1)
            cursor.execute(insert_demographic,demo2)
            for i in range(10):
                keyword=(i,domain,orgkeydata[i]['search_term'],orgkeydata[i]['share'],paidkeydata[i]['search_term'],paidkeydata[i]['share'])
                cursor.execute(insert_keyword,keyword) 
            traffic0=(domain,domain,'1_Main_Url',totalvisits[0],unique[0],averagevisitduration[0],pagespervisit[0],bouncerate[0])
            cursor.execute(insert_traffic,traffic0)
            for i in range(4):
                traffic=(domain,competitor[i],str(i+2)+'_Comp_Url',totalvisits[i+1],unique[i+1],averagevisitduration[i+1],pagespervisit[i+1],bouncerate[i+1])
                cursor.execute(insert_traffic,traffic) 
            cursor.executemany(insert_channel, final_df.values.tolist())
       
    conn.commit()
    conn.close()
    return "success"
app.run()