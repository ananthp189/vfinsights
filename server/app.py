
from asyncio.windows_events import NULL
from cmath import e
from re import search
import requests
import pandas as pd
import json
import pyodbc
from flask import Flask
from flask import request
import datetime


#create api   
app = Flask(__name__)
@app.route('/')
def index():
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
    try:
        deskData=deskVisit['visits'][0]
    except:
        deskData={'visits':0}
    mobVisit = requests.request("GET", mobileVisitsURL, headers=headers, data=payload)
    mobVisit= json.loads(mobVisit.text)
    try:
        mobData=mobVisit['visits'][0]
    except:
        mobData={'visits':0}
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
    try:
        orgkeydata=orgkey['search']
    except:
        orgkeydata={"search":0}
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
        try:
            competitor.append(similarsite['similar_sites'][i]['url'])
            score.append(similarsite['similar_sites'][i]['score'])
        except:
            break
    #make calls subsequently to get traffic of the 5 sites
    trafficurl=[]
    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
    for i in range(len(competitor)):
        turl=API_URL+competitor[i]+"/total-traffic-and-engagement/visits?api_key="+ paramLastMonth
        trafficurl.append(turl)
    

    totalvisits=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        totalvisits.append(res1['visits'][0]['visits'])
    except:
        totalvisits.append(0)

    for i in range(len(competitor)):
        resp = requests.request("GET", trafficurl[i], headers=headers, data=payload)
        res = json.loads(resp.text)
        try:
            totalvisits.append(res['visits'][0]['visits'])
        except:
            totalvisits.append(0)     
   
    for i in range (len(totalvisits ),4):
        totalvisits.append(0)  
    
    #make calls subsequently to get traffic of the 5 sites
    #pages_per_visit
    trafficurlp=[]
    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
    for i in range(len(competitor)):
        turl=API_URL+competitor[i]+"/total-traffic-and-engagement/pages-per-visit?api_key="+ paramLastMonth
        trafficurlp.append(turl)
    

    pagespervisit=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        pagespervisit.append(res1['pages_per_visit'][0]['pages_per_visit'])
    except:
        pagespervisit.append(0)

    for i in range(len(competitor)):
        resp = requests.request("GET", trafficurlp[i], headers=headers, data=payload)
        res = json.loads(resp.text)
        try:
            pagespervisit.append(res['pages_per_visit'][0]['pages_per_visit'])
        except:
            pagespervisit.append(0)     
   
    for i in range (len(pagespervisit ),4):
        pagespervisit.append(0)  
        

    ##make calls subsequently to get traffic of the 5 sites
    ##averagevisitduration 
    trafficurla=[]

    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
    for i in range(len(competitor)):
        turl=API_URL+competitor[i]+"/total-traffic-and-engagement/average-visit-duration?api_key="+ paramLastMonth
        trafficurla.append(turl)
    

    averagevisitduration=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        averagevisitduration.append(res1['average_visit_duration'][0]['average_visit_duration'])
    except:
        averagevisitduration.append(0)

    for i in range(len(competitor)):
        resp = requests.request("GET", trafficurla[i], headers=headers, data=payload)
        res = json.loads(resp.text)
        try:
            averagevisitduration.append(res['average_visit_duration'][0]['average_visit_duration'])
        except:
            averagevisitduration.append(0)     
   
    for i in range (len(averagevisitduration ),4):
        averagevisitduration.append(0)  

    ##make calls subsequently to get traffic of the 5 sites
    ##bounce-rate
    trafficurlb=[]
    trafficurl1=API_URL+domain+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
    for i in range(len(competitor)):
        turl=API_URL+competitor[i]+"/total-traffic-and-engagement/bounce-rate?api_key="+ paramLastMonth
        trafficurlb.append(turl)
    

    bouncerate=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        bouncerate.append(res1['bounce_rate'][0]['bounce_rate'])
    except:
        bouncerate.append(0)

    for i in range(len(competitor)):
        resp = requests.request("GET", trafficurlb[i], headers=headers, data=payload)
        res = json.loads(resp.text)
        try:
            bouncerate.append(res['bounce_rate'][0]['bounce_rate'])
        except:
            bouncerate.append(0)     
   
    for i in range (len(bouncerate ),4):
        bouncerate.append(0)  


    ##make calls subsequently to get traffic of the 5 sites
    ##unique
    trafficurlu=[]
    trafficurl1=API_URL+domain+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
    
    for i in range(len(competitor)):
        turl=API_URL+competitor[i]+"/dedup/deduplicated-audiences?api_key="+ paramLastMonth
        trafficurlu.append(turl)
        
    

    unique=[]

    response1 = requests.request("GET", trafficurl1, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        unique.append(res1['data'][0]['dedup_data']['total_deduplicated_audience'])
    except:
        unique.append(0)

    for i in range(len(competitor)):
        resp = requests.request("GET", trafficurlu[i], headers=headers, data=payload)
        res = json.loads(resp.text)
        try:
            unique.append(res['data'][0]['dedup_data']['total_deduplicated_audience'])
        except:
            unique.append(0)     
   
    for i in range (len(unique ),4):
        unique.append(0)  


    #Channel Table

    channelsurl=API_URL+domain+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    if len(competitor)>0:
        channelsurl1=API_URL+competitor[0]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    if len(competitor)>1:
        channelsurl2=API_URL+competitor[1]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    if len(competitor)>2:
        channelsurl3=API_URL+competitor[2]+"/traffic-sources/overview-share?api_key="+ paramTwoYears
    if len(competitor)>3:
        channelsurl4=API_URL+competitor[3]+"/traffic-sources/overview-share?api_key="+ paramTwoYears

    response1 = requests.request("get", channelsurl, headers=headers, data=payload)
    res1 = json.loads(response1.text)
    try:
        data=res1['visits'][domain][0]['visits']    
        df1 = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
        df1['total']=df1["organic"]+df1["paid"]
        del df1['organic']
        del df1['paid']
        df1['channel']=res1['visits'][domain][0]['source_type']

        data=res1['visits'][domain][1]['visits']
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
    except:
        maindf=pd.DataFrame()
        maindf['total']=0
        maindf['date']=x.strftime("%Y"+"-"+"%m"+"-01")
        maindf['Url']=domain
        maindf['Website']=domain
        maindf['channel']=""
        maindf['order']="1_Main_URL"

    maindf= pd.concat([df1, df2, df3,df4,df5])
    maindf['Url']=domain
    maindf['Website']=domain
    maindf['order']="1_Main_URL"

    

    #for comp1
    if len(competitor)>0:
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
    else:
        comp0df=pd.DataFrame()
        comp0df['Url']=domain
        comp0df['Website']=""
        comp0df['cahnel']=""
        comp0df['order']="2_Comp_Url"
        comp0df['total']=0
        comp0df['date']=x.strftime("%Y"+"-"+"%m"+"-01")

    #for comp1
    if len(competitor)>1:
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
    else:
        comp1df=pd.DataFrame()
        comp1df['Url']=domain
        comp1df['Website']=""
        comp1df['channel']=""
        comp1df['order']="2_Comp_Url"
        comp1df['total']=0
        comp1df['date']=x.strftime("%Y"+"-"+"%m"+"-01")
    #for comp2
    if len(competitor)>2:
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
    else:
        comp2df=pd.DataFrame()
        comp2df['Url']=domain
        comp2df['Website']=""
        comp2df['channel']=""
        comp2df['order']="2_Comp_Url"
        comp2df['total']=0
        comp2df['date']=x.strftime("%Y"+"-"+"%m"+"-01")
    #for Comp3
    if len(competitor)>3:
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
    else:
        comp3df=pd.DataFrame()
        comp3df['Url']=domain
        comp3df['Website']=""
        comp3df['channel']=""
        comp3df['order']="2_Comp_Url"
        comp3df['total']=0
        comp3df['date']=x.strftime("%Y"+"-"+"%m"+"-01")


    neworder = ['Url','Website','order','date','channel','total']

    final_df=pd.concat([maindf,comp0df,comp1df,comp2df,comp3df])
    final_df=final_df.reindex(columns=neworder)

    

    url_kw="https://api.similarweb.com/v1/website/"+domain+"/traffic-sources/organic-search?api_key="+paramTwoYears
    keyword=requests.request("GET", url_kw, headers=headers, data=payload)
    keyword= json.loads(keyword.text)
    try:
        keyword=(keyword["search"][0]["search_term"])
    except:
        keyword=""
    url="https://api.similarweb.com/v2/keywords/"+keyword+"/analysis/organic-competitors?api_key="+paramTwoYears+"&metrics=traffic,organic-vs-paid,volume,cpc"
    kw = requests.request("GET", url, headers=headers, data=payload)
    kw= json.loads(kw.text)



    server = 'vfpoc.database.windows.net'
    database = 'insights'
    username = 'dbadmin'
    password = 'Password123!'   
    driver= '{ODBC Driver 18 for SQL Server}'
    #Truncate tables first.

    truncate_tables="""EXEC truncate_ALL_tables"""
    get_url="select distinct Url from traffic"
    #Insert queries
    insert_device="""INSERT INTO Device values (?,?,?);"""
    insert_demographic="""INSERT INTO Demographic values (?,?,?,?,?,?,?,?,?,?);"""
    insert_channel="""INSERT INTO Channel values (?,?,?,?,?,?);"""
    insert_traffic="""INSERT INTO Traffic values (?,?,?,?,?,?,?,?);"""
    insert_keyword="""INSERT INTO Keyword_New values (?,?,?,?,?,?);"""
    insert_KW_analysis="""INSERT INTO KW_Analysis values (?,?,?,?,?);"""
    device=(domain,mobData['visits'],deskData['visits'])
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            df=pd.read_sql(get_url,conn)
            if not df['Url'].str.contains(domain).any():
                try:
                    demo1=(domain,deskAge['age_18_to_24']*totdesktraffic,deskAge['age_25_to_34']*totdesktraffic,deskAge['age_35_to_44']*totdesktraffic,deskAge['age_45_to_54']*totdesktraffic, deskAge['age_55_to_64']*totdesktraffic ,deskAge['age_65_plus']*totdesktraffic,deskgender['male']*totdesktraffic,deskgender['female']*totdesktraffic,'T')
                    demo2=(domain,mobAge['age_18_to_24']*totmobtraffic,mobAge['age_25_to_34']*totmobtraffic,mobAge['age_35_to_44']*totmobtraffic,mobAge['age_45_to_54']*totmobtraffic, mobAge['age_55_to_64']*totmobtraffic ,mobAge['age_65_plus']*totmobtraffic,mobGender['male']*totmobtraffic,mobGender['female']*totmobtraffic,'F')
                except:
                    demo1=(domain,0,0,0,0,0,0,0,0,'T')
                    demo2=(domain,0,0,0,0,0,0,0,0,'F')
    
                cursor.fast_executemany = True 
                for i in range(10):  
                    try:
                        KW_comp=(domain,keyword,kw["traffic_breakdown"][i]["domain"],kw["traffic_breakdown"][i]["traffic_share"],kw["traffic_breakdown"][i]["position"])
                        cursor.execute(insert_KW_analysis,KW_comp)
                    except:
                        break
                #cursor.execute(truncate_tables)
                cursor.execute(insert_device,device)   
                cursor.execute(insert_demographic,demo1)
                cursor.execute(insert_demographic,demo2)
                for i in range(10):
                    try:
                        keyword=(i,domain,orgkeydata[i]['search_term'],orgkeydata[i]['share'],paidkeydata[i]['search_term'],paidkeydata[i]['share'])
                        cursor.execute(insert_keyword,keyword) 
                    except:
                        break
                traffic0=(domain,domain,'1_Main_Url',totalvisits[0],unique[0],averagevisitduration[0],pagespervisit[0],bouncerate[0])            
                cursor.execute(insert_traffic,traffic0)
            
                cursor.executemany(insert_channel, final_df.values.tolist())
                for i in range(4):
                    try:
                        traffic=(domain,competitor[i],str(i+2)+'_Comp_Url',totalvisits[i+1],unique[i+1],averagevisitduration[i+1],pagespervisit[i+1],bouncerate[i+1])
                        cursor.execute(insert_traffic,traffic) 
                    except:
                        break
                        

                    
                               
       
       
    conn.commit()
    conn.close()
    return "success"
if __name__ == '__main__':
    app.run()
