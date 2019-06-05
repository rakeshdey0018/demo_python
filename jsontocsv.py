
import json
from pandas.io.json import json_normalize
import pandas as pd
import collections
import csv
import boto3
from io import StringIO



def flattenjson(b,delim):
    v={}
    for i in collections.OrderedDict(b).keys():
        fname=i.replace("fields","")
        if isinstance(b[i],dict):
            get=flattenjson(b[i],delim)
            for j in get.keys():
                if fname+delim==delim:
                    v[j]=get[j]
                else:
                    v[fname+delim+j]=get[j]
        elif isinstance(b[i],list):
            for l in range(len(b[i])):

                if isinstance(b[i][l],dict):
                    get1=flattenjson(b[i][l],delim)

                    for t in get1.keys():
                        v[fname+delim+t]=get1[t]
        else:
            v[fname]=b[i]
    return collections.OrderedDict(v)

df=pd.DataFrame()
bucket = event['Records'][0]['s3']['bucket']['name']
key = event['Records'][0]['s3']['object']['key']

s3 = boto3.client('s3')

content_object = s3.get_object(Bucket=bucket, Key=key)
file_content = content_object['Body'].read().decode('utf-8')
json_content = json.loads(file_content)
print(json_content)


t=flattenjson(json_content,"_")

r=json_normalize(t)
df=df.append(r.iloc[0],ignore_index=True)
print(df.head)
csv_buffer = StringIO()

    
df.to_csv(csv_buffer,index=False)
s3.put_object(Bucket=bucket,Key="export.csv",Body=csv_buffer.getvalue())
   

