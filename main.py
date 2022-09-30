import mysql.connector
import json
import pymongo
from pymongo import MongoClient


cnx = mysql.connector.connect(user='root', password='rohith2409',
                              host='localhost',port=3306,
                              database='DB2')
cursor = cnx.cursor()
client = MongoClient('mongodb://localhost:27017/') #localhost and default port
mongodb = client.db44
artist_collection = mongodb.artist
artwork_collection= mongodb.artwork

print("Getting required data from MySQL")
print("--------------------------------")
query =("select A.aID , A.name , A.BirthDate , S.StateName ,Aw.title , Aw.price ,Aw.form from DB2.Artist A join DB2.State S on A.StateAb = S.stateAb join DB2.Artwork Aw on A.aId = Aw.aId order BY A.aId;")
cursor.execute(query)
result = cursor.fetchall()
current_project= None
idno = 101
jsondoc ={"artistdata": []}
#convert into JSON
for obj in result:
    if current_project == None:
        idno += 1
        current_project = obj[0]
        jsonobj = {"_id": idno ,"aID" : obj[0], "name" : obj[1], "BirthDate" : obj[2],"StateName":obj[3]}
        jsonobj["artwork"] = [{"title" : obj[4],"price": obj[5], "form": obj[6]}]
    elif current_project == obj[0]:  
        jsonobj["artwork"].append({"title" : obj[4],"price": obj[5], "form": obj[6]})
    else:
        jsondoc["artistdata"].append(jsonobj)
        artist_collection.insert_one(jsonobj)
        idno += 1
        current_project = obj[0]
        jsonobj = {"_id": idno ,"aID" : obj[0], "name" : obj[1], "BirthDate" : obj[2],"StateName":obj[3]}
        jsonobj["artwork"] = [{"title" : obj[4],"price": obj[5], "form": obj[6]}]
    if(obj == result[-1]):
        #insert in project collection in mongoDB
        jsondoc["artistdata"].append(jsonobj)
        artist_collection.insert_one(jsonobj)
print(jsondoc)
artworkquery = ("select Aw.title,Aw.creationDate,Aw.price,Aw.form,A.Name,B.saleDate ,C.name,C.city,S.statename from DB2.Artist A join DB2.Artwork Aw on  A.aId=Aw.aId left outer join DB2.Bought B on Aw.artID=B.artID left outer join DB2.Customer C  on C.cID=B.cID left outer join DB2.State S on C.stateAb=S.stateAb;")
cursor.execute(artworkquery)
aw_result = cursor.fetchall()
artwork= None
idnum = 1
jsondoc_artwork = {"artworkdata": []}
#convert into JSON
for obj in aw_result:
    if artwork == None:
        idnum += 1
        artwork = obj[0]
        jsonobj = {"_id": idnum,"title" : obj[0], "creationdate" : obj[1],"price" : obj[2],"form" : obj[3],"artistname" : obj[4],"saledate" : obj[5]}
        jsonobj["customers"] = [{"customername" : obj[6],"city": obj[7], "state": obj[8]}]
    elif artwork == obj[0]:  
        jsonobj["customers"].append(obj[2])
    else:
        jsondoc_artwork["artworkdata"].append(jsonobj)
        artwork_collection.insert_one(jsonobj)
        idnum += 1
        artwork = obj[0]
        jsonobj = {"_id": idnum,"title" : obj[0], "creationdate" : obj[1],"price" : obj[2],"form" : obj[3],"artistname" : obj[4],"saledate" : obj[5]}
        jsonobj["customers"] = [{"customername" : obj[6],"city": obj[7], "state": obj[8]}]
    if(obj == aw_result[-1]):
        #insert in department collection in mongoDB
        jsondoc_artwork["artworkdata"].append(jsonobj)
        artwork_collection.insert_one(jsonobj)
print(jsondoc_artwork)