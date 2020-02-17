import os
import io
import requests
import time
import sys
import pandas as pd
from datetime import timedelta
from flask import session, app
from flask import Flask, jsonify, render_template, request, send_file
from flask_apscheduler import APScheduler
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_
from sqlalchemy import Column, String, Integer, REAL, DateTime 
from sqlalchemy.orm import sessionmaker

import googlemaps

GOOGLEMAP_KEY = ""
POSTGRESQL_DB_URI = ""
#gmaps = googlemaps.Client(key=GOOGLEMAP_KEY)

class Config(object):
    SQLALCHEMY_DATABASE_URI = POSTGRESQL_DB_URI
    JOBS = [
        {
            'id': 'update_data',
            'func': 'app:update_data',
            'args': '',
            'trigger': 'interval',
            'seconds': 1800
        }
    ]

    SCHEDULER_API_ENABLED = True

app = Flask(__name__)
app.config.from_object(Config())
db = SQLAlchemy(app)

class MaskData(db.Model):  
    __tablename__ = 'MaskData'
    __table_args__ = {'extend_existing': True}
    code = Column(String, primary_key=True)
    name = Column(String)
    location = Column(String)
    county = Column(String)
    township = Column(String)
    tel = Column(String)
    adult_remaining = Column(Integer)
    child_remaining = Column(Integer)
    updated_time = Column(DateTime)
    latitude = Column(REAL)
    longitude = Column(REAL)
    
    def to_json(self):
        return {
                'code': self.code,
                'name': self.name,
                'location': self.location,
                'county': self.county,
                'township': self.township,
                'tel': self.tel,
                'adult_remaining': self.adult_remaining,
                'child_remaining' : self.child_remaining,
                'updated_time': self.updated_time.isoformat(),
                'latitude': self.latitude,
                'longitude': self.longitude,
                }

    def to_json_for_location(self):
        return {
                'code': self.code,
                'name': self.name,
                'location': self.location,
                'tel': self.tel,
                'adult_remaining': self.adult_remaining,
                'child_remaining' : self.child_remaining,
                'updated_time': self.updated_time.isoformat(),
                'latitude': self.latitude,
                'longitude': self.longitude,
                }

class CountyMaskData(db.Model):  
    __tablename__ = 'CountyMask'
    __table_args__ = {'extend_existing': True}
    county = Column(String, primary_key=True)
    adult_remaining = Column(Integer)
    child_remaining = Column(Integer)
    updated_time = Column(DateTime)
    
    def to_json(self):
        return {
                'county': self.county,
                'adult_remaining': self.adult_remaining,
                'child_remaining' : self.child_remaining,
                'updated_time': self.updated_time.isoformat(),
                }


@app.route('/')
def home(name=None):
    return render_template('index.html', name=name)

@app.route('/get/<location>', methods=["GET"])
def get_test(location):
    results = db.session.query(MaskData).filter(or_(MaskData.name.like('%{}%'.format(location)), MaskData.location.like('%{}%'.format(location)))).all()
    json_dict = {"results": []}
    for r in results:
        json_dict["results"].append(r.to_json_for_location())
    return jsonify(json_dict)

@app.route('/get-all/county-remaining', methods=["GET"])
def get_county_remaining():
    results = db.session.query(CountyMaskData).all()
    json_dict = {}
    for r in results:
        json_dict[r.county] = r.to_json()
    return jsonify(json_dict)

# def get_lat_lng(location):
#     geocode_result = gmaps.geocode(location)
#     if len(geocode_result) > 0:
#         lat = geocode_result[0]['geometry']['location']['lat']
#         lng = geocode_result[0]['geometry']['location']['lng']
#     else:
#         lat = None
#         lng = None
#     return (lat, lng)

def update_data():
    print("Start update")
    sys.stdout.flush()
    url = "http://data.nhi.gov.tw/Datasets/Download.ashx?rid=A21030000I-D50001-001&l=https://data.nhi.gov.tw/resource/mask/maskdata.csv"
    s = requests.get(url).content
    csv = pd.read_csv(io.StringIO(s.decode('utf-8')))
    csv["醫事機構地址"] = csv["醫事機構地址"].str.replace("巿", "市")
    csv["醫事機構地址"] = csv["醫事機構地址"].str.replace("台北", "臺北")
    csv["醫事機構地址"] = csv["醫事機構地址"].str.replace("台南", "臺南")
    csv["醫事機構地址"] = csv["醫事機構地址"].str.replace("台中", "臺中")
    csv["醫事機構地址"] = csv["醫事機構地址"].str.replace("台東", "臺東")
    csv.loc[csv["醫事機構代碼"] == "5921012281", "醫事機構地址"] = "臺南市東區富強里中華東路二段４９號"
    csv.loc[csv["醫事機構代碼"] == "5931101919", "醫事機構地址"] = "新北市淡水區新市一路３段１０３號"
    csv.loc[csv["醫事機構代碼"] == "5946010256", "醫事機構地址"] = "臺東縣臺東市新生路４８９號"
    split_county = csv["醫事機構地址"].str.split('縣|市', n=1, expand=True)
    csv["County"] = split_county[0]
    csv["Township"] = split_county[1].str.split('鄉|鎮|市|區', n=1, expand=True)[0]
    grouped = csv.groupby("County")
    adult_mask_remaining = grouped['成人口罩剩餘數'].agg('sum')
    child_mask_remaining = grouped['兒童口罩剩餘數'].agg('sum')
    last_updated_time = csv["來源資料時間"][0]

    # Update county's data
    county_data_list = []
    for i in adult_mask_remaining.index:
        data = CountyMaskData(county=i,
                            adult_remaining=int(adult_mask_remaining[i]),
                            child_remaining=int(child_mask_remaining[i]),
                            updated_time=last_updated_time,
                            )
        county_data_list.append(data)
    
    for i in county_data_list:
        result = db.session.query(CountyMaskData).filter(CountyMaskData.county == i.county).first()
        if result == None:
            db.session.add(i)
        else:
            result.adult_remaining = i.adult_remaining
            result.child_remaining = i.child_remaining
            result.updated_time = i.updated_time
        db.session.flush()
    db.session.commit()

    # Update each store's data
    data_list = []
    for index, row in csv.iterrows():
        data = MaskData(code=row["醫事機構代碼"],
                    name=row["醫事機構名稱"],
                    location=row["醫事機構地址"],
                    tel=row["醫事機構電話"],
                    adult_remaining=row["成人口罩剩餘數"],
                    child_remaining=row["兒童口罩剩餘數"],
                    updated_time=row["來源資料時間"],
                    county=row["County"],
                    township=row["Township"])
        data_list.append(data)
    
    start_time = time.time()
    for i in data_list:
        result = db.session.query(MaskData).filter(MaskData.code == i.code).first()
        if result == None:
            # lat, lng = get_lat_lng(i.location)
            # i.latitude = lat
            # i.longitude = lng
            db.session.add(i)
        else:
            result.adult_remaining = i.adult_remaining
            result.child_remaining = i.child_remaining
            result.updated_time = i.updated_time
            # if result.latitude == None:
            #     lat, lng = get_lat_lng(result.location)
            #     result.latitude = lat
            #     result.longitude = lng
        db.session.flush()
    db.session.commit()
    print("#########################Finish update test#################")
    print((time.time() - start_time) / 60)
    sys.stdout.flush()
    return time.time() - start_time


if __name__ == '__main__':
    scheduler = APScheduler()
    # it is also possible to enable the API directly
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()

    app.run(debug=True)