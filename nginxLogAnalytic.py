#!/usr/bin/python
#coding:utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from influxdb import InfluxDBClient
import json
import os
import datetime
import time
from collections import defaultdict


import logging
import logging.handlers
import pdb
import urllib

LOG_FILE = 'nginxacesslog.log'

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024*100, backupCount = 5) # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

formatter = logging.Formatter(fmt)   # 实例化formatter
handler.setFormatter(formatter)      # 为handler添加formatter

logger = logging.getLogger('tst')    # 获取名为tst的logger
logger.addHandler(handler)           # 为logger添加handler
logger.setLevel(logging.DEBUG)


# get goaccess result on ip
def getNginxLogJson(ip):
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M%SZ")
    reportfile="/tmp/" + now + ip + ".json"
    cmd = """ssh root@%s 'cd /data/wanglei;
    ./nginxLogAccess.py /data/ucs-openresty/logs/ucs-api-gateway-upstream.log.1 report.json'
    scp root@%s:/data/wanglei/report.json %s
    """%(ip, ip, reportfile)
    logger.debug(cmd)
    val = os.system(cmd)
    if val != 0:
        logger.error( now + " getNginxLogJson Fail " + str(val))
        return None
    f = open(reportfile)
    jsonres = json.load(f)
    logger.debug(now)
    logger.debug( jsonres)
    return jsonres

def sendpoint(config):
    reslist = []
    # {"imagename": {"cost": "total" , "success"}}
    totalMap = defaultdict(lambda: defaultdict(int))
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    for item in config["nginx"]:
        res = getNginxLogJson(item["ip"])
        if res == None:
            continue
        # 1. add point {host:ip, image:k}
        for k, v in res.items():
#            pdb.set_trace()
            bucketNameArr = []
	    print "origin image name: " + k
	    logger.debug( "origin image name: " + str(k))

      	    k = urllib.unquote(k)
            bucketNameArr = k.split("/")
            print "decode and split: " + k, bucketNameArr
	    bucketNameArrLen = len(bucketNameArr)
	    logger.debug( "decode and split, k: " + str(k) + ",bucketNameArr:" + str(bucketNameArr) + ", bucketNameArr length:" + str(bucketNameArrLen) )
	    if	bucketNameArrLen < 3:
		logger.warn("bucketName err, imageName:" + str(k) + ", bucketNameArr:" + str(bucketNameArr))
		continue
            bucketName = bucketNameArr[1]
            print "bucketName: " + bucketName
	    logger.debug( "bucketName: " + str(bucketName))

            reslist.append({
                "measurement": "nginx_request",
                "tags": {
                    "host": item["ip"],
                    "region": item["set"],
                    "image": k,
                    "bucket": bucketName
                },
                "fields": {
                    "cost": v["cost"],
                    "success": float(v["success"]) / v["total"],
                    "total": v["total"],
                    "successNum": v["success"]
                }
            })
            totalMap[k]["total"] += v["total"]
            totalMap["Allimage"]["total"] += v["total"]
            totalMap[k]["success"] += v["success"]
            totalMap["Allimage"]["success"] += v["success"]
            totalMap[k]["totalcost"] += v["cost"] * v["total"]
            totalMap["Allimage"]["totalcost"] += v["cost"] * v["total"]
    for k, v in totalMap.items():
        # 2. add point {host:all, image:k}
        # 3. 3. add point {host:all, image:all}
        print "Allhost--------------->"
        if "Allimage" == k:
            bucketName = k
        else:
            bucketNameArr = []
	    print "origin image name: " + k
      	    k = urllib.unquote(k)
            bucketNameArr = k.split("/")
            print "decode and split: " + k, bucketNameArr
            bucketName = bucketNameArr[1]

        print "bucketName: " + bucketName
        reslist.append({
            "measurement": "nginx_request",
            "tags": {
                "host": "Allhost",
                "region": config["region"],
                "image": k,
                "bucket": bucketName
            },
            "fields": {
                "cost": int(v["totalcost"] / v["total"]),
                "success": float(v["success"]) / v["total"],
                "total": v["total"],
                "successNum": v["success"]
            }
        })

    logger.debug(reslist)
    client = InfluxDBClient(config['influxdbAddr'], config['influxdbPort'], config['username'], config['userpasswd'], config['influxdbName'])
    ret = client.write_points(reslist)
    logger.debug( "write_points " + str(ret))


if __name__ == '__main__':
    config = json.load(open("./config.json"))
    try:
        sendpoint(config)
    except Exception,e:
        logger.exception( "sendpoint fail " )
