import json
import statistics
import time
import urllib.request
from multiprocessing import Pool

with open("payload_performance.json", "r") as json_file:
    json_list = list(json_file)


data_points = []
for json_str in json_list:
    result = json.loads(json_str)
    if not result:
        continue
    body = {"data": [result["text"]]}
    data_points.append(body)

data_points = data_points[:200]


myurl = (
    "http://localhost:8080/invocations"
)  # https://pbgvppvv48.execute-api.us-east-1.amazonaws.com/test/alchemy


def send_request(body):
    req = urllib.request.Request(myurl)
    req.add_header("Content-Type", "application/json")
    jsondata = json.dumps(body)
    jsondataasbytes = jsondata.encode("utf-8")  # needs to be bytes
    req.add_header("Content-Length", len(jsondataasbytes))
    print(jsondataasbytes)

    try:
        start = time.time()
        response = urllib.request.urlopen(req, jsondataasbytes)
        end = time.time()
        return end - start
    except urllib.error.HTTPError as e:
        return 0


p = Pool(processes=2)
intervals = p.map(send_request, data_points)
p.close()

# for i, body in enumerate(data_points):
# 	req = urllib.request.Request(myurl)
# 	req.add_header('Content-Type', 'application/json')
# 	jsondata = json.dumps(body)
# 	jsondataasbytes = jsondata.encode('utf-8')   # needs to be bytes
# 	req.add_header('Content-Length', len(jsondataasbytes))
# 	print (jsondataasbytes)

# 	try:
# 		start = time.time()
# 		response = urllib.request.urlopen(req, jsondataasbytes)
# 		end = time.time()
# 		intervals.append(end - start)
# 		print(response)
# 	except urllib.error.HTTPError as e:
# 		print(e)


avg_time = statistics.mean(intervals)
print("Avg time: " + str(avg_time))
median_time = statistics.median(intervals)
print("median time: " + str(median_time))

print("Min time: " + str(min(intervals)))
print("Max time: " + str(max(intervals)))
