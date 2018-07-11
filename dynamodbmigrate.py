import requests
import json
# from multiprocessing import Pool, Manager
import csv
import threading, time
from threading import Thread
import sys

concurrency = 25

failed = 0
count = 0


lock = threading.Lock()
def getBaseData():
	orderJsonDict = {
		"order_info": {
			"order_no": "apr4_1",
			"order_id": "apr4_1",
			"order_number": "apr4_1",
			"retailer_order_id":"1111111111",
			"retailer_order_no":"1202345678",
			"order_date": "2017-03-27T04:10:16-07:00",
			"order_items": [
				{
					"sku": "1875392",
					"name": "Givenchy Le Rouge - Couture Edition",
					"quantity": "1",
					"fulfillment_status": "SHIPPED",
					"item_promise_date": "2017-03-28T06:50:19-07:00",
					"item_image": "http://www.sephora.com/productimages/sku/s1938448-main-Lhero.jpg",
					"item_url": "http://www.sephora.com/le-rouge-couture-edition-P419505?skuId=1938448&icid2=%20just%20arrived:p419505"
				}
			],
			"billing": {
				"amount": 30,
				"payments": [
					{
						"card": "XXXX-XXXX-XXXX-1234",
						"is_gift_card": True,
						"merchant": "VISA",
						"method": "swipe",
						"expiration_date": "2/18"
					}
				],
				"billed_to": {
					"email": "john.doe@narvar.com",
					"first_name": "john",
					"last_name": "doe",
					"phone": "6501234567",
					"address": {
						"city": "San Francisco",
						"country": "USA",
						"state": "CA",
						"street_1": "123 Market Street",
						"street_2": "string",
						"zip": "94110"
					}
				},
				"tax_amount": 12.5,
				"tax_rate": 9.8,
				"shipping_handling": 4.3
			},
			"shipments": [
				{
					"items_info": [
						{
							"quantity": 1,
							"sku": "1875392"
						}
					],
					"promise_date": "2017-03-28T06:50:19-07:00",
					"carrier": "ups",
					"shipped_to": {
						"address": {
							"zip": "22903"
						}
					},
					"shipped_from": {
						"address": {
							"zip": "21017"
						}
					},

					"ship_date": "2017-03-27T19:50:19-07:00",
					"tracking_number": "apr4_1"
				}
			],
			"customer": {
				"customer_id": "string",
				"address": {
					"city": "San Francisco",
					"country": "USA",
					"state": "CA",
					"street_1": "123 Market Street",
					"street_2": "string",
					"zip": "94110"
				},
				"email": "john.doe@narvar.com",
				"first_name": " john",
				"last_name": " doe",
				"phone": "6501234567",
				"customer_type": "premium"
			}
		}
	}
	return orderJsonDict

def mapRow(order_data):
	order_number = order_data[2]
	awb = order_data[8]
	retailer_moniker = order_data[9]
	partner = order_data[10]
	to_email = order_data[14]
	to_mobile = order_data[15]
	to_pin_code = order_data[18]
	orderJsonDict = getBaseData()
	orderJsonDict["order_info"]["order_no"] = order_number
	orderJsonDict["order_info"]["order_id"] = order_number
	orderJsonDict["order_info"]["order_number"] = order_number
	orderJsonDict["order_info"]["shipments"][0]["tracking_number"] = awb
	orderJsonDict["order_info"]["shipments"][0]["carrier"] = partner
	orderJsonDict["order_info"]["shipments"][0]["shipped_to"]["address"]["zip"] = to_pin_code
	orderJsonDict["order_info"]["billing"]["billed_to"]["email"] = to_email
	orderJsonDict["order_info"]["billing"]["billed_to"]["phone"] = to_mobile
	orderJsonDict["order_info"]["customer"]["email"] = to_email
	orderJsonDict["order_info"]["customer"]["phone"] = to_mobile
	orderJsonDict["order_info"]["upload_id"] = 1
	credential = retailer_moniker
	return orderJsonDict, credential



def makeRequest(args):
	global failed
	rawPayload = ""
	try:
		requestPayload = args.get("requestPayload")
		rawPayload = args.get("rawPayload")
		orderData = requestPayload[0]
		credential = requestPayload[1]
		url = "abc.com"
		headers = {
			'content-type': "application/json",
			'credential': credential,
			'cache-control': "no-cache",
		}
		response = requests.request("POST", url, data = json.dumps(orderData), headers = headers, timeout = 6)
		if (response.ok == False):
			writeFailedToCSV(row)
		with lock:
			if (response.ok == False):
				print "Failed for : "
				print orderData
				failed = failed + 1
	except Exception as e:
		writeFailedToCSV(rawPayload)
		with lock:
			failed = failed + 1


def collectPayloadAndInvokeOrderAPI(fileName):
	batchSize = concurrency * 2
	fileReader = csv.reader(open(fileName), delimiter=",")
	# header = fileReader.next() #header
	# writeFailedToCSV(header)
	batchPayload = []
	global count
	for row in fileReader:
		item = {}
		item['requestPayload'] = mapRow(row)
		item['rawPayload'] = row
		batchPayload.append(item)
		count = count + 1
		if (count % batchSize == 0):
			makeBatchRequest(batchPayload)
			batchPayload = []
			with lock:
				print count, "  Done"
		if count == 100 :
			break


def makeBatchRequest(batchPayload):
	threads = []
	for payload in batchPayload:
		t = Thread(target=makeRequest, args=(payload,))
		threads.append(t)
		t.start()

	for thread in threads:
		thread.join()

	print "over"


def writeFailedToCSV(row):
	with lock:
		with open("failedOrders.csv", 'a') as f:
			wr = csv.writer(f, quoting=csv.QUOTE_ALL)
			wr.writerow(row)





def main():
	fileName = sys.argv[1]
	collectPayloadAndInvokeOrderAPI(fileName + '.csv')
	print "***********Done**********"
	print "Total order = ", count
	print "No of failed orders = ", failed

# if __name__ == "__main__":
main()





