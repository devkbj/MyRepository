'''
pip install azure-eventhub

SELECT SensorID
	, System.TimeStamp() AS Time
	, AVG(Temperature) AS AvgTemperature
	, MIN(Temperature) AS MinTemperature
	, MAX(Temperature) AS MaxTemperature
INTO SensorOutput
FROM SensorInput
GROUP BY SensorID, TumblingWindow(second, 60)

'''

import numpy as np
import datetime
import time
import json

from azure.eventhub import EventHubProducerClient, EventData

connection_str = 'Endpoint=sb://kbj8566.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=nL2yNmadP46wsPSGT52h7f0eqntEePalw+AEhEu2rpc='
eventhub_name = 'myhub'
sensor_num = 10

'''
{'SensorID': 1, 'Temperature': 24, 'Time': '2022-12-02 10:10:17', 'Status': 'On'}
'''
def get_msg(id):
    msg = dict()
    msg['SensorID'] = id
    msg['Temperature'] = int(np.random.normal(24, 3, 1)[0])
    msg['Time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg['Status'] = 'On'

    return msg


client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
with client:
    for _ in range(10000):
        event_data_batch = client.create_batch()

        for id in range(sensor_num):
            msg = get_msg(id)
            event_data_batch.add(EventData(json.dumps(msg)))

        print(event_data_batch)
        client.send_batch(event_data_batch)
        time.sleep(1)


