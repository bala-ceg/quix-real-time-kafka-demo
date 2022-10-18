from quixstreaming import QuixStreamingClient
import pandas as pd
import os
import requests


url = "https://disease.sh/v3/covid-19/historical/all?lastdays=all"
source = requests.get(url).json()
df = pd.DataFrame.from_records(source)
df.reset_index(level = 0, inplace = True)

# Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
client = QuixStreamingClient()

# Open the output topic and create the stream
print("Opening output topic")
output_topic = client.open_output_topic(os.environ["output"])
output_stream = output_topic.create_stream()


headers = list(df)
for index, row in df.iterrows():

    data_frame_row = pd.DataFrame()

    for header in headers:
        value = row[header]
        data_frame_row[header] = [value]

    output_stream.parameters.buffer.write(data_frame_row)

output_stream.close()
