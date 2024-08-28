Readme.txt

There are couple of approachesd to sovle the above problem.

Approach 1:

We can use bulk api that is provided by opensearch to read multiple events from kafka and push data into opensearch in batches.
Have attaches sample test.json which has sample format for bulk insert.

Approach 2:

We can use pyspark to read events from kafka and push events to output stream.

Apprach 3:

Read events from kafka
Create threadpool and invoke curl to opensearch.


Currently have solved the above using third approach...


Run the above:

Installation requrirement:
pip3 install reqirements.txt

To ingest data into kafka topic:
python3 readcdcstream.py

To read from kafka topic and ingest data into opensearch:
python3 cdcstream.py