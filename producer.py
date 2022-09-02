from faker import Faker
from kafka import KafkaProducer
import json
import time
fake =Faker()


def get_registered_data():
    return{
        'name': fake.name(),
        'addess': fake.address(),
        'created_at': fake.year()
    }
def json_serializer(data):
    return json.dumps(data).encode('utf-8'),

producer =KafkaProducer(bootstrap_servers=['192.168.1.103'],
                        value_serializer=json_serializer)



if __name__ =="__main__":
    while 1==1:
        registered_data =get_registered_data()
        print(registered_data)
        producer.send('registered_user',registered_data)
        time.sleep(3)