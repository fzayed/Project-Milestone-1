from kafka.admin import KafkaAdminClient, NewTopic
import sys

try: 
    admin_client = KafkaAdminClient( 
        client_id='myapp',
        bootstrap_servers = [ 
            "localhost:9093",
            "localhost:9094",
            "localhost:9095"
        ]
    )

    topic_list = []
    topic_list.append(NewTopic(name="topic", num_partitions=2, replication_factor=2))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topic has been created!")

except Exception as e:
    print("Topic could not be created: ", e.__class__)

finally:
    sys.exit()