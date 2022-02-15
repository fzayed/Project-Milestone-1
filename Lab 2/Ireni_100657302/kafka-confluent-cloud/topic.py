from confluent_kafka.admin import AdminClient, NewTopic

def run():

    try:
        topic_list = []

        admin_client = AdminClient(
            {"bootstrap.servers": "localhost:9093, localhost:9094, localhost:9095"}
        )

        topic_list.append(NewTopic(
                "Users",
                num_partitions = 2,
                replication_factor = 1
            )
        )

        admin_client.create_topics(
            new_topics = topic_list,
            validate_only = False
        )

        print("Done! Created Successfully")

    except Exception as e:

        topic_created = "TopicAlreadyExistsError:"

        if topic_created in str(e):
            print("Topic Is Already Created")

        else: print("Something bad happened " + str(e))

run()
