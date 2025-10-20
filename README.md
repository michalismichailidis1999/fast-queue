> [!CAUTION]
> Not production ready yet, still under development

# Introduction
Distributed message broker written in C++

## This project includes the folloiwng functionalities
1. Custom application protocol for both external & internal communication
2. Raft consensus for cluster metadata updates replication to multiple follower nodes
3. Log based storage (appending messages to end of a file)
4. Custom B-Tree indexing for faster message retrieval by consumers
5. Asynchronous message replication
6. Connection pooling + handling of idle connections

# How to use
To try this project, a python client has been created (you can find it on this [link](https://github.com/michalismichailidis1999/fast-queue-python-library)).
After cloning the python client, you can start the c++ server by running the `compose.yaml` file (or start the server in debug mode if you are working on Visual Studio in Windows).

After the server started successfully you can run the `test_produer_app.py` or `test_consumer_app.py` from the python client to test the message broker.

> [!NOTE]
> Update the .conf file to your preferences (see temp.conf)
