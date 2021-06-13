#!/usr/bin/env python

import pika, sys, time, json, threading, datetime
import google.cloud.compute_v1 as compute_v1

# Server control loop:
#  while True:
#    - get number of messages on queue
#    - get number of nodes in fleet for this job (w. the right label)
#    - if number of nodes < number of messages on queue, scale up to min(n_messages, 30)
#      - launch node with the given cluster label, metadata for startup script
#    - if node is disconnected or jobless from rmq for longer than X minutes, delete it
#      - https://stackoverflow.com/questions/13037121/in-pika-or-rabbitmq-how-do-i-check-if-any-consumers-are-currently-consuming

max_nodes = 30

def delete_nodes(nodes):
    # delete a list of nodes
    zone = 'us-west1-a'
    project_id = 'moeyens-thor-dev'

    print(f"Deleting {nodes} from {zone}...")

    instance_client = compute_v1.InstancesClient()
    ops = []
    for node in nodes:
        operation = instance_client.delete(project=project_id, zone=zone, instance=node)
        ops.append(operation)
        if operation.error:
            print("Error during deletion:", operation.error, file=sys.stderr)
        if operation.warnings:
            print("Warning during deletion:", operation.warnings, file=sys.stderr)

    # wait for completion of all deletions
    operation_client = compute_v1.ZoneOperationsClient()
    for operation in ops:
        operation = operation_client.wait(operation=operation.name, zone=zone, project=project_id)
        if operation.error:
            print("Error during deletion:", operation.error, file=sys.stderr)
        if operation.warnings:
            print("Warning during deletion:", operation.warnings, file=sys.stderr)

    print(f"Instances {nodes} deleted.")

def get_nodes(cluster):
    # Get all nodes for this cluster
    instance_client = compute_v1.InstancesClient()
    request = compute_v1.ListInstancesRequest(filter=f"(labels.macgyver-name = {cluster}) AND (status = running)", project='moeyens-thor-dev', zone='us-west1-a')
    instance_list = instance_client.list(request=request)
    return list(instance_list)

def create_nodes(cluster, count, nodes=None):
    # launch a set of nodes
    zone = 'us-west1-a'
    machine_type = 'e2-standard-2'
    name_pattern = 'test-instance-######'
    network_name = 'global/networks/default'
    project_id = 'moeyens-thor-dev'

    # Launch count new nodes
    print(f"Launching {count} nodes")

    instance_client = compute_v1.InstancesClient()

    # Every machine requires at least one persistent disk
    disk = compute_v1.AttachedDisk()
    initialize_params = compute_v1.AttachedDiskInitializeParams()
    initialize_params.source_image = "projects/moeyens-thor-dev/global/images/ray-thin"
    initialize_params.disk_size_gb = "10"
    disk.initialize_params = initialize_params
    disk.auto_delete = True
    disk.boot = True
    disk.type_ = compute_v1.AttachedDisk.Type.PERSISTENT

    # Every machine needs to be connected to a VPC network.
    # The 'default' network is created automatically in every project.
    network_interface = compute_v1.NetworkInterface()
    network_interface.name = network_name

    # Scheduling
    scheduling = compute_v1.Scheduling()
    scheduling.preemptible = True

    # Collecting all the information into the Instance object
    instance = compute_v1.InstanceProperties()
    instance.scheduling = scheduling
    instance.disks = [disk]
    instance.machine_type = machine_type
    instance.network_interfaces = [network_interface]
    instance.labels = { 'macgyver-name': cluster }

#    # Create predefined names
#    if nodes is None:
#        nodes = get_nodes(cluster)
#    prefix = name_pattern.split('#')[0]
#    print(f"{prefix=}")
#    max_idx = 1
#    for node in nodes:
#        idx = int(node.name[len(prefix)+1:])
#        max_idx = max(max_idx, idx)
#    fmt = prefix + "{:0" + str(len(name_pattern) - len(prefix)) + "d}"
#    print(fmt)
#    pip = {}
#    for k in range(count):
#        name = fmt.format(max_idx + k + 1)
#        print(name)
#        pip[name] = {}

    # collect all these into a BulkInsertInstanceResource, and specify how many
    # instances to create
    bi = compute_v1.BulkInsertInstanceResource()
    bi.count = str(count)
    bi.instance_properties = instance
    bi.name_pattern = name_pattern
#    bi.per_instance_properties = pip

    # Preparing the InsertInstanceRequest
    request = compute_v1.BulkInsertInstanceRequest()
    request.zone = zone
    request.project = project_id
    request.bulk_insert_instance_resource_resource = bi

    print(f"Creating {count} instances in {zone}...")
#    print(request)

    operation = instance_client.bulk_insert(request=request)
    if operation.status == compute_v1.Operation.Status.RUNNING:
        operation_client = compute_v1.ZoneOperationsClient()
        operation = operation_client.wait(
            operation=operation.name, zone=zone, project=project_id
        )
    if operation.error:
        print("Error during creation:", operation.error, file=sys.stderr)
    if operation.warnings:
        print("Warning during creation:", operation.warnings, file=sys.stderr)
    print(f"Bulk create of {count} instances succeeded.")

def autoscaler_monitor(cluster):
    con = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    try:
        channel = con.channel()

        while True:
            now = time.time()

            #
            # Gather information about the cluster and the queue
            #
            nodes = get_nodes(cluster)
            nnodes = len(nodes)

            queue = channel.queue_declare(queue='tasks', durable=True, exclusive=False, auto_delete=False)
            ntasks = queue.method.message_count

            nplus = min(ntasks - nnodes, max_nodes)

            print(f"{now=}: {nnodes=} {ntasks=} {nplus=}")

            #
            # Should we scale up?
            #
            if nplus > 0:
                new_nodes = create_nodes(cluster=cluster, count=nplus)

            #
            # Should we scale down?
            #
            check_scaledown(nodes)

            # sleep a bit so we don't hit Google's API server too hard
            dt = max(0, 10 - (time.time() - now))
            time.sleep(dt)
    finally:
        con.close()

last_busy = {}
def check_scaledown(nodes, timeout=30):
    # see which nodes we haven't heard from in awhile, and delete them if
    # they're idle for too long
    global last_busy
    
    now = time.time()
    delete_list = []

    print("-----------")
    for node in nodes:
        ip = node.network_interfaces[0].network_i_p
        creation_time = time.mktime(datetime.datetime.strptime(node.creation_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z").timetuple())

        try:
            lb = last_busy[ip]
        except KeyError:
            lb = last_busy[ip] = now

        # if this is a newly created node, give it some time to initialize
        lb = max(lb, creation_time + 120)

        age = now - lb
        if age > timeout:
            delete_list.append(node.name)

        print(f"{node.name:20s} {age=} (deletion in {timeout - age} seconds)")

    # Now delete any nodes that have been idle for too long
    if delete_list:
        #print(f"Would delete {delete_list}")
        delete_nodes(delete_list)

def scaledown_monitor(cluster, timeout=30):
    # see which nodes we haven't heard from in awhile, and delete them if
    # they're idle for too long
    global last_busy
    
    while True:
        now = time.time()
        delete_list = []
        nodes = get_nodes(cluster)
        print("-----------")
        for node in nodes:
            ip = node.network_interfaces[0].network_i_p
            try:
                lb = last_busy[ip]
            except KeyError:
                lb = last_busy[ip] = now

            age = now - lb
            if age > timeout:
                delete_list.append(node.name)

            print(f"{node.name:20s} {age=} (deletion in {timeout - age} seconds)")

        # Now delete any nodes that have been idle for too long
        if delete_list:
            #print(f"Would delete {delete_list}")
            delete_nodes(delete_list)
        else:
            time.sleep(10)

# receive status messages from nodes
def status_receiver(cluster):
    con = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    try:
        def callback(ch, method, properties, body):
            # decode and print the status message
            ip, task_uuid, ram, cpu = json.loads(body)
            print(f"({ip=}, {task_uuid=})   {ram=}, {cpu=}")

            # record that this node is busy
            if task_uuid is not None:
                global last_busy
                last_busy[ip] = time.time()

            # acknowledge delivery
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel = con.channel()
        queue = channel.queue_declare(queue='status', durable=True)
        channel.basic_consume(queue='status', on_message_callback=callback)
        channel.start_consuming()
    finally:
        con.close()

if __name__ == "__main__":
    #autoscaler_monitor(cluster='mjuric')
    #node_count(cluster='mjuric')
    #create_nodes(cluster='mjuric', count=2)
    #status_receiver(cluster='mjuric')
    #delete_nodes(["test-instance-000007", "test-instance-000008"])


    threading.Thread(target=status_receiver, args=('mjuric',), daemon=True).start() # status receiver thread
    autoscaler_monitor(cluster='mjuric')
