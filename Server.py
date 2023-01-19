import socket, threading, sys, json, time, os, signal
from datetime import datetime

# Get own IP
def find_ownIP():
    fs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    fs.settimeout(0)
    try:
        fs.connect(('10.0.0.1', 1))
        ownIP = fs.getsockname()[0]
    except Exception:
        ownIP = "127.0.0.1"
    finally:
        fs.close()
    return ownIP
ownIP = find_ownIP()

# Constants
BC_PORT = 59999  # Broadcast Listening Port
UC_PORT = 60000  # Unicast Listening Port
UDP_PORT = 59998 # UDP Listening Port
RING_PORT = 59997 # UDP Listening Port for leader election

# Global variables to store the state of the system
lead_ADDRESS = ''
server_LIST = [[], []]
client_LIST = [[], [], [], []]
triggerElection = False
electionInProgress = False
next_SERVER = ''

# Returns the current time
def get_currentTimeMicro():
    return (f'<{datetime.now().strftime("%H:%M:%S.%f")}> ')
gctm = get_currentTimeMicro

# Sends heartbeat messages to all connected servers and checks for responses
def monitor_Server():
    global lead_ADDRESS
    global triggerElection
    global next_SERVER
    global electionInProgress
    UDP_MonitorSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDP_MonitorSocket.settimeout(2)
    monitor_message= (f'heartbeat_server_request')        
    while True:
        # send heartbeat to all servers
        for i, server in enumerate(server_LIST[0]):
            if server[0] != (ownIP) and not electionInProgress:
                print(f'{gctm()}Monitor: Sending heartbeat to {server[0]} ...')
                UDP_MonitorSocket.sendto(str.encode(monitor_message), (server[0], UDP_PORT))
                try:
                    data,addr = UDP_MonitorSocket.recvfrom(1024)    
                    if data.decode() == "living":
                        print(f'{gctm()}Monitor: Received heartbeat from {server[0]} ...')
                        if int(server_LIST[1][i]) > 0:
                            server_LIST[1][i] = 0
                        continue
                except TimeoutError:
                    server_LIST[1][i] = str(int(server_LIST[1][i]) + 1)
                    print(f'{gctm()}Monitor: No answer from {server[0]}, increasing the heartbeat counter by 1, current heartbeat counter {server_LIST[1][i]} ...')
                    if int(server_LIST[1][i]) > 2:
                        print(f'{gctm()}Monitor: {server[0]} unavailable for 3 trys, beginning to handle error ...')
                        length = len(server_LIST[0])
                        if (length < 3) and (server[0] == lead_ADDRESS):
                            print(f'{gctm()}Monitor: Connection to lead server ({server[0]}) disrupted, no quorum, POISON PILL')
                            os.kill(os.getpid(), signal.SIGINT)
                        elif (length >= 3) and (server[0] == lead_ADDRESS):
                            print(f'{gctm()}Monitor: Connection to lead server ({server[0]}) disrupted, quorom available, calling for LEADER Election')
                            remove_ServerConnection(connection=server)
                            lead_ADDRESS = None
                            ring = form_Ring(servers=server_LIST)
                            next_SERVER = get_nextServer(ring=ring)
                            triggerElection = True
                            electionInProgress = True
                        elif server[0] != lead_ADDRESS and lead_ADDRESS == ownIP:
                            print(f'{gctm()}Monitor: No response from {server[0]}, removing ...')
                            remove_ServerConnection(connection=server)
                            ring = form_Ring(servers=server_LIST)
                            next_SERVER = get_nextServer(ring=ring)
                            UDP_SENDER(task="sendLISTS")      
                        elif server[0] != lead_ADDRESS and lead_ADDRESS != ownIP:
                            print(f'{gctm()}Monitor: No response from {server[0]}, sending request for updated server_LIST to leader ...')
                            UDP_SENDER(task="requestUpdate")
        time.sleep(5)

# Sends UDP messages to clients and servers depending on the task the function is called with
def UDP_SENDER(task):
    global next_SERVER
    UDP_SenderSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDP_SenderSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    UDP_SenderSocket.settimeout(2)
    if task == "sendLEADAddress":
        print(f'{gctm()}UDP_SENDER: Send lead_ADDRESS to clients ...')
        message = (f'server_newLeader;{lead_ADDRESS}').encode()
        for i, client, in enumerate(client_LIST[2]):
            clientAddress = client.split(",")
            clientIP = clientAddress[0]
            clientIP = clientIP.strip("(").strip(")").strip("'")
            clientPort = clientAddress[1]
            clientPort = clientPort.strip().strip(")")
            UDP_SenderSocket.sendto(message, (clientIP, int(clientPort)))
    elif task == "sendLISTS":
        print(f'{gctm()}UDP_SENDER: Send lists to servers ...')
        index_list = [0,1,2]
        client_LISTIndex = [client_LIST[i] for i in index_list]
        message = (f'heartbeat_server_newserverlist{json.dumps([server_LIST, client_LISTIndex, lead_ADDRESS])}').encode()
        for i, server in enumerate(server_LIST[0]):
            if server[0] != (ownIP):
                UDP_SenderSocket.sendto(message, (server[0], UDP_PORT))
    elif task == "requestUpdate":
        print(f'{gctm()}UDP_SENDER: Send updateLists request to leader ...')
        message ="server_request_updateLists".encode()
        UDP_SenderSocket.sendto(message, (lead_ADDRESS, UDP_PORT))
    elif task == "sendHeartbeatElection":
        print(f'{gctm()}UDP_SENDER: Send heartbeat to {next_SERVER} because of leader election ...')
        message= ('heartbeat_server_request_leader')
        server = next_SERVER        
        UDP_SenderSocket.sendto(str.encode(message), (server, UDP_PORT))
        try:
            data_heartbeat, addr = UDP_SenderSocket.recvfrom(1024)
            if data_heartbeat:
                print(f'{gctm()}UDP_SENDER: Heartbeat received from {addr[0]}, next_SERVER is valid ...')
        except TimeoutError:
            print(f'{gctm()}UDP_SENDER: {server} no longer answering, removing and setting new next_SERVER ...')
            remove_ServerConnection(connection=server)
            ring = form_Ring(servers=server_LIST)
            next_SERVER = get_nextServer(ring=ring)
            print(f'{gctm()}RING: next_SERVER is {next_SERVER}')
            UDP_SENDER(task="sendHeartbeatElection")

    UDP_SenderSocket.close()

# Forms a ring of servers
def form_Ring(servers):
    sorted_binary_ring = sorted([socket.inet_aton(server[0]) for server in servers[0]])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
    return sorted_ip_ring

# Returns the next server in the ring
def get_nextServer(ring):
    current_node_index = ring.index(ownIP) if ownIP in ring else -1
    if current_node_index != -1:   
        if current_node_index + 1 == len(ring):
            return ring[0]
        else:
            return ring[current_node_index + 1]
    else:
        return None

# Listens for incoming election messages and participates in the election process
def RING_Listener():
    global lead_ADDRESS
    global triggerElection
    global next_SERVER
    global electionInProgress

    RING_Socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    RING_Socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    RING_Socket.bind((ownIP, RING_PORT))
    RING_Socket.settimeout(4)

    print(f'{gctm()}RING: Listening to election messages from servers on {ownIP}, {RING_PORT}')

    participant = False

    while True:
        if triggerElection == True:
            print(f'{gctm()}RING: Election was triggered, sending message to {next_SERVER}')
            election_message = {
                "mid": ownIP,
                "isLeader": False
            }
            participant = True
            triggerElection = False
            RING_Socket.sendto(json.dumps(election_message).encode(), (next_SERVER, RING_PORT))
        try:
            data, addr = RING_Socket.recvfrom(1024)
        except TimeoutError:
            continue
        if data:
            election_message = json.loads(data.decode())
            print(f'{gctm()}RING: Received election message {election_message} from {addr[0]} ...')
            UDP_SENDER(task="sendHeartbeatElection")
            if election_message['isLeader'] and election_message['mid'] != ownIP: 
                print(f'{gctm()}RING: Voting is finished, {election_message["mid"]} is the new leader!')               
                lead_ADDRESS = election_message['mid']
                # forward received election message to left neighbour
                participant = False
                electionInProgress = False
                RING_Socket.sendto(json.dumps(election_message).encode(), (next_SERVER, RING_PORT))
            elif election_message['mid'] < ownIP and not participant:
                print(f'{gctm()}RING: Voting in progress, UID is lower than my own, setting MID as {ownIP}!')
                new_election_message = {
                    "mid": ownIP,
                    "isLeader": False
                }
                participant = True
                electionInProgress = True
                RING_Socket.sendto(json.dumps(new_election_message).encode(), (next_SERVER, RING_PORT))
            elif election_message['mid'] > ownIP:
                print(f'{gctm()}RING: Voting in progress, UID is higher than my own, forwarding message!')
                participant = True
                electionInProgress = True
                RING_Socket.sendto(json.dumps(election_message).encode(), (next_SERVER, RING_PORT))
            elif election_message['mid'] == ownIP and not election_message['isLeader']:
                print(f'{gctm()}RING: Voting in progress, UID is my own, declaring myself as leader!')
                lead_ADDRESS = ownIP
                new_election_message = {
                    "mid": ownIP,
                    "isLeader": True
                }
                # send new election message to left neighbour
                participant = False
                electionInProgress = False
                RING_Socket.sendto(json.dumps(new_election_message).encode(), (next_SERVER, RING_PORT))
                UDP_SENDER(task="sendLEADAddress")
            else:
                print(f'{gctm()}RING: Election message received, unknown message, <{election_message}>, discarding ...')

            data = None

# Listens for incoming messages and processes them based on their content
def UDP_Listener():
    global next_SERVER

    UDP_ListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDP_ListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    UDP_ListenSocket.bind((ownIP, UDP_PORT))

    print(f'{gctm()}UDP_Listener: Listening to UDP from servers on {ownIP}, {UDP_PORT}')
    try:
        while True:
            data, addr = UDP_ListenSocket.recvfrom(1024)
            message = data.decode()
            if (message == "heartbeat_server_request") and (any(addr[0] in server for server in server_LIST[0])):
                UDP_ListenSocket.sendto("living".encode(), addr)
                print(f'{gctm()}UDP_Listener: Heartbeat message from {addr[0]} received, answering ...')
            elif (message == "heartbeat_server_request_leader") and (any(addr[0] in server for server in server_LIST[0])):
                UDP_ListenSocket.sendto("living".encode(), addr)
                print(f'{gctm()}UDP_Listener: Heartbeat message for leader election from {addr[0]} received, answering ...')
            elif (message == "heartbeat_server_request") and not (any(addr[0] in server for server in server_LIST[0])):
                print(f'{gctm()}UDP_Listener: Heartbeat message from {addr[0]} received, not in SERVER_LIST, no answer ... ')
            elif (message == "heartbeat_server_request_leader") and not (any(addr[0] in server for server in server_LIST[0])):
                UDP_ListenSocket.sendto("living".encode(), addr)
                print(f'{gctm()}UDP_Listener: Heartbeat message for leader election from {addr[0]} received, not in SERVER_LIST, no answer ...')
            elif message.startswith("heartbeat_server_newserverlist") and (addr[0] == lead_ADDRESS):
                data = data.decode()
                data = data.replace("heartbeat_server_newserverlist","")
                data = data.encode()
                create_ServerList(data)
                ring = form_Ring(server_LIST)
                next_SERVER = get_nextServer(ring=ring)
                print(f'{gctm()}UDP_Listener: New LISTs were received, updating local LISTs.')
            elif message.startswith("server_request_updateLists") and (any(addr[0] in server for server in server_LIST[0])):
                UDP_SENDER(task="sendLISTS")
                print(f'{gctm()}UDP_Listener: Request for update of LISTs received, pushing them out...')
            else:
                print(f'{gctm()}UDP_Listener: Unknown message receveid, content: <{message}>, discarding ...')
    finally:
        UDP_ListenSocket.close()

# Broadcast listener for Server and Client 
def BC_Listener():
    global next_SERVER
    bc_ListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    bc_ListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    bc_ListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    bc_ListenSocket.bind((ownIP, BC_PORT))

    print(f'{gctm()}BC_Listener: Listening to broadcasts from servers and clients on {ownIP}, {BC_PORT}')

    try:
        while True:
            data, addr = bc_ListenSocket.recvfrom(1024)
            message = data.decode()
            if message.startswith('join_chat_request'):
                new_client_address = message.split(';')[1].split(',')[:2]
                print(f'{gctm()}BC_Listener: New client discovery request: {new_client_address}')
                bc_ListenSocket.sendto(lead_ADDRESS.encode(), addr)
            elif message.startswith('join_server_group'):                
                if ownIP == lead_ADDRESS:
                    new_server_address = message.split(';')[1]
                    new_server_address = (new_server_address, UC_PORT)
                    print(f'{gctm()}BC_Listener: New server discovery request: {new_server_address}')
                    server_LIST[0].append(new_server_address)
                    server_LIST[1].append('0')
                    ring = form_Ring(servers=server_LIST)
                    next_SERVER = get_nextServer(ring=ring)
                    index_list = [0,1,2]
                    client_LISTIndex = [client_LIST[i] for i in index_list]
                    bc_ListenSocket.sendto(json.dumps([server_LIST, client_LISTIndex, lead_ADDRESS]).encode(), addr)
                    UDP_SENDER(task="sendLISTS")
                    print(f'{gctm()}BC_Listener: Server connected: {new_server_address}')
                else:
                    print(f'{gctm()}BC_Listener: Not the leader, no answer ...')
    except ConnectionResetError:
        print(f'{gctm()}BC_Listener: Connection was closed, client probably crashed ...')

# Sends a broadcast message over the network asking to join a server group and listens for a response that it can work with
def BC_Sender(port):
    global lead_ADDRESS
    global next_SERVER
    global server_LIST
    BC_Socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    BC_Socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    BC_Socket.settimeout(2)
    BC_message= f'join_server_group_request;{ownIP}'
    BC_Socket.sendto(str.encode(BC_message), ('255.255.255.255', port))
    while True:
        try:
            data, addr = BC_Socket.recvfrom(1024)
            if data:
                create_ServerList(data)
                ring = form_Ring(servers=server_LIST)
                next_SERVER = get_nextServer(ring=ring)
                print(f'{gctm()}BC_Sender: Other server found, joining the server group with leader {lead_ADDRESS}')
                BC_Socket.close()
                break
        except TimeoutError:
            lead_ADDRESS = ownIP
            server_LIST = [[(ownIP, UC_PORT)], ['0']]
            BC_Socket.close()
            print(f'{gctm()}BC_Sender: No other server found, creating group and setting myself as leader. \n{gctm()}Current Leader is {lead_ADDRESS}')
            break

# Accpets data in and updates the server_LIST, client_LIST and lead_ADDRESS variables
def create_ServerList(data):
    global lead_ADDRESS
    global server_LIST
    global client_LIST

    jsonData = json.loads(data)
    current_server_LIST = jsonData[0]
    current_client_LIST = jsonData[1]
    current_leader = jsonData[2]

    server_LIST = [[], []]
    client_LIST = [[], [], [], []]

    # get current client list
    for client_name in current_client_LIST[0]:
         client_LIST[0].append(client_name)

    for client_IP in current_client_LIST[1]:
        client_LIST[1].append(client_IP) 
    
    for client_UDP in current_client_LIST[2]:
        client_LIST[2].append(client_UDP)

    #print(f'{gctm()}new client list: {client_LIST}')

    # get current server list
    for server_address in current_server_LIST[0]:
        server_LIST[0].append(tuple(server_address))

    for heartbeat_number in current_server_LIST[1]:
        server_LIST[1].append('0')
    #print(f'{gctm()}new server list: {server_LIST}')

    # get current leader
    
    lead_ADDRESS = current_leader
    #print(f'{gctm()}current leader: {lead_ADDRESS}')

# Thread that handles the connection with a client
def client_Thread(conn, addr):
    # sends a message to the client whose user object is conn    
    conn.sendall(bytes("request_server_client_setup", 'UTF-8'))
    clientSetup = False
    while clientSetup != True:
        try:
            data = conn.recv(4096)
            message = data.decode()
            if message.startswith("response_client_setup"):
                splitMessage = message.split(';')
                client_LIST[0].append(splitMessage[1])
                client_LIST[1].append(splitMessage[2])
                client_LIST[2].append(splitMessage[3])
                client_LIST[3].append(conn)
                UDP_SENDER(task="sendLISTS")
                print(f'{gctm()}Client_Thread: {splitMessage[1]} was added to the list and is now ready!')
                conn.sendall(bytes("request_server_client_finished", "UTF-8"))
                clientSetup = True
            else:
                conn.close()
                print(f'{gctm()}Client_Thread: Closed {conn}, setup failed.')
                sys.exit()
        except ConnectionResetError:
            print(f'{gctm()}Client_Thread: Closed {conn}, no connection etablished.')
            conn.close()
            sys.exit()
        except Exception as e:
            print(e)
            continue
    while True:
        try:
            message = conn.recv(4096)
            if message:
                message = message.decode()
                message = (f'\n<{datetime.now().strftime("%H:%M.%S")}><{splitMessage[1]}>{message}')
                message = message.encode()
                for client in client_LIST[3]:
                    if client != conn:
                        client.sendall(message)
            else:
                """message may have no content if the connection
					is broken, in this case we remove the connection"""
                remove_ClientConnection(conn)
                UDP_SENDER(task="sendLISTS")
                print(f'{gctm()}Client_Thread: Removed {conn} because destroyed connection')
                sys.exit()
        except ConnectionResetError:
            remove_ClientConnection(conn)
            UDP_SENDER(task="sendLISTS")
            print(f'{gctm()}Client_Thread: Removed {conn} because ConnectionResetError.')
            sys.exit()
        except Exception as e:
            print(e)
            #logging.Logger.exception(e)
            continue

# Removes a disconnected client from the list of connected clients
def remove_ClientConnection(connection):
    try:
        index = server_LIST[0].index(connection)
    except ValueError:
        print(f'{gctm()}remove_ClientConnection: Failed to remove {connection}, probably already deleted ...')
        return
    del client_LIST[0][index]
    del client_LIST[1][index]
    del client_LIST[2][index]
    del client_LIST[3][index]

#  Removes a server from the list of connected servers
def remove_ServerConnection(connection):
    try:
        index = server_LIST[0].index(connection)
    except ValueError:
        print(f'{gctm()}remove_ServerConnection: Failed to remove {connection}, probably already deleted ...')
        return
    del server_LIST[0][index]
    del server_LIST[1][index]

# Listens for incoming client connections and starts the function client_Thread()
def UC_Listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((ownIP, UC_PORT))
    
    server.listen(100)

    print(f'{gctm()}UC_Listener: Listening to connections from clients on {ownIP}, {UC_PORT}')

    while True:
            conn, addr = server.accept()
            print(f'{gctm()}UC_Listener: {addr[0]} connected, starting own thread')
            threading.Thread(target=client_Thread, args=(conn, addr)).start()

# Starts several threads
if __name__ == '__main__':
    ownIP = find_ownIP()
    print(f'{gctm()}Default network adapter to use: {ownIP}')
    BC_Sender(BC_PORT)
    try:
        # Define the threads to start
        t1 = threading.Thread(target=BC_Listener)
        t2 = threading.Thread(target=UC_Listener)
        t3 = threading.Thread(target=monitor_Server)
        t4 = threading.Thread(target=UDP_Listener)
        t5 = threading.Thread(target=RING_Listener)

        # Start the definied threads
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
    except KeyboardInterrupt:
        os.kill(os.getpid(), signal.SIGINT)
        quit()