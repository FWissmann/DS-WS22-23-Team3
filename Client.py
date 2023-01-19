import socket, time, threading, sys, os, signal
from datetime import datetime

# Global variables to store the state of the system
serverAddress = ''
UDP_Addr = ''
sendMessage = None
connectAttempts = 0

# Constants
BC_Port = 59999 # Broadcast Listening Port
UC_Port = 60000 # Unicast Listening Port

# Get own IP address
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

# Returns the current time
def get_currentTimeMicro():
    return (f'<{datetime.now().strftime("%H:%M:%S.%f")}> ')
gctm = get_currentTimeMicro

# Broadcast a message to join a chat
def BC_Sender(port):
    BC_SenderSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    BC_SenderSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    BC_SenderSocket.settimeout(2)
    bc_message = f'join_chat_request;{ownIP}'
    BC_SenderSocket.sendto(str.encode(bc_message), ('255.255.255.255', port))

    while True:
        try:
            data, addr = BC_SenderSocket.recvfrom(1024)
            BC_SenderSocket.close()
            return data.decode()
        except TimeoutError:
            BC_SenderSocket.close()
            break
        finally:
            BC_SenderSocket.close()

# Listen for UDP messages from servers on the network
def UDP_Listener():
    global UDP_ListenSocket
    global UDP_Addr
    UDP_ListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDP_ListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    UDP_ListenSocket.bind((ownIP, 0))
   
    UDP_Addr = UDP_ListenSocket.getsockname()

    print(f'{gctm()}UDP_Listener: Listening to UDP from servers on {UDP_Addr}')
    while True:
        try:
            data, addr = UDP_ListenSocket.recvfrom(1024)
        except TimeoutError:
            continue
        if data:
            message = data.decode()
            if message.startswith("server_newLeader"):
                global serverAddress
                new_leadServer = message.split(";")[1]
                serverAddress = new_leadServer

# Waits for user input and stores it
def get_Text():
    global sendMessage
    while True:
        try:
                message = input(f'<{clientName}>(You)')
        except EOFError:
                print(f'{gctm()}Received shutdown signal, closing!')
                os.kill(os.getpid(), signal.SIGINT)
        if message == ":q":
                os.kill(os.getpid(), signal.SIGINT)
        sendMessage = message

# Sends messages to a server
def UC_Sender(port, IP_address):
    global serverAddress
    global sendMessage
    global connectAttempts
    UC_SenderSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    UC_SenderSocket.settimeout(1)
    try:
        UC_SenderSocket.connect((IP_address, port))
    except TimeoutError:
        print(f'{gctm()}UC_Sender: Couldn\'t connect to {IP_address}, trying again ...')
        UC_SenderSocket.close()
        connectAttempts = connectAttempts + 1
        return

    while True:
        try:
            data, addr = UC_SenderSocket.recvfrom(1024)
        except ConnectionResetError:
            print(f'{gctm()}UC_Sender: Server has closed the connection while waiting for setup, trying again ...')
            UC_SenderSocket.close()
            connectAttempts = connectAttempts + 1
            break
        except TimeoutError:
            print(f'{gctm()}UC_Sender: Server has closed the connection while waiting for setup, trying again ...')
            UC_SenderSocket.close()
            connectAttempts = connectAttempts + 1
            break
        if data:
            message = data.decode()
            if message == "request_server_client_setup":
                response = (f'response_client_setup;{clientName};{ownIP};{UDP_Addr}')
                UC_SenderSocket.sendall(bytes(response, 'UTF-8'))
            elif message == "request_server_client_finished":
                print(f'{gctm()}UC_Sender: Setup finished, chat is ready!')
                connectAttempts = 0
                break                

    t2 = threading.Thread(target=get_Text)
    t2.start()

    while True:
        if sendMessage:
            try:
                UC_SenderSocket.sendall(bytes(sendMessage, 'UTF-8'))
                sendMessage = None
            except ConnectionResetError:
                print(f'{gctm()}UC_Sender: Server has closed the connection, trying to open new connection ..')
                UC_SenderSocket.close()
                break
        try:
            data, addr = UC_SenderSocket.recvfrom(1024)
        except ConnectionResetError:
            print(f'{gctm()}UC_Sender: Server has closed the connection, trying to open new connection ...')
            UC_SenderSocket.close()
            break
        except TimeoutError:
            continue
        if data:
            messsage = data.decode()
            print(f'{messsage}')

# Entry point of the script, calling functions and running scripts
if __name__ == '__main__':
    ownIP = find_ownIP()
    print(f'{gctm()}Main thread: Your IP is: {ownIP}\nPlease check if this appears to be true, otherwise disconnect VPN.')
    t1 = threading.Thread(target=UDP_Listener)
    t1.start()
    time.sleep(1)
    serverAddress = BC_Sender(BC_Port)
    while serverAddress == None:
        try:
            print(f'{gctm()}Main thread: No server found, retry in 5 seconds (cancel with Ctrl + C)')
            time.sleep(5)
            print(f'{gctm()}Main thread: New try to find server ...')
            serverAddress = BC_Sender(BC_Port)
        except KeyboardInterrupt:
            print(f'{gctm()}Main thread: Received shutdown signal, closing!')
            os.kill(os.getpid(), signal.SIGINT)
    print(f'{gctm()}Main thread: Server was found: {serverAddress}')
    clientName = input(f'Please enter your name: ')
    print(f'Your name is {clientName}')

    while True:
        try:
            if serverAddress != None:
                UC_Sender(UC_Port, serverAddress)
            if connectAttempts > 5:
                serverAddressBC = BC_Sender(BC_Port)
                if serverAddressBC != None:
                    print(f'{gctm()}Main thread: New server {serverAddressBC} has been found, do you want to join? This is not the same server group as before!\n')
                    if (input("yes/no\n") == "yes"):
                        serverAddress = serverAddressBC
            print(f'{gctm()}Main thread: No server available, waiting 5 seconds... (cancel with Ctrl + C)')
            time.sleep(5)
        except KeyboardInterrupt:
            print(f'{gctm()}Main thread: Received shutdown signal, closing!')
            os.kill(os.getpid(), signal.SIGINT)