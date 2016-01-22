#!/usr/bin/python

# btpeer.py

import socket
import struct
import threading
import time
import traceback


def btdebug(msg):
    """
    Prints a message to the screen with the name of the current thread
    :param str msg: message which would be printed
    """
    print "[%s] %s" % (str(threading.currentThread().getName()), msg)


# ==============================================================================

class BTPeerException(Exception):
    """ Common base class for all  exceptions. """
    def __init__(self, *args, **kwargs):
        pass


class BTPeer:
    """ Implements the core functionality that might be used by a peer in a
    P2P network.

    """
    # --------------------------------------------------------------------------
    def __init__(self, max_peers, server_port, my_id=None, server_host=None):
        """ Initializes a peer servent (sic.) with the ability to catalog
        information for up to max_peers number of peers (max_peers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (server_host) will be determined by attempting to connect to an
        Internet host like Google.

        """
        self.debug = False

        self.max_peers = int(max_peers)
        self.server_port = int(server_port)
        if server_host:
            self.server_host = server_host
        else:
            self.__init_server_host()

        if my_id:
            self.my_id = my_id
        else:
            self.my_id = '%s:%d' % (self.server_host, self.server_port)

        # ensure proper access to peers list (maybe better to use threading.RLock (reentrant))
        self.peer_lock = threading.Lock()
        self.peers = {}        # peer_id ==> (host, port) mapping
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

    # --------------------------------------------------------------------------
    def __init_server_host(self):
        """ Attempt to connect to an Internet host in order to determine the
        local machine's IP address.

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("www.google.com", 80))
        self.server_host = s.getsockname()[0]
        s.close()

    # --------------------------------------------------------------------------
    def __debug(self, msg):
        if self.debug:
            btdebug(msg)

    # --------------------------------------------------------------------------
    def __handle_peer(self, client_sock):
        """
        handle_peer(new socket connection) -> ()

        Dispatches messages from the socket connection
        """

        self.__debug('New child ' + str(threading.currentThread().getName()))
        self.__debug('Connected ' + str(client_sock.getpeername()))

        host, port = client_sock.getpeername()
        peer_conn = BTpeer_connectionection(None, host, port, client_sock, debug=False)

        try:
            msgtype, msgdata = peer_conn.recv_data()
            if msgtype:
                msgtype = msgtype.upper()
            if msgtype not in self.handlers:
                self.__debug('Not handled: %s: %s' % (msgtype, msgdata))
            else:
                self.__debug('Handling peer msg: %s: %s' % (msgtype, msgdata))
                self.handlers[msgtype](peer_conn, msgdata)
        except KeyboardInterrupt:
            raise
        except BTPeerException:
            if self.debug:
                traceback.print_exc()

        self.__debug('Disconnecting ' + str(client_sock.getpeername()))
        peer_conn.close()

    # end handle_peer method

    # --------------------------------------------------------------------------
    def __run_stabilizer(self, stabilizer, delay):
        while not self.shutdown:
            stabilizer()
            time.sleep(delay)

    # --------------------------------------------------------------------------
    def set_my_id(self, my_id):
        self.my_id = my_id

    # --------------------------------------------------------------------------
    def start_stabilizer(self, stabilizer, delay):
        """ Registers and starts a stabilizer function with this peer.
        The function will be activated every <delay> seconds.

        """
        t = threading.Thread(target = self.__run_stabilizer, args = [stabilizer, delay])
        t.start()

    # --------------------------------------------------------------------------
    def add_handler(self, msgtype, handler):
        """ Registers the handler for the given message type with this peer """
        assert len(msgtype) == 4
        self.handlers[msgtype] = handler

    # --------------------------------------------------------------------------
    def add_router(self, router):
        """ Registers a routing function with this peer. The setup of routing
        is as follows: This peer maintains a list of other known peers
        (in self.peers). The routing function should take the name of
        a peer (which may not necessarily be present in self.peers)
        and decide which of the known peers a message should be routed
        to next in order to (hopefully) reach the desired peer. The router
        function should return a tuple of three values: (next-peer-id, host,
        port). If the message cannot be routed, the next-peer-id should be
        None.
        :param router
        """
        self.router = router

    # --------------------------------------------------------------------------
    def add_peer(self, peer_id, host, port):
        """ Adds a peer name and host:port mapping to the known list of peers.

        """
        if peer_id not in self.peers and (self.max_peers == 0 or
                         len(self.peers) < self.max_peers):
            self.peers[peer_id] = (host, int(port))
            return True
        else:
            return False

    # --------------------------------------------------------------------------
    def get_peer(self, peer_id):

        """ Returns the (host, port) tuple for the given peer name """
        assert peer_id in self.peers    # maybe make this just a return NULL?
        return self.peers[peer_id]

    # --------------------------------------------------------------------------
    def remove_peer(self, peer_id):
        """ Removes peer information from the known list of peers. """
        if peer_id in self.peers:
            del self.peers[peer_id]

    # --------------------------------------------------------------------------
    def add_peer_at(self, loc, peer_id, host, port):
        """ Inserts a peer's information at a specific position in the
        list of peers. The functions add_peer_at, get_peer_at, and remove_peer_at
        should not be used concurrently with add_peer, get_peer, and/or
        remove_peer.

        """
        self.peers[loc] = (peer_id, host, int(port))

    # --------------------------------------------------------------------------
    def get_peer_at(self, loc):
        if loc not in self.peers:
            return None
        return self.peers[loc]

    # --------------------------------------------------------------------------
    def remove_peer_at(self, loc):
        # TODO fix this shit
        # remove_peer(self, loc)
        pass

    # --------------------------------------------------------------------------
    def get_peer_ids(self):
        """ Return a list of all known peer id's. """
        return self.peers.keys()

    # --------------------------------------------------------------------------
    def get_peer_number(self):
        """ Return the number of known peer's. """
        return len(self.peers)

    # --------------------------------------------------------------------------
    def get_max_peers_reached(self):
        """ Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if max_peers is set to
        0.

        """
        assert self.max_peers == 0 or len(self.peers) <= self.max_peers
        return self.max_peers > 0 and len(self.peers) == self.max_peers

    # --------------------------------------------------------------------------
    def make_server_socket(self, port, backlog=5):
        """ Constructs and prepares a server socket listening on the given
        port.

        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', port))
        sock.listen(backlog)
        return sock

    # --------------------------------------------------------------------------
    def send_to_peer(self, peer_id, msgtype, msgdata, waitreply=True):
        """
        sendtopeer(peer id, message type, message data, wait for a reply)
         -> [(reply type, reply data), ...]

        Send a message to the identified peer. In order to decide how to
        send the message, the router handler for this peer will be called.
        If no router function has been registered, it will not work. The
        router function should provide the next immediate peer to whom the
        message should be forwarded. The peer's reply, if it is expected,
        will be returned.

        Returns None if the message could not be routed.
        """

        if self.router:
            next_pid, host, port = self.router(peer_id)
        if not self.router or not next_pid:
            self.__debug('Unable to route %s to %s' % (msgtype, peer_id))
            return None
        # host,port = self.peers[nextpid]
        return self.connect_and_send(host, port, msgtype, msgdata,
                                     pid=next_pid,
                                     waitreply=waitreply)
    
    # --------------------------------------------------------------------------
    def connect_and_send(self, host, port, msgtype, msgdata,
                         pid=None, waitreply=True):
        """
        connect_and_send(host, port, message type, message data, peer id,
        wait for a reply) -> [(reply type, reply data), ...]

        Connects and sends a message to the specified host:port. The host's
        reply, if expected, will be returned as a list of tuples.

        """
        msgreply = []
        one_reply = None
        try:
            peer_connection = BTpeer_connectionection(pid, host, port, debug=self.debug)
            peer_connection.send_data(msgtype, msgdata)
            self.__debug('Sent %s: %s' % (pid, msgtype))

            if waitreply:
                one_reply = peer_connection.recv_data()
            while one_reply != (None, None):
                msgreply.append(one_reply)
                self.__debug('Got reply %s: %s' % (pid, str(msgreply)))
                one_reply = peer_connection.recv_data()
            peer_connection.close()
        except KeyboardInterrupt:
            raise
        except BTPeerException:
            if self.debug:
                traceback.print_exc()

        return msgreply

    # end connect_send method
    # --------------------------------------------------------------------------
    def check_live_peers(self):
        """ Attempts to ping all currently known peers in order to ensure that
        they are still active. Removes any from the peer list that do
        not reply. This function can be used as a simple stabilizer.

        """
        to_delete = []
        for pid in self.peers:
            is_connected = False
            try:
                self.__debug('Check live %s' % pid)
                host, port = self.peers[pid]
                peer_connection = BTpeer_connectionection(pid, host, port, debug=self.debug)
                peer_connection.send_data('PING', '')
                is_connected = True
            except BTPeerException:
                to_delete.append(pid)
                if is_connected:
                    peer_connection.close()

        self.peer_lock.acquire()
        try:
            for pid in to_delete:
                if pid in self.peers: del self.peers[pid]
        finally:
            self.peer_lock.release()
        # end check_live_peers method

    # --------------------------------------------------------------------------
    def main_loop(self):
        s = self.make_server_socket(self.server_port)
        s.settimeout(2)
        self.__debug('Server started: %s (%s:%d)' % (self.my_id, self.server_host, self.server_port))

        while not self.shutdown:
            try:
                self.__debug('Listening for connections...')
                clientsock, clientaddr = s.accept()
                clientsock.settimeout(None)

                t = threading.Thread(target=self.__handle_peer,
                                     args=[clientsock])
                t.start()
            except KeyboardInterrupt:
                print 'KeyboardInterrupt: stopping mainloop'
                self.shutdown = True
                continue
            except BTPeerException:
                if self.debug:
                    traceback.print_exc()
                    continue

        # end while loop
        self.__debug('Main loop exiting')

        s.close()

    # end mainloop method
# end BTPeer class
# **********************************************************


class BTpeer_connectionection:

    # --------------------------------------------------------------------------
    def __init__(self, peer_id, host, port, sock=None, debug=False):
        # any exceptions thrown upwards

        self.id = peer_id
        self.debug = debug

        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect((host, int(port)))
        else:
            self.s = sock

        self.sd = self.s.makefile('rw', 0)

    # --------------------------------------------------------------------------
    def __make_msg(self, msgtype, msgdata):
        # --------------------------------------------------------------------------
        msg_len = len(msgdata)
        msg = struct.pack("!4sL%ds" % msg_len, msgtype, msg_len, msgdata)
        return msg

    # --------------------------------------------------------------------------
    def __debug(self, msg):
        if self.debug:
            btdebug(msg)

    # --------------------------------------------------------------------------
    def send_data(self, msgtype, msgdata):
        """
        send_data(message type, message data) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """

        try:
            msg = self.__make_msg(msgtype, msgdata)
            self.sd.write(msg)
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except BTPeerException:
            if self.debug:
                traceback.print_exc()
            return False
        return True

    # --------------------------------------------------------------------------
    def recv_data(self):
        """
        recv_data() -> (msgtype, msgdata)

        Receive a message from a peer connection. Returns (None, None)
        if there was any error.
        """

        try:
            msgtype = self.sd.read(4)
            if not msgtype: 
                return tuple((None, None))

            str_len = self.sd.read(4)
            msg_len = int(struct.unpack("!L", str_len)[0])
            msg = ""

            while len(msg) != msg_len:
                data = self.sd.read(min(2048, msg_len - len(msg)))
                if not len(data):
                    break
            msg += data

            if len(msg) != msg_len:
                return tuple((None, None))

        except KeyboardInterrupt:
            raise
        except BTPeerException:
            if self.debug:
                traceback.print_exc()
            return tuple((None, None))

        return tuple((msgtype, msg))

        # end recv_data method

    # --------------------------------------------------------------------------
    def close(self):
        """
        close()

        Close the peer connection. The send and recv methods will not work
        after this call.
        """

        self.s.close()
        self.s = None
        self.sd = None

    # --------------------------------------------------------------------------
    def __str__(self, peer_id):
        return "|%s|" % peer_id




