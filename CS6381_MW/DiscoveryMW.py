###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json # for json parsing

# import serialization logic
from CS6381_MW import discovery_pb2

##################################
#       Discovery Middleware class
##################################


class DiscoveryMW():
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statemeisreadyh our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.hash_to_ip = None # hash to ip table
        self.req = None # a REQ socket for talking to other discovery node
        self.send_buffer = None
    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        '''Initialize the object'''
        try:
            self.logger.info("DiscoveryMW::configure")

            # retrieve our advertised ip addr and publication port num
            self.port = args.port
            self.addr = args.addr

            # obtain the ZMQ context
            self.logger.debug("DiscoveryMW::configure: obtain ZMQ context")
            context = zmq.Context()
            
            # TODO: POLLER HERE
            self.logger.debug("DiscoveryMW::configure: create ZMQ POLLER")
            self.poller = zmq.Poller()

            self.logger.debug("DiscoveryMW::configure: create ZMQ REP socket")
            self.rep = context.socket(zmq.ROUTER)
            # making a dictionary of req objects based on hash
            self.req = {}
            self.logger.debug("DiscoveryMW::configure: register poller")
            self.poller.register(self.rep, zmq.POLLIN)
            # bind socket to the address
            self.logger.debug(
                "DiscoveryMW::configure: bind REP socket to address")
            bind_string = "tcp://*:" + str(self.port)
            self.rep.bind(bind_string)
            self.logger.debug("DiscoveryMW::configure: reading hash to ip json")
            file_hash_to_ip = open("hash_to_ip.json")
            self.hash_to_ip = json.load(file_hash_to_ip) 

            # create buffer
            self.send_buffer = {}
            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            raise e
        
    def connectTable(self, table):
        self.logger.debug("DiscoveryMW::connectTable: connecting to all nodes in finger table")
        context = zmq.Context()
        for hash in table:
            key = str(hash)
            if key not in self.req:
                self.req[key] = context.socket(zmq.DEALER)
                node = self.hash_to_ip[key]
                connect_str = "tcp://" + node['IP'] + ":" + str(node['port'])
                self.poller.register(self.req[key], zmq.POLLIN)
                self.req[key].setsockopt(zmq.LINGER, 0)
                self.req[key].setsockopt(zmq.RCVTIMEO, 1000)
                self.req[key].connect(connect_str)
                self.logger.debug(f"DiscoveryMW::connectTable: {key} - {self.req[key]} connected to {self.hash_to_ip[key]['id']}")
        self.logger.debug("DiscoveryMW::connectTable: connected in dictionary self.req")
    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop (self, timeout=None):
        try:
            self.logger.info("DiscoveryMW::event_loop: start")
            while self.handle_events:
                self.logger.debug("waiting for events...")
                events = dict(self.poller.poll(timeout = timeout))
                fault = True
                self.logger.debug(events)
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                    fault = False
                if self.rep in events:
                    self.logger.debug(f"Calling handle_request()")
                    timeout = self.handle_request()
                    fault = False
                # check if any of the req sockets in dictionary are in events
                for hash, socket in self.req.items():
                    if socket in events:
                        self.logger.debug(f"Calling handle_reply() for hash {hash}")
                        timeout = self.handle_reply(hash)
                        fault = False
                if (fault):
                    raise Exception ("Unknown event after poll")
                
                self.logger.info("control back to event loop")

            self.logger.info("DiscoveryMW::event_loop out of the event loop")
        except Exception as e:
            raise e


    def handle_reply(self, next):
        try:
            self.logger.info("DiscoveryMW::handle_reply:")
            # receive message
            _, bytesRcvd = self.req[next].recv_multipart()
            self.logger.info("DiscoveryMW::handle_reply: received a reply from {}".format(self.hash_to_ip[str(next)]['id']))
            # parse the request
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd) 
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER_DHT):
                #make an upcall to the application logic
                timeout = self.upcall_obj.handle_register_reply_dht(disc_resp.register_resp_dht)
        
            if (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY_DHT):
                #make an upcall to the application logic
                timeout = self.upcall_obj.handle_isready_reply_dht(disc_resp.isready_resp_dht)
                
            if (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC_DHT):
                timeout = self.upcall_obj.lookup_pub_by_topic_reply_dht(disc_resp.lookup_resp_dht)
            next = str(next)
            self.logger.info("DiscoveryMW::handle_reply: checking buffer of next is " + str(next))
            if (self.send_buffer[next] != []):
                self.send_buffer[next].pop()
            else:
                self.logger.info("DiscoveryMW::handle_reply: buffer is empty - not expected")
            if (self.send_buffer[next] != []):
                self.logger.info("DiscoveryMW::handle_reply: sending next message in buffer to {}".format(self.hash_to_ip[str(next)]['id']))
                identity, buf2send = self.send_buffer[next][0]
                self.req[next].send([identity, buf2send]) 
            self.logger.debug("DiscoveryMW::handle_request: done handling reply with timeout {}".format(timeout))
            return None
        except Exception as e:
            raise e
    # handle the poller request:
    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request:")
            # receive message
            rcv = self.rep.recv_multipart()
            print(rcv)
            for identity, bytesRcvd in rcv:
                # parse the request
                disc_req = discovery_pb2.DiscoveryReq()
                disc_req.ParseFromString(bytesRcvd) 
                
                
                # we have a request. Now we need to handle it. For that we need
                # to call the application logic. So we need to upcall to the
                # application logic. We will pass the request
                if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                    #make an upcall to the application logic
                    timeout = self.upcall_obj.handle_register(disc_req.register_req, identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                    # the is ready message should be empty
                    timeout = self.upcall_obj.isready_request(identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                    timeout = self.upcall_obj.handle_lookup(disc_req.lookup_req, True, identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC_DHT):
                    if (disc_req.lookup_req_dht.end):
                        timeout = self.upcall_obj.lookup_chord(disc_req.lookup_req_dht, identity)
                    else:
                        timeout = self.upcall_obj.lookup_pub_by_topic_request(disc_req.lookup_req_dht, disc_req.lookup_req_dht.from_broker, identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                    timeout = self.upcall_obj.handle_lookup(disc_req.lookup_req, False, identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_REGISTER_DHT):
                    if (disc_req.register_req_dht.end):
                        timeout = self.upcall_obj.register_chord(disc_req.register_req_dht, identity)
                    else:
                        timeout = self.upcall_obj.handle_register_dht(disc_req.register_req_dht, identity)
                elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY_DHT):
                    timeout = self.upcall_obj.isready_loop(disc_req.isready_req_dht, identity )
                    
                else:
                    raise ValueError("DiscoveryMW::event_loop: unknown message type")
                self.logger.debug("DiscoveryMW::handle_request: done handling request with timeout = {}".format(timeout))
            return timeout
        except Exception as e:
            raise e
    
    
    # is_ready_reply
    def is_ready_reply(self, status):
        try:
            self.logger.debug("DiscoveryMW::is_ready_reply")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_ISREADY
            isready_resp = discovery_pb2.IsReadyResp()
            isready_resp.status = status
            disc_rep.isready_resp.CopyFrom(isready_resp)

            self.logger.debug("DiscoveryMW::is_ready_reply: done building reply")
            
            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send this to the client
            self.logger.debug("DiscoveryMW::is_ready_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::is_ready_reply: done replying to client is_ready request")
        except Exception as e:
            raise e
    
    # is_ready_reply_dht
    def is_ready_reply_dht(self, status, src, cur):
        try:
            self.logger.debug("DiscoveryMW::is_ready_reply_dht")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_ISREADY_DHT
            isready_resp_dht = discovery_pb2.IsReadyRespDHT()
            isready_resp_dht.status = status
            isready_resp_dht.src = src
            isready_resp_dht.cur = cur
            disc_rep.isready_resp_dht.CopyFrom(isready_resp_dht)

            self.logger.debug("DiscoveryMW::is_ready_reply: done building reply")
            
            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send this to the client
            self.logger.debug("DiscoveryMW::is_ready_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::is_ready_reply: done replying to client is_ready request")
        except Exception as e:
            raise e
                
        
    def register_reply(self, status):
        try:
            self.logger.debug("DiscoveryMW::register_reply")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_REGISTER
            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = status
            disc_rep.register_resp.CopyFrom(register_resp)
            
            self.logger.debug("DiscoveryMW::register_reply: done building reply")
            
            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send this to the client
            self.logger.debug("DiscoveryMW::register_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::register_reply: done replying to client register request")
            return None
        except Exception as e:
            raise e
        
    def register_reply_dht(self, status, id, src, reason, cur):
        try:
            self.logger.debug("DiscoveryMW::register_reply")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_REGISTER_DHT
            register_resp = discovery_pb2.RegisterRespDHT()
            register_resp.status = status
            register_resp.id = id
            register_resp.src = src
            register_resp.reason = reason
            register_resp.cur = cur
            disc_rep.register_resp_dht.CopyFrom(register_resp)
            
            self.logger.debug("DiscoveryMW::register_reply: done building reply")
            
            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send this to the client
            self.logger.debug("DiscoveryMW::register_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::register_reply: done replying to client register request")
            return None
        except Exception as e:
            raise e
        
    
    def lookup_pub_by_topic_reply(self, publist):
        try: 
            self.logger.debug("DiscoveryMW::lookup_reply")

            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            pub_info = []
            for pub in publist:
                info = discovery_pb2.RegistrantInfo()
                info.id = pub.id
                info.addr = pub.addr
                info.port = pub.port
                pub_info.append(info)
            
            lookup_resp.publist.extend(publist)
            disc_rep.lookup_resp.CopyFrom(lookup_resp)
            
            self.logger.debug("DiscoveryMW::lookup_reply: done building reply")
            
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send to client
            self.logger.debug("DiscoveryMW::lookup_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::lookup_reply: done replying to client lookup request")
            return None
        except Exception as e:
            raise e
        
    def lookup_pub_by_topic_reply_dht(self, publist, src, cur, jobid):
        try: 
            self.logger.debug("DiscoveryMW::lookup_reply")

            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC_DHT
            lookup_resp_dht = discovery_pb2.LookupPubByTopicRespDHT()
            pub_info = []
            for pub in publist:
                info = discovery_pb2.RegistrantInfo()
                info.id = pub.id
                info.addr = pub.addr
                info.port = pub.port
                pub_info.append(info)
            
            lookup_resp_dht.publist.extend(publist)
            lookup_resp_dht.src = src
            lookup_resp_dht.cur = cur
            lookup_resp_dht.jobid = jobid
            disc_rep.lookup_resp_dht.CopyFrom(lookup_resp_dht)
            
            self.logger.debug("DiscoveryMW::lookup_reply: done building reply")
            
            buf2send = disc_rep.SerializeToString()
            self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
            
            # send to client
            self.logger.debug("DiscoveryMW::lookup_reply: send reply")
            self.rep.send(buf2send)
            
            self.logger.info("DiscoveryMW::lookup_reply: done replying to client lookup request")
            return None
        except Exception as e:
            raise e
        
    def set_upcall_handle (self, upcall_obj):
        self.upcall_obj = upcall_obj
    
    # disable event loop
    def disable_event_loop(self):
        self.handle_events = False
        
        
    def propagateRegister(self, next, reg_req_dht, end, dest, src):
        self.logger.info("DiscoveryMW::propagateRegister: building dht request to send to {}".format(self.hash_to_ip[str(next)]['id']))
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER_DHT
        register_req_dht = discovery_pb2.RegisterReqDHT()
        register_req_dht.role = reg_req_dht.role
        register_req_dht.info.CopyFrom(reg_req_dht.info)
        register_req_dht.topiclist[:] = reg_req_dht.topiclist
        register_req_dht.end = end
        register_req_dht.dest = dest
        register_req_dht.src = src
        disc_req.register_req_dht.CopyFrom(register_req_dht)
        self.logger.debug("DiscoveryMW::propagateRegister: done building dht request")
        buf2send = disc_req.SerializeToString()
        self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
        # check buffer before sending, if empty, send it, otherwise, queue it
        next = str(next)
        if (next not in self.send_buffer):
            self.logger.debug("DiscoveryMW::propagateRegister: next not in send_buffer, initializing empty")
            self.send_buffer[next] = []
        
        if (self.send_buffer[next] == []):
            self.logger.info("DiscoveryMW::propagateRegister: sending dht request")
            self.logger.info("DiscoveryMW::propagateRegister: next = {}".format(self.req[next]))
            self.req[next].send(buf2send)
            self.logger.info("DiscoveryMW::propagateRegister: done sending dht request")
        else:
            self.logger.info("DiscoveryMW::propagateRegister: socket busy, queueing dht request")
        self.send_buffer[next].append(buf2send)

    
    def propagateIsReady(self, next, count_pub, count_sub, src):
        next = str(next)
        self.logger.info("DiscoveryMW::propagateIsReady: building dht request to send to {} from src {}".format(self.hash_to_ip[str(next)]['id'], self.hash_to_ip[str(src)]['id']))
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_ISREADY_DHT
        isready_req_dht = discovery_pb2.IsReadyReqDHT()
        isready_req_dht.count_pub = count_pub
        isready_req_dht.count_sub = count_sub
        isready_req_dht.src = src
        disc_req.isready_req_dht.CopyFrom(isready_req_dht)
        self.logger.debug("DiscoveryMW::propagateIsReaedy: done building dht request")
        buf2send = disc_req.SerializeToString()
        self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
        if (next not in self.send_buffer):
            self.logger.debug("DiscoveryMW::propagateIsReady: next not in send_buffer, initializing empty")
            self.send_buffer[next] = []
        
        if (self.send_buffer[next] == []):
            self.req[next].send(buf2send)
            self.logger.info("DiscoveryMW::propagateIsReady: done sending dht request")
        else:
            self.logger.info("DiscoveryMW::propagateIsReady: socket busy, queueing dht request")
        self.send_buffer[next].append(buf2send)


    def propagateLookup(self, next, reg_req_dht, end, dest, src):
        next = str(next)
        self.logger.info("DiscoveryMW::propagateLookup: building dht request")
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC_DHT
        lookup_req_dht = discovery_pb2.LookupPubByTopicReqDHT()
        lookup_req_dht.topiclist = reg_req_dht.topic
        lookup_req_dht.end = end
        lookup_req_dht.dest = dest
        lookup_req_dht.src = src

        disc_req.lookup_req_dht.CopyFrom(lookup_req_dht)
        self.logger.debug("DiscoveryMW::propagateLookup: done building dht request")
        buf2send = disc_req.SerializeToString()
        self.logger.debug("Stringified serialized buffer = {}".format(buf2send))
        if (next not in self.send_buffer):
            self.logger.debug("DiscoveryMW::propagateLookupyy: next not in send_buffer, initializing empty")
            self.send_buffer[next] = []
        
        if (self.send_buffer[next] == []):
            self.req[next].send(buf2send)
            self.logger.info("DiscoveryMW::propagateLookupyy: done sending dht request")
        else:
            self.logger.info("DiscoveryMW::propagateLookupyy: socket busy, queueing dht request")
        self.send_buffer[next].append(buf2send)

