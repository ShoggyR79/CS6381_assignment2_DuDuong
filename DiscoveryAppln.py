###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.
import json  # for JSON
import hashlib  # for the secure hash library


# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in


class DiscoveryAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        WAITING = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.count_publishers = 0
        self.count_subscribers = 0
        self.exp_publishers = 0
        self.exp_subscribers = 0
        self.mw_obj = None
        self.logger = logger
        self.lookup = None
        self.dissemination = None
        self.topics_to_publishers = None
        self.publisher_to_ip = None
        self.broker = None
        self.dht = None   # centralized hash table for all nodes
        self.hash = None    # the hash value of THIS node
        self.table = None   # finger table

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln::configure")
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.exp_publishers = args.exp_publishers
            self.exp_subscribers = args.exp_subscribers
            self.topics_to_publishers = {}
            self.publisher_to_ip = {}

            # get the configuration object
            self.logger.debug("DiscoveryAppln::configure - parsing {}".format(args.config))
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # setup underlying middleware object
            self.logger.debug(
                "DiscoveryAppln::configure - setup underlying middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            # pass remainder of the args to the m/w object
            
            # getting hash table
            self.logger.debug("DiscoveryAppln::configure - getting hash and finger table")
            file_dht = open("dht.json")
            dht = json.load(file_dht)
            self.dht = dht["dht"]
            self.mw_obj.configure(args)
            file_finger = open("finger_table.json")
            finger_json = json.load(file_finger)
            self.logger.debug("DiscoveryAppln::configure - generating table for node")
            for item in self.dht:
                if item["id"] == args.name:
                    self.hash = item["hash"]
                    self.table = finger_json[str(self.hash)]
                    break
            self.mw_obj.connectTable(self.table)
            self.logger.info("DiscoveryAppln::configure completed")

        except Exception as e:
            raise e

    def driver(self):
        ''' Driver program '''
        try:
            self.logger.info("DiscoveryAppln::driver")
            # dump contents
            self.dump()

            # setting upcall handle
            self.logger.debug("DiscoveryAppln::driver - setting upcall handle")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.WAITING

            self.mw_obj.event_loop(timeout=0)

            self.logger.info("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e

    def invoke_operation(self):
        ''' invoke operation depending on state '''
        try:
            self.logger.info("DiscoveryAppln::invoke_operation")
            if (self.state == self.State.WAITING or self.state == self.State.ISREADY):
                return None
            else:
                raise ValueError("undefined")
        except Exception as e:
            raise e


    def handle_regster(self, reg_req):
        try:
            if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
                if self.lookup == "Distributed":
                    node_hash = self.hash_func(reg_req.info.id + reg_req.info.ip + str(reg_req.info.port))
                    disc_req = discovery_pb2.DiscoveryReq()
                    disc_req.msg_type = discovery_pb2.TYPE_REGISTER_DHT
                    disc_req.register_req_dht.role = reg_req.role
                    disc_req.register_req_dht.info.CopyFrom(reg_req.info)
                    disc_req.register_req_dht.topiclist[:] = reg_req.topiclist
                    disc_req.register_req_dht.end = False
                    disc_req.register_req_dht.dest = node_hash
                else:
                    self.handle_register_dht(reg_req)
                return self.register_chord(disc_req)
                 
            elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
                self.logger.info("DiscoveryAppln::handle_register increment number of seen subscribers")
                self.count_subscribers += 1
        except Exception as e:
            raise e
    # program to handle incoming register request.
    def handle_register_dht(self, reg_req):
        ''' handle register request'''
        try:
            self.logger.info("DiscoveryAppln::handle_register")
            if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
                req_info = reg_req.info
                self.logger.info("DiscoveryAppln::handle_register checking if name is unique")

                if (req_info.id in self.publisher_to_ip):
                    raise Exception("Name should be unique")
                self.logger.info("DiscoveryAppln::handle_register assigning user id to info")
                self.publisher_to_ip[req_info.id] = req_info
                self.logger.info("DiscoveryAppln::handle_register adding topics to publishers")
                for topic in reg_req.topiclist:
                    if topic not in self.topics_to_publishers:
                        self.topics_to_publishers[topic] = []
                    self.topics_to_publishers[topic].append(req_info.id)
                self.logger.info("DiscoveryAppln::handle_register increment number of seen publishers")

                self.count_publishers += 1
            elif (reg_req.role == discovery_pb2.ROLE_BOTH and self.dissemination == "Broker"):
                self.logger.info("DiscoveryAppln::handle_register broker registered and saved")
                self.broker = reg_req.info

            self.mw_obj.register_reply( discovery_pb2.STATUS_SUCCESS)

        except Exception as e:
            raise e
    
    def register_chord(self, reg_req):
        id = reg_req.dest
        if self.hash < id and id <= self.table[0].hash:
            self.mw_obj.propagate(self.table[0], reg_req, True, id)
        else:
            n0 = self.closest_preceding_node(id)
            self.mw_obj.propagate(n0, reg_req, False, id)
        return None        
    def handle_register_dht_reply(self, reg_resp):
        self.mw_obj.register_reply(reg_resp.status)
    # program to handle isready request


    def isready_request(self):
        if (self.lookup == "Distributed"):
            ready_req = discovery_pb2.IsReadyReqDHT()
            ready_req.count_sub = 0
            ready_req.count_pub = 0
            ready_req.origin = self.hash
            self.isready_loop(ready_req)
        else:
            self.logger.debug("DiscoveryAppln::isready_request")
            self.logger.info ("     Expected Subscribers: {}".format (self.exp_subscribers))
            self.logger.info ("     Expected Publishers: {}".format (self.exp_publishers))
            self.logger.info ("     Count Subscribers: {}".format (self.count_subscribers))
            self.logger.info ("     Count Publishers: {}".format (self.count_publishers))
            status = (self.state == self.State.ISREADY and (self.dissemination!="Broker" or self.broker != None))
            return self.mw_obj.is_ready_reply(status)
    def isready_loop(self, isready_req):
        try:
            total_pub = self.count_publishers + isready_req.count_pub
            total_sub = self.count_subscribers + isready_req.count_sub
            origin = isready_req.origin
            if (self.table[0] == origin):
                # checks if total pub and sub is equal to expected
                status = (total_pub == self.exp_publishers and total_sub == self.exp_subscribers)
                self.mw_obj.is_ready_reply(status)
            else:
                self.mw_obj.propagateIsReady(self.table[0], total_pub, total_sub, origin)
            return None
        except Exception as e:
            raise e
    def handle_isready_reply_dht(self, isready_resp):
        if (isready_resp.status == True):
            self.state = self.State.ISREADY
        self.mw_obj.is_ready_reply(isready_resp.status)
        
        
    
    def lookup_pub_by_topic_request(self, topiclist, broker):
        try:
            publist = []
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reqest")
            if (self.dissemination == "Broker" and not broker):
                publist.append(self.broker)
                return self.mw_object.loopup_pub_by_topic_reply(publist)
            for topic in topiclist:
                self.logger.debug("DiscoveryAppln::lookup_pub_by_topic_request - topic: {}".format(topic))
                publishers = []
                if topic in self.topics_to_publishers:
                    publishers = self.topics_to_publishers[topic]
                for publisher in publishers:
                    self.logger.debug("DiscoveryAppln::lookup_pub_by_topic_request - publisher: {}".format(publisher))
                    pub_info = self.publisher_to_ip[publisher]
                    
                    publist.append(pub_info)
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reqest - returning publist")
            return self.mw_obj.lookup_pub_by_topic_reply(publist)
        except Exception as e:
            raise e
    #################
    # hash value
    #################
    def hash_func (self, id):
        self.logger.debug ("ExperimentGenerator::hash_func")

        # first get the digest from hashlib and then take the desired number of bytes from the
        # lower end of the 256 bits hash. Big or little endian does not matter.
        hash_digest = hashlib.sha256 (bytes (id, "utf-8")).digest ()  # this is how we get the digest or hash value
        # figure out how many bytes to retrieve
        num_bytes = int(self.bits_hash/8)  # otherwise we get float which we cannot use below
        hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes
        return hash_val


    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     Expected Subscribers: {}".format (self.exp_subscribers))
            self.logger.info ("     Expected Publishers: {}".format (self.exp_publishers))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e

    #implement the CHORD algorithm to find the successor of a hash value
    def send_to_successor(self, id, req):
        if self.hash < id and id <= self.table[0].hash:
            return self.mw_obj.propagate(self.table[0], req, True)
        else:
            n0 = self.closest_preceding_node(id)
            return self.mw_obj.propagate(n0, req, False)
    
    # search the local finger table to find the closest preceding node
    def closest_preceding_node(self, id):
        # loop from the back of finger table to front
        for i in range(self.m-1, -1, -1):
            if self.hash < self.table[i].hash and self.table[i].hash < id:
                return self.table[i]
            
    # send request around in O(n) manner
    def send_around(self, req):
        if (req.hash == self.table[0]):
            # send is_ready_reply
            status = (self.exp_publishers == self.count_publishers and self.exp_subscribers == self.count_subscribers)
            
            self.mw_obj.is_ready_reply(self.count_publishers, self.count_subscribers)
        else:
            # send chord request
            self.mw_obj.propagate(self.table[0], req, True)

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Application")

    parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")


    parser.add_argument("-cp", "--exp_publishers", type=int, default=1,
                        help="Number of publishers to be expected before ready")

    parser.add_argument("-cs", "--exp_subscribers", type=int, default=1,
                        help="Number of subscribers to be expected before ready")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this discovery to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5555,
                        help="Port number for underlying discovery ZMQ, default=5557")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="Configuration file, default=config.ini")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    return parser.parse_args()

# main program


def main():
    try:
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")  # get a child logger

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(
            logger.getEffectiveLevel()))

        # obatin a discovery application
        logger.debug("Main: obtain a discovery application object")
        dis_app = DiscoveryAppln(logger)

        # configure the object
        logger.debug("Main: configure the discovery application object")
        dis_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the discovery application driver")
        dis_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))


# main entry point
if __name__ == "__main__":
    # set underlying default logging capability
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s %(levelname)s - %(message)s')
    main()
