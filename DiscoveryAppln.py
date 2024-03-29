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
        self.register_jobs = None  # dictionary of jobs to be registered
        self.register_job_status = None  # dictionary of register job status
        self.register_buffer = None
        self.lookup_jobs = None
        self.lookup_jobs_result = None
        self.job_id = 0
        self.bits_hash = 0

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln::configure")
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.exp_publishers = args.exp_publishers
            self.exp_subscribers = args.exp_subscribers
            self.topics_to_publishers = {}
            self.publisher_to_ip = {}
            self.register_jobs = {}  # dictionary of jobs to be registered
            self.register_job_status = {}  # dictionary of register job status
            self.register_buffer = {}
            self.lookup_jobs = {}
            self.lookup_jobs_result = {}
            self.bits_hash = args.bits_hash
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
            file_dht = open(args.jsonfile)
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
            self.mw_obj.connectTable(self.table, self.hash)
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


    def handle_register(self, reg_req, chain):
        try:
            if (reg_req.role == discovery_pb2.ROLE_PUBLISHER or reg_req.role == discovery_pb2.ROLE_BOTH):
                if self.lookup == "Distributed":
                    self.logger.info("DiscoveryAppln::handle_register building requests and sending them to chord")
                    self.register_jobs[reg_req.info.id] = 0
                    for topic in reg_req.topiclist:
                        node_hash = self.hash_func(topic)
                        register_req_dht = discovery_pb2.RegisterReqDHT()
                        # self.logger.info("role")
                        register_req_dht.role = (reg_req.role)
                        # self.logger.info("info")

                        register_req_dht.info.CopyFrom(reg_req.info)
                        # self.logger.info("topiclist")

                        register_req_dht.topiclist[:] = reg_req.topiclist
                        # self.logger.info("end")

                        register_req_dht.end = False
                        # self.logger.info("dest")

                        register_req_dht.dest = node_hash
                        # self.logger.info("src")
                        register_req_dht.src = self.hash
                        # self.logger.info("incrementing job")
                        if (reg_req.info.id not in self.register_jobs):
                            self.register_jobs[reg_req.info.id] = 0
                        
                        self.register_jobs[reg_req.info.id] += 1
                        self.register_chord(register_req_dht, chain)
                    self.logger.info("DiscoveryAppln::handle_register sent {} requests".format(self.register_jobs[reg_req.info.id]))
                    self.register_job_status[reg_req.info.id] = True                     
                else:
                    self.handle_register_dht(reg_req)
                self.logger.info("DiscoveryAppln::handle_register increment number of seen publishers")

                self.count_publishers += 1
                return None
                 
            elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
                self.logger.info("DiscoveryAppln::handle_register increment number of seen subscribers")
                self.count_subscribers += 1
                self.mw_obj.register_reply(discovery_pb2.STATUS_SUCCESS, chain)
                return None
        except Exception as e:
            raise e
    # program to handle incoming register request.
    def handle_register_dht(self, reg_req, chain = None):
        ''' handle register request'''
        try:
            self.logger.info("DiscoveryAppln::handle_register endpoint")
            if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
                req_info = reg_req.info
                self.logger.info("DiscoveryAppln::handle_register checking if name is unique")

                if (req_info.id in self.publisher_to_ip):
                    # already registered
                    self.logger.info("DiscoveryAppln::handle_register already registered")
                    if (self.lookup == 'Centralized'):
                        raise Exception("Id must be unique")
                    else:
                        self.mw_obj.register_reply_dht( discovery_pb2.STATUS_SUCCESS, reg_req.info.id, reg_req.src, "", self.hash, chain)
                        return None
                self.logger.info("DiscoveryAppln::handle_register assigning user id to info")
                self.publisher_to_ip[req_info.id] = req_info
                self.logger.info("DiscoveryAppln::handle_register adding topics to publishers")
                for topic in reg_req.topiclist:
                    if topic not in self.topics_to_publishers:
                        self.topics_to_publishers[topic] = []
                    self.topics_to_publishers[topic].append(req_info.id)
                
            elif (reg_req.role == discovery_pb2.ROLE_BOTH and self.dissemination == "Broker"):
                self.logger.info("DiscoveryAppln::handle_register broker registered and saved")
                self.broker = reg_req.info
            if (self.lookup == 'Centralized'):
                self.mw_obj.register_reply( discovery_pb2.STATUS_SUCCESS, chain)
            else:
                self.mw_obj.register_reply_dht( discovery_pb2.STATUS_SUCCESS, reg_req.info.id, reg_req.src, "", self.hash, chain)
            return None
        except Exception as e:
            raise e
    def handle_register_reply_dht(self, reg_resp, chain = None):
        # if we are at the original source, accumulate the status
        self.logger.info("DiscoveryAppln::handle_register_reply_dht received reply from node {}".format(reg_resp.cur))
        if (self.hash == reg_resp.src):
            self.logger.info("DiscoveryAppln::handle_register_reply_dht reply at source")
            # decrement value by 1
            self.register_jobs[reg_resp.id] -= 1
            self.register_job_status[reg_resp.id] =  self.register_job_status[reg_resp.id] and reg_resp.status
            self.logger.debug("DiscoveryAppln::handle_register_reply_dht {} requests left".format(self.register_jobs[reg_resp.id]))
            # send reply if value is 0
            if (self.register_jobs[reg_resp.id] == 0):
                self.logger.info("DiscoveryAppln::handle_register_reply_dht all requests completed, sending reply to source")
                self.mw_obj.register_reply(self.register_job_status[reg_resp.id], chain)
        # else propagate the reply
        else:
            self.logger.info("DiscoveryAppln::handle_register_reply_dht propagating reply to source")
            self.mw_obj.register_reply_dht(reg_resp.status, reg_resp.id, reg_resp.src, reg_resp.reason, self.hash, chain)
        return None
    def register_chord(self, reg_req, chain):
        id = reg_req.dest
        self.logger.info("DiscoveryAppln::register_chord received chord request for hash {}".format(id))
        endHash = 0
        status = False
        if self.hash < id and id <= self.table[0]:
            endHash = self.table[0]
            status = True
        else:
            n0 = self.closest_preceding_node(id)
            endHash =  n0
            status = False
        
        self.logger.info("DiscoveryAppln::register_chord - propagating lookup request to node {} ".format(endHash))
        self.mw_obj.propagateRegister(endHash, reg_req, status, reg_req.dest, reg_req.src, chain)
        return None        
        


    def isready_request(self, chain = None):
        if (self.lookup == "Distributed"):
            ready_req = discovery_pb2.IsReadyReqDHT()
            ready_req.count_sub = 0
            ready_req.count_pub = 0
            ready_req.src = self.hash
            self.isready_loop(ready_req, chain)
        else:
            self.logger.debug("DiscoveryAppln::isready_request")
            self.logger.info ("     Expected Subscribers: {}".format (self.exp_subscribers))
            self.logger.info ("     Expected Publishers: {}".format (self.exp_publishers))
            self.logger.info ("     Count Subscribers: {}".format (self.count_subscribers))
            self.logger.info ("     Count Publishers: {}".format (self.count_publishers))
            status = (self.state == self.State.ISREADY and (self.dissemination!="Broker" or self.broker != None))
            return self.mw_obj.is_ready_reply(status)
        return None
    def isready_loop(self, isready_req, chain):
        try:
            total_pub = self.count_publishers + isready_req.count_pub
            total_sub = self.count_subscribers + isready_req.count_sub
            src = isready_req.src
            self.logger.debug("DiscoveryAppln::isready_loop src = {}".format(src))
            self.logger.debug("DiscoveryAppln::isready_loop current node = {}".format(self.hash))
            if (self.table[0] == src):
                self.logger.info("DiscoveryAppln::isready_loop at source")
                # checks if total pub and sub is equal to expected
                self.logger.info("seen publishers: {}, expected publishers: {}".format(total_pub, self.exp_publishers))
                self.logger.info("seen subscribers: {}, expected subscribers: {}".format(total_sub, self.exp_subscribers))
                status = (total_pub == self.exp_publishers and total_sub == self.exp_subscribers)
                self.logger.info("DiscoveryAppln::isready_loop replying with status: {}".format(status))
                self.mw_obj.is_ready_reply_dht(status, src, self.hash, chain)
            else:
                self.mw_obj.propagateIsReady(self.table[0], total_pub, total_sub, src, chain)
            return None
        except Exception as e:
            raise e
    def handle_isready_reply_dht(self, isready_resp, chain):
        # if we are at the original source, accumulate the status
        self.logger.info("DiscoveryAppln::handle_isready_reply_dht received reply from node {}".format(isready_resp.cur))
        if (self.hash == isready_resp.src):
            self.logger.info("DiscoveryAppln::handle_isready_reply_dht sending source reply of status {}".format(isready_resp.status))
            self.mw_obj.is_ready_reply(isready_resp.status, chain)
        else:
            self.logger.info("DiscoveryAppln::handle_isready_reply_dht propagating reply to source")
            chain.pop()
            self.mw_obj.is_ready_reply_dht(isready_resp.status, isready_resp.src, self.hash, chain)
        return None
        
        
    
    def handle_lookup(self, lookup_req, from_broker, chain = None):
        try:
            if self.lookup == "Distributed":
                
                self.logger.info("DiscoveryAppln::handle_lookup building lookup request for job id {}".format(self.job_id))
                # build lookup chords
                self.lookup_jobs[self.job_id] = 0
                for topic in lookup_req.topiclist:
                    node_hash = self.hash_func(topic)
                    discovery_pb2.DiscoveryReq()
                    lookup_req_dht = discovery_pb2.LookupPubByTopicReqDHT()
                    lookup_req_dht.topiclist[:] = [topic]
                    lookup_req_dht.end = False
                    lookup_req_dht.dest = node_hash
                    lookup_req_dht.src = self.hash
                    lookup_req_dht.jobid = self.job_id
                    lookup_req_dht.from_broker = from_broker
                    self.lookup_jobs[self.job_id] += 1
                    self.lookup_chord(lookup_req_dht, chain)
                self.logger.info("DiscoveryAppln::handle_lookup built {} lookup requests for job id {}".format(self.lookup_jobs[self.job_id], self.job_id))
                self.lookup_jobs_result[self.job_id] = []
                self.job_id+=1
            else:
                self.lookup_pub_by_topic_request(lookup_req, from_broker)
            return None
        except Exception as e:
            raise e
    def lookup_pub_by_topic_request(self, lookup_req, from_broker, chain = None):
        try:
            publist = []
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reqest")
            topiclist = lookup_req.topiclist
            if (self.dissemination == "Broker" and  not from_broker):
                publist.append(self.broker)
                return self.mw_obj.lookup_pub_by_topic_reply(publist, chain)
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
            if self.lookup == "Distributed":
                self.mw_obj.lookup_pub_by_topic_reply_dht(publist, lookup_req.src, self.hash, lookup_req.jobid, chain)
            else:
                self.mw_obj.lookup_pub_by_topic_reply(publist, chain)
            return None
        except Exception as e:
            raise e
    def lookup_pub_by_topic_reply_dht(self, lookup_resp, chain=None):
         # if we are at the original source, accumulate the status
        if (lookup_resp.src and self.hash == lookup_resp.src):
            # decrement value by 1
            self.lookup_jobs[lookup_resp.jobid] -= 1
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reply_dht received reply from node {}".format(lookup_resp.cur))
            for pub in lookup_resp.publist:
                self.lookup_jobs_result[lookup_resp.jobid].append(pub)
            # send reply if value is 0
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reply_dht job id {} has {} replies left".format(lookup_resp.jobid, self.lookup_jobs[lookup_resp.jobid]))
            if (self.lookup_jobs[lookup_resp.jobid] == 0):
                self.logger.info("DiscoveryAppln::lookup_pub_by_topic_reply_dht sending source reply")
                self.mw_obj.lookup_pub_by_topic_reply(self.lookup_jobs_result[lookup_resp.jobid], chain)
        # else propagate the reply
        else:
            self.mw_obj.lookup_pub_by_topic_reply_dht(lookup_resp.status, chain)
        return None
    def lookup_chord(self, lookup_req, chain):
        id = lookup_req.dest
        self.logger.info("DiscoveryAppln::lookup_chord - lookup request for id {}".format(id))
        endHash = 0
        status = False
        self.logger.info("DiscoveryAppln::lookup_chord - checking next node in chord ring")
        print(self.table)
        if self.hash < id and id <= self.table[0]:
            endHash = self.table[0]
            status = True
        else:
            self.logger.info("DiscoveryAppln::lookup_chord - finding closest preceding node")
            n0 = self.closest_preceding_node(id)
            endHash =  n0
            status = False
        self.logger.info("DiscoveryAppln::lookup_chord - propagating lookup request to node {}".format(endHash))
        self.mw_obj.propagateLookup(endHash, lookup_req, lookup_req.end, lookup_req.dest, lookup_req.src, lookup_req.jobid, lookup_req.from_broker, chain)
        return None 
        
        
    # search the local finger table to find the closest preceding node
    def closest_preceding_node(self, id):
        self.logger.info("DiscoveryAppln::closest_preceding_node - searching for closest preceding node for id {}".format(self.hash))
        # loop from the back of finger table to front
        for i in range(len(self.table)-1 , -1 , -1):
            # self.logger.debug("cpn1")
            if self.hash > id and self.table[i] < self.hash and self.table[i] > id:
                self.logger.info("DiscoveryAppln::closest_preceding_node1 - found closest preceding node {}".format(self.table[i]))
                return self.table[i]
            # self.logger.debug("cpn2")
 
            if self.hash < id and self.table[i] > self.hash and self.table[i] < id:
                self.logger.info("DiscoveryAppln::closest_preceding_node2 - found closest preceding node {}".format(self.table[i]))
                return self.table[i]
            # self.logger.debug("cpn3")

            if self.hash > id and (self.table[i] > self.hash or self.table[i] < id):
                self.logger.info("DiscoveryAppln::closest_preceding_node3 - found closest preceding node {}".format(self.table[i]))
                return self.table[i]
        self.logger.info("DiscoveryAppln::closest_preceding_node - no closest preceding node found")
        return self.hash
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
        return (hash_val)


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

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    
    parser.add_argument("-j", "--jsonfile", default="dht.json", help="JSON file for configuration")

    parser.add_argument ("-b", "--bits_hash", type=int, choices=[8,16,24,32,40,48,56,64], default=48, help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")


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
