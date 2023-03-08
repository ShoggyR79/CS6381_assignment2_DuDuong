# Du Duong
# the purpose of this script is to generate finger table for each node in the network
# based on the dht.json file created by the exp_generator.py script

import os
import random  # random number generation
import hashlib  # for the secure hash library
import argparse  # argument parsing
import json  # for JSON
import logging  # for logging. Use it in place of print statements.
# import collection
from collections import OrderedDict


class TableGenerator():
    def __init__(self, logger):
        self.int_to_node = None
        self.dht = None
        self.successor = None
        self.predecessor = None
        self.bits_hash = None
        self.N = None
        self.logger = logger
    def configure(self, args):
        self.logger.debug("TableGenerator::configure")
        self.dht = self.getDht()
        self.int_to_node = self.getIntToNode()
        self.bits_hash = args.bits_hash
        self.N = 2**(self.bits_hash)
    def getDht(self):
        file = open("dht.json")
        dht = json.load(file)
        return dht['dht']
    def getIntToNode(self):
        table_num = {}
        # each hash table is a number
        for item in self.dht:
            table_num[item['hash']] = item
        return table_num
    def findSuccessor(self, start):
        # if we loop through the disctionary, we will have increasing hash values
        for hash in sorted(self.int_to_node.keys()):
            # print(hash, start)
            if hash > start:
                return hash
        return list(sorted(self.int_to_node.keys()))[0]
    def generateJson(self):
        output = {}
        for node in self.dht:
            output[node['hash']] = []
            for index in range(0, self.bits_hash):
                start_key = (node['hash'] + 2**(index)) % self.N
                output[node['hash']].append(self.findSuccessor(start_key))
        with open("finger_table.json", "w") as outfile:
            sorted_output = OrderedDict(sorted(output.items()))
            json.dump(sorted_output, outfile)
        outfile.close()
    def generateHashToIp(self):
        output = {}
        for node in self.dht:
            output[node['hash']] = node
        with open("hash_to_ip.json", "w") as outfile:
            json.dump(output, outfile)
        outfile.close()
    def driver(self):
        self.logger.debug("TableGenerator::driver")
        self.generateJson()
        self.generateHashToIp()

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="FingerTableGen")
    parser.add_argument ("-b", "--bits_hash", type=int, choices=[8,16,24,32,40,48,56,64], default=48, help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")
    parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")

    return parser.parse_args()

def main():
    try:
        
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("TableGenerator")

        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs ()

        # reset the log level to as specified
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

          # Obtain the test object
        logger.debug ("Main: obtain the tablegenerator object")
        tab_obj = TableGenerator(logger)

        # configure the object
        logger.debug ("Main: configure the generator object")
        tab_obj.configure (args)

        # now invoke the driver program
        logger.debug ("Main: invoke the driver")
        tab_obj.driver ()

    except Exception as e:
        raise e


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
