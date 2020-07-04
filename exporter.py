"""Exports objects and generates CSV files for metadata at the collection  from
 Fedora 3 repository.
"""
__author__ = "Jeremy Nelson"

import click
import datetime
import logging
import os
import requests
import sys
import xml.etree.ElementTree as etree
from copy import deepcopy
from rdflib import Namespace, RDF

DC = Namespace("http://purl.org/dc/elements/1.1/")
FEDORA_ACCESS = Namespace("http://www.fedora.info/definitions/1/0/access/")
FEDORA = Namespace("info:fedora/fedora-system:def/relations-external#")
FEDORA_MODEL = Namespace("info:fedora/fedora-system:def/model#")
ISLANDORA = Namespace("http://islandora.ca/ontology/relsext#")
etree.register_namespace("fedora", str(FEDORA))
etree.register_namespace("fedora-model", str(FEDORA_MODEL))
etree.register_namespace("islandora", str(ISLANDORA))

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("Elasticsearch").setLevel(logging.ERROR)


class WebPageHandler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)
        self.messages = []

    def emit(self, record):
        self.messages.append(self.format(record))

    def get_messages(self):
        return self.messages



class Exporter(object):
    """Exports DC and MODS metadata from   Fedora Repository 3.8"""

    def __init__(self, **kwargs):
        """Initializes an instance of the IndexerClass

		Keyword args:
		  auth -- Tuple of username and password to authenticate to Fedora,
			   defaults to Fedora's standard login credentials
                  rest_url -- REST URL for Fedora 3.x, defaults to Fedora
		  ri_url -- SPARQL Endpoint, defaults to Fedora's standard search URL

	"""
        self.auth = kwargs.get("auth")
        self.logger = logging.getLogger(__file__)
        self.rest_url = kwargs.get("rest_url")
        self.ri_search = kwargs.get("ri_url")
        

    def __export_datastreams__(self, pid, title=None):
        """Internal method takes a PID, queries Fedora to extract 
        datastreams, renames object if title exists, and sets the 
        objects to the 

        Args:
            pid -- PID
            title -- Title
        """
        ds_pid_url = f"{self.rest_url}{pid}/datastreams?format=xml"
        result = requests.get(ds_pid_url)
        if result.status_code > 399:
            raise ExporterError(
                f"Failed to retrieve datastreams for {pid}",
                f"Code {result.status_code} for url {ds_pid_url} \nError {result.text}")
        result_xml = etree.XML(result.text)
        for row in datastreams:
            file_name, file_ext = '', ''
            mime_type = row.attrib.get('mimeType')
            if mime_type.startswith("application/octet-stream"):
                file_ext = 'bin'
            else:
                file_ext = mime_type.split("/")[-1]
            # Download and save datastream
            if title:
                file_name = title
            else:
                file_name = row.attrib.get('label')
            file_name = file_name.replace(" ", "_")
            file_response = requests.get()
            with open(f"{pid_directory}/{file_name}.{file_ext}", "wb+") as ds_file:
                ds_file.write(file_response.data)


    def __index_compound__(self, pid):
        """Internal method takes a parent PID and exports  all children.
        Args:
            pid -- PID of parent Fedora object
        """
        output = []
        sparql = """SELECT DISTINCT ?s
WHERE {{
   ?s <fedora-rels-ext:isConstituentOf> <info:fedora/{0}> .
}}""".format(pid)
        result = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql},
            auth=self.auth)
        if result.status_code > 399:
            raise IndexerError(
                "Could not retrieve {} constituent PIDS".format(pid),
                "Error code {} for pid {}\n{}".format(
                    result.status_code,
                    pid,
                    result.text))
        for row in result.json().get('results'):
            constituent_pid = row.get('s').split("/")[-1]
            self.skip_pids.append(constituent_pid)
            pid_as_ds = self.__process_constituent__(constituent_pid)
            if pid_as_ds is not None:
                output.extend(pid_as_ds)
        return output

    def __process_constituent__(self, pid, rels_ext=None):
        """Export constituent PID and returns dictionary compatible with datastream

		Args:
		    pid -- PID
        """
        if not rels_ext:
            rels_ext = self.__get_rels_ext__(pid)
        xpath = "{{{0}}}Description/{{{1}}}isConstituentOf".format(
            RDF,
            FEDORA)
        isConstituentOf = rels_ext.find(xpath)
        parent_pid = isConstituentOf.attrib.get(
            "{{{0}}}resource".format(RDF)).split("/")[-1]
        xpath = "{{{0}}}Description/{{{1}}}isSequenceNumberOf{2}".format(
            RDF,
            ISLANDORA,
			parent_pid.replace(":","_"))
        self.__export_datastreams__(pid)
        return datastreams
       
        
    def __get_rels_ext__(self, pid):
        """Extracts and returns RELS-EXT base on PID

        Args:
            pid -- PID
        """
        rels_ext_url = "{}{}/datastreams/RELS-EXT/content".format(
            self.rest_url,
            pid)

        rels_ext_result = requests.get(rels_ext_url)
        if rels_ext_result.status_code > 399:
            raise ExporterError("Cannot get RELS-EXT for {}".format(pid),
                "Tried URL {} status code {}\n{}".format(
                    rels_ext_url,
                    rels_ext_result.status_code,
                    rels_ext_result.text))
        return etree.XML(rels_ext_result.text)

    def export_pid(self, pid, parent=None, inCollections=[]):
        """Method retrieves MODS and any PDF datastreams and indexes
        into repository's Elasticsearch instance

        Args:
            pid: PID to index
            parent: PID of parent collection, default is None
            inCollections: List of pids that this object belongs int, used for
			    aggregations.

        Returns:
            boolean: True if indexed, False otherwise
        """
        rels_ext = self.__get_rels_ext__(pid)
        xpath = "{{{0}}}Description/{{{1}}}isConstituentOf".format(
            RDF,
            FEDORA)
        is_constituent = rels_ext.find(xpath)
        # Skip and don't index if pid is a constituent of another compound 
	# object
        if is_constituent is not None:
            return False
        # Extract MODS XML Datastream
        mods_url = "{}{}/datastreams/MODS/content".format(
            self.rest_url,
            pid)
        mods_result = requests.get(
            mods_url,
            auth=self.auth)
        mods_result.encoding = 'utf-8'
        if mods_result.status_code > 399:
            err_title = "Failed to index PID {}, error={} url={}".format(
                pid,
                mods_result.status_code,
                mods_url)
            logging.error(err_title)
            # 404 error assume that MODS datastream doesn't exist for this
            # pid, return False instead of raising IndexerError exception
            if mods_result.status_code == 404:
                return False
            raise IndexerError(
                err_title,
                mods_result.text)
        try:
            if not isinstance(mods_result.text, str):
                mods_xml = etree.XML(mods_result.text.decode())
            else:
                mods_xml = etree.XML(mods_result.text)
        except etree.ParseError:
            msg = "Could not parse pid {}".format(pid)
            return False
        mods_body = mods2rdf(mods_xml)
        # Extract and process based on content model
        return False

    def index_collection(self, pid, parents=[]):
        """Method takes a parent collection PID, retrieves all children, and
        iterates through and indexes all pids

        Args:
            pid -- Collection PID
            parents -- List of all Fedora Object PIDs that pid is in the 
	               collection

        """
        sparql = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}""".format(pid)
        started = datetime.datetime.utcnow()
        msg = "Started indexing collection {} at {}".format(
            pid,
            started.isoformat())
        self.logger.info(msg)
        self.messages.append(msg)
        children_response = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql},
            auth=self.auth)
        if children_response.status_code < 400:
            children = children_response.json().get('results')
            for row in children:
                iri = row.get('s')
                child_pid = iri.split("/")[-1]
                child_parents = deepcopy(parents)
                child_parents.append(pid)
                self.index_pid(child_pid, pid, child_parents)
                is_collection_sparql = """SELECT DISTINCT ?o
WHERE {{        
  <info:fedora/{0}> <fedora-model:hasModel> <info:fedora/islandora:collectionCModel> .
  <info:fedora/{0}> <fedora-model:hasModel> ?o
}}""".format(child_pid)
                is_collection_result = requests.post(
                    self.ri_search,
                    data={"type": "tuples",
                          "lang": "sparql",
                          "format": "json",
                          "query": is_collection_sparql},
                    auth=self.auth)
                if len(is_collection_result.json().get('results')) > 0:
                    self.index_collection(child_pid, child_parents)
        else:
            err_title = "Failed to index collection PID {}, error {}".format(
                pid,
                children_response.status_code)
            logging.error(err_title)
            raise IndexerError(
                err_title,
                children_response.text)
        end = datetime.datetime.utcnow()
        msg = "Indexing done {} at {}, total object {} total time {}".format(
            pid,
            end.isoformat(),
            len(children),
            (end-started).seconds / 60.0)
        self.logger.info(msg)
        self.messages.append(msg)
        click.echo(msg)

    def reset(self):
        """Deletes existing repository index and reloads Map"""
        if self.elastic.indices.exists('repository'):
            self.elastic.indices.delete(index='repository')
            # Load mapping
            self.elastic.indices.create(index='repository', body=MAP)


class ExporterError(Exception):
    """Base for any errors indexing Fedora 3.x objects into Elasticsearch"""

    def __init__(self, title, description):
        """Initializes an instance of IndexerError

	    Args:
	       title -- Title for Error
		   description -- More detailed information about the exception
        """
        super(IndexerError, self).__init__()
        self.title = title
        self.description = description

    def __str__(self):
        """Returns string representation of the object using the instance's
		title"""
        return repr(self.title)
