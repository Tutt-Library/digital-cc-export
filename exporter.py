"""Exports objects and generates CSV files for metadata at the collection  from
 Fedora 3 repository.
"""
__author__ = "Jeremy Nelson"

import click
import csv
import datetime
import logging
import os
import requests
import pathlib
import string
import sys
import lxml.etree as etree
from rdflib import Namespace, RDF

import config

DC = Namespace("http://purl.org/dc/elements/1.1/")
FEDORA_ACCESS = Namespace("http://www.fedora.info/definitions/1/0/access/")
FEDORA = Namespace("info:fedora/fedora-system:def/relations-external#")
FEDORA_MODEL = Namespace("info:fedora/fedora-system:def/model#")
ISLANDORA = Namespace("http://islandora.ca/ontology/relsext#")

NS = { "foxml": "info:fedora/fedora-system:def/foxml#",
       "access": str(FEDORA_ACCESS),
       "mods": "http://www.loc.gov/mods/v3",
       "obj": "http://www.fedora.info/definitions/1/0/access/"}

etree.register_namespace("fedora", str(FEDORA))
etree.register_namespace("fedora-model", str(FEDORA_MODEL))
etree.register_namespace("islandora", str(ISLANDORA))

logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(filename="production.log",
                    level=logging.INFO)

def format_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ','_') # I don't like spaces in filenames.
    return filename


def get_filename(pid):
    object_url = f"{config.REST_URL}{pid}?format=xml"
    object_result = requests.get(object_url)
    label = None
    try:
        object_xml = etree.XML(object_result.text.encode())
        label = object_xml.find("obj:objLabel", namespaces=NS)
    except etree.XMLSyntaxError:
        click.echo(f"{pid} Object XML Syntax Error")
    if label is not None:
        return format_filename(label.text)
    else:
        return pid.replace(":", "_")

def collection_objects(pid):
    sparql = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}""".format(pid)
    children_response = requests.post(
        config.RI_URL,
        data={"type": "tuples",
              "lang": "sparql",
              "format": "json",
               "query": sparql},
        auth=config.FEDORA_AUTH)
    click.echo(children_response.json())

class MODStoCSV(object):

    def __init__(self, rest_url, pid):
        # Extract MODS XML Datastream
        mods_url = "{}{}/datastreams/MODS/content".format(
            est_url,
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

    def __init__(self):
        """Initializes an instance of the IndexerClass"""
        self.auth = config.FEDORA_AUTH
        self.logger = logging.getLogger(__file__)
        self.rest_url = config.REST_URL
        self.ri_search = config.RI_URL
        self.current_directory = pathlib.Path(config.EXPORT_DIR)
        self.dublin_core = {}
        self.mods = {}

    def __add_dc_row__(self, pid, collection_pid, dc_url):
        dc_result = requests.get(dc_url)
        if dc_result.status_code > 399:
            return
        dc_result.encoding = 'utf-8'
        dc_xml = etree.XML(dc_result.content)
        dc_dict = { "pid": pid }
        for child in dc_xml.iter():
            if child.text is None:
                continue
            column = f"{child.tag}".replace(r"{http://purl.org/dc/elements/1.1/}", "dc:")
            if column in dc_dict:
                try:
                    counter = int(column[-1])
                    column = f"{column[:-1]}{counter + 1}"
                except ValueError:
                    column = f"{column}1"
            dc_dict[column] = child.text
            self.dublin_core[collection_pid]['fields'].append(column)
        self.dublin_core[collection_pid]['rows'].append(dc_dict)

    def __add_mods_row__(self, pid, collection_pid, mods_url):
        mods_result = requests.get(mods_url)
        if mods_result.status_code > 399:
            return
        mods_result.encoding = 'utf-8'
        try:
            mods_xml = etree.XML(mods_result.content)
        except etree.XMLSyntaxError:
            click.echo(f"{pid} MODS {mods_url} XML Syntax Error")
            return
        mods_dict = {}
        for child in mods_xml.iter():
            if child.text is None or len(child.text.strip()) < 1:
                continue
            column = ''
            ancestors = [ancestor for ancestor in child.iterancestors()]
            ancestors.reverse()
            for ancestor in ancestors:
                column += f"{ancestor.tag}".replace(r"{http://www.loc.gov/mods/v3}", "mods:")
                if 'type' in ancestor.attrib:
                    column += f"[type={ancestor.attrib.get('type')}]"
                column += " > " 
            column += f"{child.tag}".replace(r"{http://www.loc.gov/mods/v3}", "mods:")
            if 'type' in child.attrib:
                column += f"[type={child.attrib.get('type')}]"
            if column in mods_dict:
                # Extracts last character, if int, increment column by 1
                try:
                    current = int(column[-1])
                    column = f"{column[:-1]}{current + 1}"
                except ValueError:
                    column = f"{column}1"
            mods_dict[column] = child.text
            self.mods[collection_pid]['fields'].append(column)
        mods_dict['pid'] = pid
        self.mods[collection_pid]['rows'].append(mods_dict)
        title = mods_xml.find("mods:titleInfo/mods:title", namespaces=NS)
        if title is None:
            return
        return title.text


    def __export_datastreams__(self, pid, current_directory, is_collection=False):
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
            click.echo(f"{ds_pid_url} returns {result.status_code}")
            raise ExporterError(
                f"Failed to retrieve datastreams for {pid}",
                f"Code {result.status_code} for url {ds_pid_url} \nError {result.text}")
        result_xml = etree.XML(result.content)
        datastreams = result_xml.findall("access:datastream", namespaces=NS)
        if is_collection:
            pid_directory = current_directory
        else:
            pid_directory = current_directory/pid.replace(":","_")
        pid_directory.mkdir(parents=True, exist_ok=True)
        title = get_filename(pid)
        for row in datastreams:
            dsid = row.attrib.get("dsid")
            label = row.attrib.get('label')
            ds_url = f"{self.rest_url}{pid}/datastreams/{dsid}/content"
            file_ext = self.__generate_ext__(row.attrib.get('mimeType'))
            file_name = dsid
            if dsid.startswith("DC") or dsid.startswith("MODS"):
                continue
            if dsid.startswith("TN"):
                file_name = "thumbnail"
            if dsid.startswith("OBJ"):
                if title:
                    file_name = title
                elif len(label) > 0:
                    file_name = label
                else:
                    file_name = dsid
            file_name = format_filename(file_name).replace(".jpg", "").replace(".mp3", "")
            file_path = pid_directory/f"{file_name}{file_ext}"
            file_response = requests.get(ds_url, stream=True)
            if file_response.status_code > 300:
                continue
            with open(file_path.absolute(), 'wb+') as ds:
                for chunk in file_response.iter_content(chunk_size=10000):
                     ds.write(chunk)
            logging.info(f"{datetime.datetime.utcnow()} {ds_url} exported size {file_response.headers.get('Content-Length')} {file_path.absolute()}")

 
    def __export_compound__(self, pid, parent_path):
        """Internal method takes a parent PID and exports all children.
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
        current_path = parent_path/pid.replace(":", "_")
        for row in result.json().get('results'):
            constituent_pid = row.get('s').split("/")[-1]
            pid_as_ds = self.__process_constituent__(constituent_pid, current_path)
            if pid_as_ds is not None:
                output.extend(pid_as_ds)
        return output

    def __generate_ext__(self, mime_type):
        last_ = mime_type.split("/")[-1]
        return f".{last_.split('+')[-1]}"

    def __generate_csv__(self, path, collection_pid):
        if path.name.startswith("mods"):
            metadata = self.mods[collection_pid]['rows']
            fieldnames = list(set(self.mods[collection_pid]['fields']))
        elif path.name.startswith("dublin_core"):
            metadata = self.dublin_core[collection_pid]['rows']
            fieldnames = list(set(self.dublin_core[collection_pid]['fields']))
        else:
            return
        if len(fieldnames) < 2:
            return
        with path.open("w", encoding='utf-8', newline='') as fo:
            csv_writer = csv.DictWriter(fo, fieldnames=fieldnames)
            csv_writer.writeheader()

            for row in metadata:
                csv_writer.writerow(row)


    def __generate_metadata__(self, path, collection_pid):
        if len(self.mods) > 1:
            self.__generate_csv__(path/"mods.csv", collection_pid)
            logging.info(f"{datetime.datetime.utcnow()} MODS CSV {path}/mods.csv generated for {collection_pid}")
        if len(self.dublin_core) > 1:
            self.__generate_csv__(path/"dublin_core.csv", collection_pid)
            logging.info(f"{datetime.datetime.utcnow()} Dublin Core CSV {path}/dublin_core.csv generated for {collection_pid}")
 

    def __get_child_metadata__(self, child_pid, collection_pid):
        dc_url = f"{self.rest_url}{child_pid}/datastreams/DC/content"
        mods_url = f"{self.rest_url}{child_pid}/datastreams/MODS/content"
        self.__add_dc_row__(child_pid, collection_pid, dc_url)
        self.__add_mods_row__(child_pid, collection_pid, mods_url)
        

    def __process_constituent__(self, pid, parent_path):
        """Export constituent PID and returns dictionary compatible with datastream

		Args:
		    pid -- PID
        """
        self.__export_datastreams__(pid, parent_path)
       

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
        return etree.XML(rels_ext_result.content)

    def export_pid(self, pid, parent_path):
        """Method retrieves MODS and any PDF datastreams and indexes
        into repository's Elasticsearch instance

        Args:
            pid: PID to index
            parent_path: path of parent            
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
        click.echo(f"{pid} ", nl=False)

        # Checks if pid has a compound model
        cmp_xpath = "rdf:Description/fedora-model:hasModel"
        models = rels_ext.findall(cmp_xpath, namespaces={ "rdf": str(RDF),
                                                          "fedora-model": str(FEDORA_MODEL)})
        for model in models:
            if model.attrib["{{{0}}}resource".format(RDF)] == 'info:fedora/islandora:compoundCModel':
                self.__export_compound__(pid, parent_path)
                return
        self.__export_datastreams__(pid, parent_path)
        logging.info(f"{datetime.datetime.utcnow()} {pid} exported")

    def export_collection(self, pid, parent_path):
        """Method takes a parent collection PID, retrieves all children, and
        iterates through and export all pids

        Args:
            pid -- Collection PID
            parent_path -- Path of parent

        """
        sparql = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}""".format(pid)
        started = datetime.datetime.utcnow()
        msg = "Started exporting collection {} at {}".format(
            pid,
            started.isoformat())
        click.echo(msg)
        self.mods[pid] = { "fields": ['pid'],
                           "rows": [] }
        self.dublin_core[pid] = { "fields": ['pid'],
                                  "rows": [] }
        children_response = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql},
            auth=self.auth)
        collection_path = get_filename(pid)        
        current_directory = parent_path/collection_path
        # Export any collection datastreams
        self.__export_datastreams__(pid, current_directory, True)
        if children_response.status_code < 400:
            children = children_response.json().get('results')
            for row in children:
                iri = row.get('s')
                child_pid = iri.split("/")[-1]
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
                self.__get_child_metadata__(child_pid, pid)
                if len(is_collection_result.json().get('results')) > 0:
                    self.export_collection(child_pid, current_directory)
                else:
                    self.export_pid(child_pid, current_directory)
        else:
            err_title = "Failed to export collection PID {}, error {}".format(
                pid,
                children_response.status_code)
            logging.error(err_title)
            raise ExporterError(
                err_title,
                children_response.text)
        self.__generate_metadata__(current_directory, pid)
        # Clear collection pid 
        self.dublin_core.pop(pid)
        self.mods.pop(pid)
        end = datetime.datetime.utcnow()
        msg = "\nExport done {} at {}, total object {} total time {}".format(
            pid,
            end.isoformat(),
            len(children),
            (end-started).seconds / 60.0)
        logging.info(msg)
        click.echo(msg)

class ExporterError(Exception):
    """Base for any errors indexing Fedora 3.x objects into Elasticsearch"""

    def __init__(self, title, description):
        """Initializes an instance of IndexerError

	    Args:
	       title -- Title for Error
		   description -- More detailed information about the exception
        """
        super(ExporterError, self).__init__()
        self.title = title
        self.description = description

    def __str__(self):
        """Returns string representation of the object using the instance's
		title"""
        return repr(self.title)

@click.command()
@click.option("--collection", help="Pid of collection")
@click.option("--export", help="Export directory")
def run(collection, export):
    start = datetime.datetime.utcnow()
    click.echo(f"Digital CC Fedora 3.8 Exporter\nStarted at {start}")
    exporter = Exporter()
    if collection == None:
        collection = config.INITIAL_PID
    if export == None:
        export = config.EXPORT_DIR
    exporter.export_collection(collection, pathlib.Path(export))
    #exporter.export_collection("coccc:10504", pathlib.Path(config.EXPORT_DIR))
    #exporter.export_pid('coccc:10463', pathlib.Path("E:\export"))
    #collection_objects(config.INITIAL_PID)
    end = datetime.datetime.utcnow()
    click.echo(f"Finished at {end} total time {(end-start).seconds / 60.} minutes")

if __name__ == "__main__":
    run()
