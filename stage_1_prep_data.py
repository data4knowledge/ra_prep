import yaml
import os
import csv
import copy
from uuid import uuid4

id_number = 1
uuid_to_id = {}
nodes = { "RegistrationAuthority": [], "Namespace": [] }
relationships = { "MANAGES": [] }
repeat = {}

def process_nodes(node_set, ra_uri, ns_uri):
  for ra in node_set:
    ra_record = ra
    ra_record = copy.deepcopy(ra)
    ra_record.pop('namespaces')
    ra_record['uri'] = "%s%s" % (ra_uri, ra_record['name'].replace(' ', '-').lower()) 
    ra_record['uuid'] = str(uuid4())
    nodes["RegistrationAuthority"].append(ra_record)
    for ns in ra['namespaces']:
      ns_record = ns
      ns_record['uri'] = "%s%s" % (ns_uri, ns_record['name'].replace(' ', '-').lower()) 
      ns_record['uuid'] = str(uuid4())
      nodes["Namespace"].append(ns_record)
      relationships["MANAGES"].append({"from": ra_record['uri'], "to": ns_record['uri']})

with open("source_data/ra_and_ns.yaml") as file:
    model = yaml.load(file, Loader=yaml.FullLoader)
    base_uri = "http://ra.d4k.dk/"
    ns_base_uri = "%sdataset/ns/" % (base_uri)
    ra_base_uri = "%sdataset/ra/" % (base_uri)
    process_nodes(model['registration_authorities'], ra_base_uri, ns_base_uri)

def delete_dir(dir_path):
    target_dir = "load_data"
    files = os.listdir(target_dir)
    for f in files:
      os.remove("%s/%s" % (target_dir, f))
      print("Deleted %s" % (f))

def write_nodes(the_data, csv_filename, id_field="id:ID"):
  if len(the_data) == 0:
    return 
  global id_number
  with open(csv_filename, mode='w', newline='') as csv_file:
    fields = list(the_data[0].keys())
    fieldnames = [id_field] + fields
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writeheader()
    for row in the_data:
      row[id_field] = id_number
      uuid_to_id[row["uri"]] = id_number
      id_number += 1
      writer.writerow(row)

def write_relationships(the_data, csv_filename, id_field="id:ID"):
  if len(the_data) == 0:
    return 
  global id_number
  with open(csv_filename, mode='w', newline='') as csv_file:
    fieldnames = [ ":START_ID", ":END_ID" ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writeheader()
    for row in the_data:
      new_row = { ":START_ID": uuid_to_id[row["from"]], ":END_ID": uuid_to_id[row["to"]] }
      writer.writerow(new_row)

delete_dir("load_data")

for k, v in nodes.items():
  csv_filename = "load_data/node-%s-1.csv" % (k.lower())
  write_nodes(v, csv_filename)

for k, v in relationships.items():
  csv_filename = "load_data/relationship-%s-1.csv" % (k.lower())
  write_relationships(v, csv_filename)
