from neo4j import GraphDatabase
from utility.service_environment import ServiceEnvironment

load_nodes = [
  { 'filename': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSBQKSIdj7TsBqFUF6CnJdZCpnOKiwWfZCRIR83sIfNDylfh6JH-Bzx_WYqvq1IGYJE8BMauLwUrMAB/pub?output=csv', 'label': 'Namespace' }, 
  { 'filename': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRtItBGKeYsgF-ifRZA2OJI7JA8dN0JI5LRqbGQteL-9nz55O5knt75GNeHB1V7QnH8Ud9YqVzMuE1H/pub?output=csv', 'label': 'RegistrationAuthority' }
]

load_relationships = [
  { 'filename': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRNsbtFpoFuHnKYixC6-UFHJ-IuPYkc6RHZ-teoN3LLWLWnMvH6YyVsZT7QWFxF3SU0DXchoRcOXjE3/pub?output=csv', 'type': 'MANAGES' }
]

def file_load(driver, database):
  session = driver.session(database=database)
  nodes = []
  relationships = []
  for file_item in load_nodes:
    nodes.append("{ fileName: '%s', labels: ['%s'] }" % (file_item["filename"], file_item["label"]) )
  for file_item in load_relationships:
    relationships.append("{ fileName: '%s', type: '%s' }" % (file_item["filename"], file_item["type"]) )
  query = """CALL apoc.import.csv( [%s], [%s], {stringIds: false})""" % (", ".join(nodes), ", ".join(relationships))
  print(query)
  result = session.run(query)
  for record in result:
    print(record)
    return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
  driver.close()
  return return_value

def clear(tx):
  tx.run("CALL apoc.periodic.iterate('MATCH (n) WHERE NOT n:`_Neodash_Dashboard` RETURN n', 'DETACH DELETE n', {batchSize:1000})")

def clear_neo4j(driver, database):
  with driver.session(database=database) as session:
    session.write_transaction(clear)
  driver.close()

db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
url = ServiceEnvironment().get('NEO4J_URL')
usr = ServiceEnvironment().get('NEO4J_USER')
pwd = ServiceEnvironment().get('NEO4J_PWD')
driver = GraphDatabase.driver(url, auth=(usr, pwd))

print("Deleting database ...")
clear_neo4j(driver, db_name)
print("Database deleted. Load new data ...")
result = file_load(driver, db_name)
print("Load complete. %s nodes and %s relationships loaded in %s milliseconds." % (result['nodes'], result['relationships'], result['time']))

