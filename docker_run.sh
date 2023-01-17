docker run -it --rm \                                              
  --name neo4j-apoc \
  --publish=7474:7474 --publish=7687:7687 \
  --user="$(id -u):$(id -g)" \
  -e NEO4J_AUTH=neo4j/cdisc001 \
  --env NEO4J_PLUGINS='["apoc"]' \
  -e NEO4J_apoc_export_file_enabled=true \
  -e NEO4J_apoc_import_file_enabled=true \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  -e apoc.import.file.use_neo4j_config=false \
  neo4j:5.3.0-enterprise