# Copyright 2026 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import json
import logging
import multiprocessing
import sqlite3

from pyld import jsonld
from rdflib import Graph
from rdflib import Literal
from rdflib import Namespace
from rdflib import RDF
from rdflib import URIRef
from stats.util import is_entity_reference
from util.filesystem import create_store

DCID_URL = "https://datacommons.org/browser/"
PREDICATE_URL = "url"


def expand_id(item):
  """Expands a short ID into a full URIRef."""
  if not item:
    return None
  if item.startswith("http://") or item.startswith("https://"):
    return URIRef(item)
  if item.startswith("dcid:"):
    return URIRef(f"{DCID_URL}{item[5:]}")
  return URIRef(f"{DCID_URL}{item.lstrip('/')}")


def process_triples(db, output_dir, ns_map: dict, chunk_size: int):
  """Processes schema triples in chunks and writes them to JSON-LD shards."""
  offset = 0
  shard_index = 0

  while True:
    triples_tuples = db.engine.fetch_all(
        "SELECT subject_id, predicate, object_id, object_value FROM triples LIMIT ? OFFSET ?",
        (chunk_size, offset))

    if not triples_tuples:
      break

    g = Graph()
    DCID = Namespace(DCID_URL)
    g.bind("dcid", DCID)

    for idx, row in enumerate(triples_tuples):
      sub_id, pred, obj_id, obj_val = row
      sub = expand_id(sub_id)
      p = expand_id(pred)

      if not sub:
        logging.error("Subject is None for row: %s (sub_id: '%s')", row, sub_id)
        # Print surrounding rows for context (using loop index to avoid O(N) search)
        start = max(0, idx - 5)
        end = min(len(triples_tuples), idx + 5)
        logging.error("Surrounding rows for context:")
        for r in triples_tuples[start:end]:
          marker = "->" if r == row else "  "
          logging.error("  %s %s", marker, r)

      if obj_id:
        o = expand_id(obj_id)
      else:
        o = Literal(obj_val)

      if pred == 'typeOf':
        g.add((sub, RDF.type, o))
      else:
        g.add((sub, p, o))

    write_shard(g, shard_index, output_dir, ns_map, prefix="node")
    shard_index += 1
    offset += chunk_size

    if len(triples_tuples) < chunk_size:
      break


def _add_observation_to_graph(g, row, DCID, prov_urls):
  """Helper to add an observation row to the graph."""
  entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row

  # Generate a deterministic ID for the observation to avoid collisions across runs
  key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_{props}"
  obs_hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
  subject = DCID[f"obs_{obs_hash}"]

  g.add((subject, RDF.type, DCID["StatVarObservation"]))
  entity_ref = expand_id(entity)
  if entity_ref:
    g.add((subject, DCID["observationAbout"], entity_ref))
  var_ref = expand_id(variable)
  g.add((subject, DCID["variableMeasured"], var_ref))
  if props:
    try:
      props_dict = json.loads(props)
      for k in props_dict.keys():
        g.add((var_ref, DCID["observationProperty"], expand_id(k)))
    except json.JSONDecodeError:
      pass
  g.add((subject, DCID["observationDate"], Literal(date)))

  try:
    g.add((subject, DCID["value"], Literal(float(value))))
  except ValueError:
    g.add((subject, DCID["value"], Literal(value)))

  if provenance:
    g.add((subject, DCID["provenance"], expand_id(provenance)))
    if provenance in prov_urls and prov_urls[provenance]:
      g.add((subject, DCID["provenanceUrl"], Literal(prov_urls[provenance])))
  if unit:
    g.add((subject, DCID["unit"], expand_id(unit)))
  if scaling_factor:
    g.add((subject, DCID["scalingFactor"], Literal(scaling_factor)))
  if mmethod:
    g.add((subject, DCID["measurementMethod"], expand_id(mmethod)))
  if period:
    g.add((subject, DCID["observationPeriod"], Literal(period)))

  if props:
    try:
      props_dict = json.loads(props)
      for k, v in props_dict.items():
        if is_entity_reference(v):
          g.add((subject, expand_id(k), expand_id(v)))
        else:
          g.add((subject, expand_id(k), Literal(v)))
    except json.JSONDecodeError as e:
      logging.warning(
          f"Failed to decode properties JSON for observation {entity}/{variable}: {e}"
      )


def _process_observation_chunk(args):
  """Worker function to process a single chunk of observations in parallel.
  
  This runs in a separate process, so it must establish its own DB connection
  and import necessary modules locally.
  """
  shard_index, offset, chunk_size, db_path, output_dir_path, ns_map, prov_urls = args

  # Open a new connection for this worker process (SQLite connections cannot be shared across processes)
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()

  # Fetch the specific chunk of observations for this shard
  cursor.execute(
      "SELECT entity, variable, date, value, provenance, unit, scaling_factor, "
      "measurement_method, observation_period, properties FROM observations LIMIT ? OFFSET ?",
      (chunk_size, offset))
  obs_tuples = cursor.fetchall()
  conn.close()

  if not obs_tuples:
    return False

  # Build the RDF graph for this chunk
  g = Graph()
  DCID = Namespace(DCID_URL)
  g.bind("dcid", DCID)

  for row in obs_tuples:
    _add_observation_to_graph(g, row, DCID, prov_urls)

  # Write the graph to a JSON-LD shard file
  with create_store(output_dir_path) as store:
    output_dir = store.as_dir()
    write_shard(g, shard_index, output_dir, ns_map, prefix="observation")

  return True


def process_observations(db, output_dir, ns_map: dict, chunk_size: int):
  """Processes observations in chunks in parallel and writes them to JSON-LD shards."""
  db_path = db.engine.db_file.syspath()
  output_dir_path = output_dir.full_path()

  # Calculate the total number of chunks needed
  total_obs = db.engine.fetch_all("SELECT COUNT(*) FROM observations")[0][0]
  num_chunks = (total_obs + chunk_size - 1) // chunk_size

  # Fetch all provenance URLs once to pass to workers
  rows = db.engine.fetch_all(
      f"SELECT subject_id, object_value FROM triples WHERE predicate = '{PREDICATE_URL}'"
  )
  prov_urls = {row[0]: row[1] for row in rows}

  # Prepare arguments for the worker pool (each chunk gets its own offset and index)
  args_list = [(i, i * chunk_size, chunk_size, db_path, output_dir_path, ns_map,
                prov_urls) for i in range(num_chunks)]

  # Cap the number of processes to avoid overloading the machine
  num_processes = min(multiprocessing.cpu_count(), 8)
  logging.info("Starting observations export with %d processes for %d chunks",
               num_processes, num_chunks)

  # Run the workers in parallel
  with multiprocessing.Pool(processes=num_processes) as pool:
    pool.map(_process_observation_chunk, args_list)


def export_to_jsonld(db,
                     output_dir,
                     chunk_size: int = 10000,
                     context: dict = None):
  """Exports resolved data from the database to JSON-LD shards.

  Args:
  ----
    db: The database instance containing triples and observations.
    output_dir: The directory where JSON-LD shards will be written.
    chunk_size: The number of rows to fetch and process at a time.
    context: Optional custom JSON-LD context mappings.
  """
  logging.info("Exporting resolved data to JSON-LD in shards")

  ns_map = {"dcid": DCID_URL}
  if context:
    ns_map.update(context)

    # 1. Process Triples (Schema) in chunks
  process_triples(db, output_dir, ns_map, chunk_size)

  # 2. Process Observations in chunks
  process_observations(db, output_dir, ns_map, chunk_size)


def write_shard(g: Graph,
                index: int,
                output_dir,
                ns_map: dict,
                prefix: str = "output"):
  """
  Serializes and writes an RDF graph to a JSON-LD shard.

  Args:
  -----
    g: The RDF graph to serialize.
    index: The shard index for the filename.
    output_dir: The directory to write the shard file to.
    ns_map: The namespace map for context compaction.
    prefix: The file name prefix (e.g. 'node' or 'observation').

  """
  jsonld_str = g.serialize(context=ns_map, format="json-ld", indent=4)
  expanded_jsonld = json.loads(jsonld_str)
  compacted_jsonld = jsonld.compact(expanded_jsonld, ns_map)

  if "@graph" not in compacted_jsonld:
    data_only = {k: v for k, v in compacted_jsonld.items() if k != "@context"}
    compacted_jsonld = {
        "@context": compacted_jsonld.get("@context"),
        "@graph": [data_only]
    }

  shard_name = f"{prefix}-{index:05d}.jsonld"
  output_dir.open_file(shard_name).write(json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name}")
