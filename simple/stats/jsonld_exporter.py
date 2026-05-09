# Copyright 2024 Google Inc.
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
import os
from pyld import jsonld
from rdflib import Graph, Literal, Namespace, RDF, URIRef

DCID_URL = "https://datacommons.org/browser/"

def export_to_jsonld(db, output_dir, chunk_size: int = 10000, context: dict = None):
    """Exports resolved data from the database to JSON-LD shards.
    
    Args:
        db: The database instance containing triples and observations.
        output_dir: The directory where JSON-LD shards will be written.
        chunk_size: The number of rows to fetch and process at a time.
        context: Optional custom JSON-LD context mappings.
    """
    logging.info("Exporting resolved data to JSON-LD in shards")
    
    ns_map = {"dcid": DCID_URL}
    if context:
        ns_map.update(context)
    
    def expand_id(item):
        if not item:
            return None
        if item.startswith("http://") or item.startswith("https://"):
            return URIRef(item)
        if item.startswith("dcid:"):
            return URIRef(f"{DCID_URL}{item[5:]}")
        return URIRef(f"{DCID_URL}{item.lstrip('/')}")
        
    shard_index = 0
    
    # 1. Process Triples (Schema) in chunks
    offset = 0
    while True:
        triples_tuples = db.engine.fetch_all(
            f"SELECT subject_id, predicate, object_id, object_value FROM triples LIMIT {chunk_size} OFFSET {offset}"
        )
        
        if not triples_tuples:
            break
            
        g = Graph()
        DCID = Namespace(DCID_URL)
        g.bind("dcid", DCID)
        
        for row in triples_tuples:
            sub_id, pred, obj_id, obj_val = row
            sub = expand_id(sub_id)
            p = expand_id(pred)
            
            if obj_id:
                o = expand_id(obj_id)
            else:
                o = Literal(obj_val)
                
            if pred == 'typeOf':
                g.add((sub, RDF.type, o))
            else:
                g.add((sub, p, o))
                
        write_shard(g, shard_index, output_dir, ns_map)
        shard_index += 1
        offset += chunk_size
        
        if len(triples_tuples) < chunk_size:
            break
            
    # 2. Process Observations in chunks
    offset = 0
    while True:
        obs_tuples = db.engine.fetch_all(
            f"SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties FROM observations LIMIT {chunk_size} OFFSET {offset}"
        )
        
        if not obs_tuples:
            break
            
        g = Graph()
        DCID = Namespace(DCID_URL)
        g.bind("dcid", DCID)
        
        for row in obs_tuples:
            entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row
            
            # Generate a deterministic ID for the observation to avoid collisions across runs
            key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}"
            obs_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
            subject = DCID[f"obs_{obs_hash}"]
            
            g.add((subject, RDF.type, DCID["StatVarObservation"]))
            g.add((subject, DCID["observationAbout"], expand_id(entity)))
            g.add((subject, DCID["variableMeasured"], expand_id(variable)))
            g.add((subject, DCID["observationDate"], Literal(date)))
            
            try:
                g.add((subject, DCID["value"], Literal(float(value))))
            except ValueError:
                g.add((subject, DCID["value"], Literal(value)))
                
            if provenance:
                g.add((subject, DCID["provenance"], expand_id(provenance)))
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
                        g.add((subject, expand_id(k), Literal(v)))
                except json.JSONDecodeError as e:
                    logging.warning(f"Failed to decode properties JSON for observation {entity}/{variable}: {e}")
                    
        write_shard(g, shard_index, output_dir, ns_map)
        shard_index += 1
        offset += chunk_size
        
        if len(obs_tuples) < chunk_size:
            break

def write_shard(g: Graph, index: int, output_dir, ns_map: dict):
    """Serializes and writes an RDF graph to a JSON-LD shard.
    
    Args:
        g: The RDF graph to serialize.
        index: The shard index for the filename.
        output_dir: The directory to write the shard file to.
        ns_map: The namespace map for context compaction.
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
        
    shard_name = f"output-{index:05d}.jsonld"
    output_dir.open_file(shard_name).write(json.dumps(compacted_jsonld, indent=4))
    logging.info(f"Saved JSON-LD shard to {shard_name}")
