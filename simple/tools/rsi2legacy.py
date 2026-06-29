# Copyright 2023 Google Inc.
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

from absl import app
from absl import flags

import glob
import json
import os
import csv

FLAGS = flags.FLAGS

flags.DEFINE_string('input_path', '', 'Path with RSI stuff')
flags.DEFINE_string('output_path', '', 'Path for producing tmcf and mcf files')


def generate_tmcf(input_path, output_path):
   for filepath in glob.glob(os.path.join(input_path, '*.csv')):
        with open(filepath, 'r') as csvfile:
            obs_abt = ''
            obs_date = ''
            vars = []

            header = csv.DictReader(csvfile).fieldnames
            for h in header:
                if h in ['dcid', 'country', 'observationAbout']:
                    obs_abt = h
                elif h in ['year', 'date', 'observationDate']:
                    obs_date = h
                else:
                    vars.append(h)

            if not obs_abt or not obs_date or not vars:
                print(f'ERROR: Did not find observationAbout ({obs_abt}), observationDate ({obs_date}), or variables ({len(vars)})')
                continue

            nodes = []
            for i, var in enumerate(vars):
                parts = [
                        f'Node: E:Table->E{i}',
                        f'typeOf: dcs:StatVarObservation',
                        f'observationAbout: C:Table->{obs_abt}',
                        f'observationDate: C:Table->{obs_date}',
                        f'variableMeasured: dcs:{var}',
                        f'value: C:Table->{var}',
                        ''
                ]
                nodes.append('\n'.join(parts))

            fname = os.path.basename(filepath).replace('.csv', '.tmcf')
            _write_mcf(output_path, fname, nodes)


def generate_vars(input_path, output_path):
    with open(os.path.join(input_path, 'config.json')) as fp:
        variable_map = json.load(fp)['variables']
    
    svgs_emitted = set()
    svs = []
    svgs = []
    for sv in sorted(variable_map.keys()):
        data = variable_map[sv]
        sv_parts = [
            f'Node: dcid:{sv}',
            'typeOf: dcs:StatisticalVariable',
            'populationType: schema:Thing',
            f'measuredProperty: dcs:{sv}',
        ]
        if data.get('name'):
            sv_parts.append('name: "' + data['name'] + '"')
        if data.get('description'):
            sv_parts.append('description: "' + data['description'] + '"')

        svg = data.get('group')
        if svg:
            sv_parts.append(f'memberOf: dcs:{_svg_dcid(svg)}')

            path_parts = svg.split('/')
            for i, part in enumerate(path_parts):
                cur = '/'.join(path_parts[:i+1])
                par = ''
                if i > 0:
                    par = '/'.join(path_parts[:i])
                if cur in svgs_emitted:
                    continue
                svgs.append(_svg_node(cur, par, part))
                svgs_emitted.add(cur)
        
        svs.append('\n'.join(sv_parts) + '\n')
    
    _write_mcf(output_path, 'variables.mcf', svs)
    _write_mcf(output_path, 'variable_groups.mcf', svgs)


def _write_mcf(folder, file, nodes):
    with open(os.path.join(folder, file), 'w') as fp:
        fp.write('\n'.join(nodes))


# Input is of the form "ONE/Health/Health Expenditure".
# Corresponding output is of form: "ONE/g/Health/Health_Expenditure"
def _svg_dcid(grp):
    grp = grp.replace(' ', '')
    if '/' in grp:
        left, right = grp.split('/', 1)
        return f'{left}/g/{right}'
    else:
        return f'{grp}/g/Root'


def _svg_node(cur, parent, name):
    cur_id = _svg_dcid(cur)
    svg_parts = [
        f'Node: dcid:{cur_id}',
        'typeOf: dcs:StatVarGroup',
        f'name: "{name}"',
    ]
    if parent:
        par_id = _svg_dcid(parent)
        svg_parts.append(f'specializationOf: dcs{par_id}')
    return '\n'.join(svg_parts) + '\n'


def main(_):
  assert FLAGS.input_path and FLAGS.output_path
  generate_tmcf(FLAGS.input_path, FLAGS.output_path)
  generate_vars(FLAGS.input_path, FLAGS.output_path)

if __name__ == "__main__":
  app.run(main)