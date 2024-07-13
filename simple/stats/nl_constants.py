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
"""
This file includes constants for artifacts used from main dc for 
generating a custom catalog yaml.

See this for full reference:

https://github.com/datacommonsorg/website/blob/1f46ef4f22d4777194f4dc6bb730ea56ca6f3207/deploy/nl/catalog.yaml#L119-L123
"""

# The custom embeddings index type.
# See: https://github.com/datacommonsorg/website/blob/2dabf815b82dcd5ff226ed94d8bbdab545ffd4d7/nl_server/custom_dc_env.yaml#L6
CUSTOM_EMBEDDINGS_INDEX = "user_all_minilm_mem"
# The name of the model used for custom dc embeddings.
# See: https://github.com/datacommonsorg/website/blob/1f46ef4f22d4777194f4dc6bb730ea56ca6f3207/deploy/nl/catalog.yaml#L119
CUSTOM_MODEL = "ft-final-v20230717230459-all-MiniLM-L6-v2"
# The GCS path to the model.
# See: https://github.com/datacommonsorg/website/blob/1f46ef4f22d4777194f4dc6bb730ea56ca6f3207/deploy/nl/catalog.yaml#L122
CUSTOM_MODEL_PATH = "gs://datcom-nl-models/ft_final_v20230717230459.all-MiniLM-L6-v2"
