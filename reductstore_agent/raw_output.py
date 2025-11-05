# Copyright 2025 ReductSoftware UG
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""Raw output format logic per pipeline."""

from rclpy.serialization import serialize_message
from reductstore_agent.recorder import Recorder
from reductstore_agent.config_models import PipelineConfig
from reduct import Client
from typing import Any
BATCH_LIMIT = 100 * 1024


#4. decide if we batch or stream data depending on record size:
# batch if record < 100KB
# meaning u put it into the buffer 
# streaming = send it immediately



# Possible workflow
#1. serialize data via serialization
#2. decide to either batch or stream data
#2. save the data
#3. send http call to reduct client 


class RawOutputWriter:
    def __init__(self, reduct_client: Client, entry_name:str):
        self.client = reduct_client


    def write_message(self, message: Any, topic_type: str, publish_time: int):
        # 1. serialize data 
        
        try: 
            serialized_data = serialize_message(message)
        except:
            return
        
        # 2. Add labels timestamp etc
        # use timestamp in ms as index

        labels = {
            "type": f"{topic_type} Message",
            "serialization": "cdr",
        }
        timestamp_ms =  publish_time // 1000


    async def upload_to_reductstore():
        pass




