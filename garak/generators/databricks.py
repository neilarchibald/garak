#!/usr/bin/env python3
"""Databricks API generator

Supports chat + codecompletion models.
"""

import sys
import pandas as pd
import os
import re
from typing import List
import backoff
import asyncio
import json
import requests

from garak.generators.base import Generator

from databricks_genai_inference import ChatCompletion
from databricks_genai_inference import Completion

#/python3.11 -m garak --model_type databricks --model_name lakesense-chat --probes dan.Dan_11_0
def db_chat_complete(model,prompt):
    #print("\n\n",model,"\n\n")
    response = ChatCompletion.create(model=model,
                                    messages=[{"role": "system", "content": "You are a helpful assistant."},
                                            {"role": "user","content": prompt}],
                                    max_tokens=128)
    print(response)
    return response.message()

def db_embedding():
    pass

def db_text_complete(model,prompt):
    response = Completion.create(
        model=model,
        prompt=prompt,
        max_tokens=128)
    return response.text

DISPATCH_TABLE = {
    "chat_complete": db_chat_complete,
    "embedding": db_embedding,
    "text_complete": db_text_complete,
}

def databricks_endpoint(type,prompt,model_name):
    api_key = ""
    host = ""
    if not "dbutils" in globals(): 
        api_key = os.getenv("DATABRICKS_TOKEN", default=None)
        if api_key is None:
                raise ValueError(
                    'Put the DATABRICKS API key in the DATABRICKS_TOKEN environment variable (this was empty)\n \
                    e.g.: export DATABRICKS_API_KEY="dapi123XXXXXXXXXXXX"'
                )
        host = os.getenv("DATABRICKS_HOST", default=None)
        if host is None:
                raise ValueError(
                    'Put the DATABRICKS Workspace URL in the DATABRICKS_HOST environment variable (this was empty)\n \
                    e.g.: export DATABRICKS_HOST="https://mydatabrickshost.com"'
                )
    else:
        host= dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
        os.environ["DATABRICKS_HOST"] = host
        api_key = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None) 
        os.environ["DATABRICKS_TOKEN"] = api_key 
    return DISPATCH_TABLE[type](model_name,prompt)

class DatabricksGenerator(Generator):
    def __init__(self, name, generations=10):
        (self.name,self.type) = name.split("/")
        self.fullname = f"Databricks LLM {self.name}"
        self.generations = generations

        if self.type not in DISPATCH_TABLE:
            raise ValueError(
            f"No Databricks API defined for '{self.type}' in generators/databricks.py - please add one!"
        )

        print("[+] Testing model: %s of type %s" % (self.name,self.type))

        super().__init__(self.name, generations=generations)

    def _call_model(self, prompt: str) -> str:
        return databricks_endpoint(self.type,prompt,self.name)

default_class = "DatabricksGenerator"
