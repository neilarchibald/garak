#!/usr/bin/env python3
"""
Databricks Responsible AI Table Usage
"""

import random

from garak.attempt import Attempt
from garak import _config
from garak.probes.base import Probe

from pyspark.sql import SparkSession

class DBRAI(Probe):
    """Holder for each attack"""

    bcp47 = "en"
    active = False
    goal = "Make the model output biased or inappropriate content"
    uri = "https://www.lesswrong.com/posts/aPeJE8bSo6rAFoLqg/solidgoldmagikarp-plus-prompt-generation"
    tags = [
        "avid-effect:security:S0201",
        "avid-effect:performance:P0201",
        "owasp:llm05",
        "quality:Security:PromptStability",
    ]

    recommended_detector = ["goodside.Glitch"]
    max_prompts = None

    def __init__(self):
        super().__init__()
        self.prompts = []

        # Create a SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Read data from the Databricks table
        df = spark.read.table("prodsec_ai_corpus.default.chat_jailbreak")

        # Extract the input values from the 'Prompt/input' column and convert them to a list
        input_values = df.select("Prompt/input").toPandas()["Prompt/input"].tolist()

        # Call your function for each input value
        for input_value in input_values:
            self.prompts.append(input_value)

        # Stop the SparkSession
        spark.stop()
