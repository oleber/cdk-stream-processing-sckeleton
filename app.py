#!/usr/bin/env python3

from aws_cdk import (core)
from template_generator.stream_processor import StreamProcessorStack
import os

app = core.App()

prefix = os.environ.get('PREFIX')
if prefix is None:
    prefix = "test"

StreamProcessorStack(app, f"mrebelo-{prefix}-stream-delete-me", prefix, env={'region': 'eu-central-1'})

app.synth()
