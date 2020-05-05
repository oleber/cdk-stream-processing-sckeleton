#!/usr/bin/env python3

from aws_cdk import (core)
from template_generator.stream_processor import StreamProcessorStack

app = core.App()
StreamProcessorStack(app, "mrebelo-test-cloudformation", env={'region': 'eu-central-1'})

app.synth()
