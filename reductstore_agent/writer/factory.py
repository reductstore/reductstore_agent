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

"""Factory for creating output writers."""

from reduct import Bucket

from ..dynamic_labels import LabelStateTracker
from ..models import OutputFormat, PipelineConfig
from .base import OutputWriter
from .cdr import CdrOutputWriter
from .mcap import McapOutputWriter


def create_writer(
    config: PipelineConfig, bucket: Bucket, pipeline_name: str = None, logger=None
) -> OutputWriter:
    """
    Create an appropriate output writer based on configuration.

    Args:
    ----
    config : PipelineConfig
        Pipeline configuration
    bucket : Bucket
        ReductStore bucket
    pipeline_name : str, optional
        Name of the pipeline
    logger : optional
        Optional logger
    Returns
    -------
    OutputWriter
        Configured writer instance

    """
    if config.output_format == OutputFormat.MCAP:
        return McapOutputWriter(
            bucket=bucket,
            pipeline_name=pipeline_name or "default",
            config=config,
            logger=logger,
            label_tracker=LabelStateTracker(config, logger) if config.labels else None,
        )
    elif config.output_format == OutputFormat.CDR:
        return CdrOutputWriter(
            bucket=bucket,
            pipeline_name=pipeline_name or "default",
            logger=logger,
            label_tracker=LabelStateTracker(config, logger) if config.labels else None,
        )
    else:
        raise ValueError(f"Unsupported output format: {config.output_format}")
