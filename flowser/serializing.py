"""Serializing.

The purpose is to serialize and unserialize inputs and outputs to and from
workflows and tasks.

Now, this is just a facade for the standard `json` module.
"""
import json

dumps = json.dumps
loads = json.loads
