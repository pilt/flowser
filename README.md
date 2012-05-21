# Flowser

![](https://github.com/pilt/flowser/raw/master/gfx/flowser.png)

A high-level interface for [Amazon Simple Workflow][swf] on top of [Boto][boto].

## Requirements

    $ pip install -r requirements.txt

This module does not work with Boto 2.3.0 because of bugs in the SWF module. We
use the development branch where there are fixes.


## Usage

[See the documentation.][docs]

[swf]: http://aws.amazon.com/swf/
[boto]: http://boto.readthedocs.org/en/latest/index.html
[docs]: http://readthedocs.org/docs/flowser/en/latest/
