#!/usr/bin/env python3

from simple_http_server import route, server
from simple_http_server import StaticFile
import os

@route("/")
def index():
    root = os.path.dirname(os.path.abspath(__file__))
    return StaticFile("%s/edr_target/elementary_report.html" % root, "text/html; charset=utf-8")

server.start(port=8501)
