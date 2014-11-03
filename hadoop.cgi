#!/usr/bin/env python

import cgi
import os
import sys
import subprocess

_view_input="""
<html>
  <head>
    <title>Sample Applications</title>
  </head>
  <body>
  <form method="POST" action="hadoop.cgi">
  <p>
  please input : <input name="inputName">
  <input type="submit">
  </p>
  </form>
  </body>
</html>
"""

_view_header="""

<html>
  <head>
    <title>Sample Applications</title>
  </head>
  <body>
  <pre>
"""
_view_footer="""
  </pre>
  </body>
</html>
"""

print "Content-Type: text/html"
print
print ""

form = cgi.FieldStorage()
if form.has_key('inputName'):
  if os.environ['REQUEST_METHOD'] != "POST":
    print "Request Error"
    sys.exit()

  inputName = form['inputName'].value
  args = ['/usr/bin/hadoop', 'dfs', '-ls', inputName]
  output = subprocess.Popen(args, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  print _view_header
  for line in output.stdout:
    print("%s" % line),
  print _view_footer
else:
  print _view_input

