"""
  MediaRelay/relay.py
  Uploads new FTP files to other server.
  This doesn't synchronize stuff, just copies new things from A to B
  @author Filipp Lepalaan <filipp@mac.com>
  @created 19.10.2009
  
  1. Poll folder on FTP server A for changes
  2. Copy changed files to local machine
  3. Upload changed files to FTP server B
  - What if the changed file is just being uploaded to server A? (must use separate folder? Maybe the
  server is smart enough not to list an incomplete file?)
  - What if the poller runs before the last upload finishes? (fixed with PID checking)
  - Does "LIST -T" work on any FTP server? (no, but we don't need it)
  - What does "changes on server A" mean? (aything not in the local skiplist)
  - What if the new item is a folder? (will throw exception)
  - What if there are more than 1 new file? (it's OK)
"""
import yaml, pickle, os, sys, re
from ftplib import FTP

tmp_path    = ""
sl_path     = ".skiplist"
pfile_path  = "/private/tmp/MediaRelay.pid"

# Borrowed from http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
try:
  pf = file(pfile_path, "r")
  pid = int(pf.read().strip())
  pf.close()
except Exception, e:
  pid = None
  
if pid:
  sys.stderr.write("Relay already running with PID %d\n" % (pid))
  sys.exit(1)

try:
  c = yaml.load(file("config.yaml", "r"))
except Exception, e:
  sys.stderr.write("Cannot load config file")
  sys.exit(1)

# Remember who we are just in case this gets called during transfer
pid = str(os.getpid())
pf = file(pfile_path, "w")
pf.write(pid)
pf.close()

# The global skiplist stores the listings of all configs
g_skiplist = {}

# Loop through the config
for l in c:
  for k in l:
    (src_auth, src_url) = re.split("@", l[k]['source'])
    (src_uname, src_pwd) = re.split(":", src_auth)
    (src_host, src_path) = re.split("/", src_url, maxsplit=1)
    (dst_auth, dst_url) = re.split("@", l[k]['destination'])
    (dst_uname, dst_pwd) = re.split(":", dst_auth)
    (dst_host, dst_path) = re.split("/", dst_url, maxsplit=1)

    ftp_source = FTP(src_host)
    ftp_source.login(src_uname, src_pwd)

    ftp_destination = FTP(dst_host)
    ftp_destination.login(dst_uname, src_pwd)
    
    new = []; srv_list = []; skiplist = []
    
    # Load skiplist from file
    try:
      skiplist = pickle.load(file(sl_path, "r"))
    except Exception, e:
      skiplist = {k:[]}
      g_skiplist[k] = []
    
    # Load current list from source server
    srv_list = ftp_source.nlst(src_path)
    # Filter out skiplist
    new = filter(lambda i: i not in skiplist[k], srv_list)
      
    print "-- checking %s" % (k)

    # Download new files
    for f in new:
      try:
        print "  <- getting %s" % (f)
        ftp_cmd = "RETR %s/%s" % (src_path, f)
        ftp_source.retrbinary(ftp_cmd, open(f, "wb").write)
      except Exception, e:
        sys.stderr.write("Error getting %s - %s\n" % (f, e))
      else:
        try:
          # Upload new file to destination
          print "  -> sending %s" % (f)
          ftp_cmd = "STOR %s/%s" % (dst_path, f)
          ftp_destination.storbinary(ftp_cmd, open(f, "r"))
        except Exception, e:
          sys.stderr.write("Error sending %s - %s\n" % (f, e))
  
      os.remove(f)

    if len(new) < 1:
      print "  -- nothing to download"
    
    # Update global skiplist
    g_skiplist[k] = srv_list
    
    # Close FTP connections
    ftp_source.close(); ftp_destination.close()

# Update skiplist
pickle.dump(g_skiplist, file(sl_path, "w"))

# Remove PID file
os.remove(pfile_path)
