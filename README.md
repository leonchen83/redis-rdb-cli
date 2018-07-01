```java  

Options:
  -h, --help            show this help message and exit
  -c FILE, --command=FILE
                        Command to execute. Valid commands are json, diff,
                        justkeys, justkeyvals, memory and protocol
  -f FILE, --file=FILE  Output file
  -n DBS, --db=DBS      Database Number. Multiple databases can be provided.
                        If not specified, all databases will be included.
  -k KEYS, --key=KEYS   Keys to export. This can be a regular expression
  -o NOT_KEYS, --not-key=NOT_KEYS
                        Keys Not to export. This can be a regular expression
  -t TYPES, --type=TYPES
                        Data types to include. Possible values are string,
                        hash, set, sortedset, list. Multiple typees can be
                        provided.                      If not specified, all
                        data types will be returned
  -b BYTES, --bytes=BYTES
                        Limit memory output to keys greater to or equal to
                        this value (in bytes)
  -l LARGEST, --largest=LARGEST
                        Limit memory output to only the top N keys (by size)
  -e ESCAPE, --escape=ESCAPE
                        Escape strings to encoding: raw (default), print,
                        utf8, or base64.

```