solr-replay
===========

Distributed log replay to stress-test an Apache Solr server with real-world traffic

1. Edit Config.py for your System Under Test (SUT)

2. Run:

* `./Manager.py` on a central server (typically same as SUT/RequestGenerator.py)
* If you want to run the RequestGenerator (sends querries to solr) on the same server as the Manager, just set MANAGER_STARTS_CONSUMERS to be > 0 (however many Consumers you want)
* `tail -f /var/log/solr/solr.log | grep QueryKeyWord | ./LogParser.py` on your source solr server where QueryKeyWord is an optional step to pre-filter input to only be what you care about
* If running distributed Request Generators, `./RequestGenerator.py` on your SUT solr server (one or more times... typicaly the same number or one more than the number of Producers)

You should get a file SolrPerfTestResults_*.csv wherever you ran Manager.py that has columns:
'TimeStamp, FullTime, QTime, numFound, ResponseLen, OrigQTime'
* TimeStamp is just a unixtime timestamp useful for plotting on x-axis
* FullTime is the time it took RequestGenerator.py to issue the request to the solr server and read the full response back (no parsing being done within the timed block)
* QTime is parsed out of the JSON response from the Solr server (make sure your query includes wt=json or these won't work)
* numFound is parsed out of the JSON response from the Solr server
* ResponseLen is just the str-len of the full response (approximating size to determine correlation between size and time increase)
* OrigQTime is the QTime of the original request on the source solr server (from the log) - useful to compare sometimes...