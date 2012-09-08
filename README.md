solr-replay
===========

Distributed log replay to stress-test an Apache Solr server with real-world traffic

* Edit Config.py for your System Under Test (SUT)
* Run:
** `./Manager.py` on a central box (typically same as SUT/Consumer.py)
** `tail -f /var/log/solr/solr.log | grep QueryKeyWord | ./Producer.py` on your source solr server where QueryKeyWord is an optional step to pre-filter input to only be what you care about
** `./Consumer.py` on your SUT solr server (one or more times... typicaly the same number or one more than the number of Producers)