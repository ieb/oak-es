# oak-es

This repository is work in progress containing a port of the Oak Lucene Plugin to use Elastic Search. It aims, when complete
to provide all the functionality present in the Oak Lucene Plugin able to run with both installed. By default it runs ES co-located
in the same JVM as Oak. Where there are multiple Oak JVMs it aims to form an ES cluster co-located into those JVMs. Connecting to
an external ES cluster is achieved by changing OSGi configuration.


