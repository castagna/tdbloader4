TDB Loader 3
------------

This is a small prototype to show how TDB indexes can be generated using
MapReduce. It is done to investigate whether it faster and/or cheaper to 
use MapReduce to create TDB indexes.


Options
-------

A few options supported by tdbloader3:

 -D overrideOutput=true ........................ to override the output path
 -D verify=true ............... used for small datasets to check correctness
 -D useCompression=true ... to compress intermediate output (SeqFile BLOCKs)
 -D copyToLocal=true ......... TDB indexes are copied to a local output path
 -D runLocal=true ....................... to run tests locally (and quickly)

You can of course use your Hadoop cluster or Apache Whirr to start one on EC2.


How to use Apache Whirr
-----------------------

To start up an Hadoop cluster using Apache Whirr:

  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  cd /opt/
  curl -O http://www.apache.org/dist/incubator/whirr/whirr-0.6.0-incubating/whirr-0.6.0-incubating.tar.gz
  tar zxf whirr-0.6.0-incubating.tar.gz
  ssh-keygen -t rsa -P '' -f ~/.ssh/whirr
  export PATH=$PATH:/opt/whirr-0.6.0-incubating/bin/
  whirr version
  whirr launch-cluster --config hadoop-ec2.properties --private-key-file ~/.ssh/whirr
  . ~/.whirr/hadoop/hadoop-proxy.sh
  # Proxy PAC configuration here: http://apache-hadoop-ec2.s3.amazonaws.com/proxy.pac

To copy stuff from your local disk or S3 into your HDFS cluster running on EC2:

  hadoop --config ~/.whirr/hadoop fs -mkdir /user/castagna/input
  hadoop --config ~/.whirr/hadoop fs -put src/test/resources/input/* /user/castagna/input

Or from a location on S3 (in parallel) use distcp:

  hadoop distcp s3n://$AWS_ACCESS_KEY_ID_LIVE:$AWS_SECRET_ACCESS_KEY_LIVE@{bucketname} hdfs://{hostname}:8020/user/castagna/input

To launch the tdbloader3 MapReduce jobs:

  mvn hadoop:pack
  hadoop --config ~/.whirr/hadoop jar target/hadoop-deploy/tdbloader3-hdeploy.jar cmd.tdbloader3 -D overrideOutput=true -D copyToLocal=true input target/output

To shutdown the cluster:

  whirr destroy-cluster --config hadoop-ec2.properties


How it works
------------

TDB Loader 3 is a sequence of four MapReduce jobs. 

The first MapReduce computes offsets which are used to build the node table. 
The second MapReduce job builds the node table and produces the input for the 
next job. The third MapReduce job generates the input data using node ids 
rather than URIs, blank nodes or literals. The last (i.e. fourth) MapReduce 
job reuses the same BPlusTreeRewriter used by tdbloader2 to finally create 
the nine TDB indexes (i.e. SPO, POS, OSP, GSPO, etc.).

Follows a complete example which shows how data flows between the four 
MapReduce jobs.

Here is the input data (a data.nq N-Quads file and a data.nt N-Triples file):

data.nq:

<http://example.org/alice/foaf.rdf#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/alice/foaf.rdf> .
<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/name>                  "Alice"                            <http://example.org/alice/foaf.rdf> .
<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/knows>                 _:bnode1                           <http://example.org/alice/foaf.rdf> .
_:bnode1                               <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/alice/foaf.rdf> .
_:bnode1                               <http://xmlns.com/foaf/0.1/name>                  "Bob"                              <http://example.org/alice/foaf.rdf> .
_:bnode1                               <http://xmlns.com/foaf/0.1/homepage>              <http://example.org/bob/>          <http://example.org/alice/foaf.rdf> .
_:bnode1                               <http://www.w3.org/2000/01/rdf-schema#seeAlso>   <http://example.org/bob/foaf.rdf>  <http://example.org/alice/foaf.rdf> .
<http://example.org/bob/foaf.rdf#me>   <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/bob/foaf.rdf> .
<http://example.org/bob/foaf.rdf#me>   <http://xmlns.com/foaf/0.1/name>                  "Bob"                              <http://example.org/bob/foaf.rdf> .
<http://example.org/bob/foaf.rdf#me>   <http://xmlns.com/foaf/0.1/homepage>              <http://example.org/bob/>          <http://example.org/bob/foaf.rdf> .

data.nt:

<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/mbox>                  <mailto:alice@example.org> . 
<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/mbox>                  <mailto:alice@example.org> . 
<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/name>                  "Alice" .
<http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/knows>                 _:bnode1 .


First MapReduce job ('<' == input, '>' == output):

The map function of the first job parses N-Triples or N-Quads files using RIOT
and it emits (key, value) pairs where the key is the RDF node and the value is
null. This is the data processed by the map function:

< (0, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, (null))
> (<http://xmlns.com/foaf/0.1/Person>, (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (162, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Alice"])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/name>, (null))
> ("Alice", (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (324, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/knows mrbnode_-459157714_-2124521360_bnode1])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/knows>, (null))
> (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (486, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, (null))
> (<http://xmlns.com/foaf/0.1/Person>, (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (648, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://xmlns.com/foaf/0.1/name "Bob"])
> (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (<http://xmlns.com/foaf/0.1/name>, (null))
> ("Bob", (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (810, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://xmlns.com/foaf/0.1/homepage http://example.org/bob/])
> (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (<http://xmlns.com/foaf/0.1/homepage>, (null))
> (<http://example.org/bob/>, (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (972, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://www.w3.org/2000/01/rdf-schema#seeAlso http://example.org/bob/foaf.rdf])
> (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (<http://www.w3.org/2000/01/rdf-schema#seeAlso>, (null))
> (<http://example.org/bob/foaf.rdf>, (null))
> (<http://example.org/alice/foaf.rdf>, (null))

< (1133, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (<http://example.org/bob/foaf.rdf#me>, (null))
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, (null))
> (<http://xmlns.com/foaf/0.1/Person>, (null))
> (<http://example.org/bob/foaf.rdf>, (null))

< (1293, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Bob"])
> (<http://example.org/bob/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/name>, (null))
> ("Bob", (null))
> (<http://example.org/bob/foaf.rdf>, (null))

< (1453, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://xmlns.com/foaf/0.1/homepage http://example.org/bob/])
> (<http://example.org/bob/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/homepage>, (null))
> (<http://example.org/bob/>, (null))
> (<http://example.org/bob/foaf.rdf>, (null))

< (0, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/mbox mailto:alice@example.org])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/mbox>, (null))
> (<mailto:alice@example.org>, (null))

< (119, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/mbox mailto:alice@example.org])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/mbox>, (null))
> (<mailto:alice@example.org>, (null))

< (238, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Alice"])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/name>, (null))
> ("Alice", (null))

< (337, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/knows mrbnode_-459157714_-2124521357_bnode1])
> (<http://example.org/alice/foaf.rdf#me>, (null))
> (<http://xmlns.com/foaf/0.1/knows>, (null))
> (_:mrbnode_-459157714_-2124521357_bnode1, (null))


The reduce function of the first job is used to compute offsets, since node
ids are offset to the nodes.dat file and we must be able to generate this file
using multiple reducers, we need a first pass over the data to precompute
offsets of each partition of the data. This is the input/output of the reduce
function (only one reducer is used in this example):

< 0: ("Alice", (null))
< 0: ("Bob", (null))
< 0: (<http://example.org/alice/foaf.rdf#me>, (null))
< 0: (<http://example.org/alice/foaf.rdf>, (null))
< 0: (<http://example.org/bob/>, (null))
< 0: (<http://example.org/bob/foaf.rdf#me>, (null))
< 0: (<http://example.org/bob/foaf.rdf>, (null))
< 0: (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, (null))
< 0: (<http://www.w3.org/2000/01/rdf-schema#seeAlso>, (null))
< 0: (<http://xmlns.com/foaf/0.1/Person>, (null))
< 0: (<http://xmlns.com/foaf/0.1/homepage>, (null))
< 0: (<http://xmlns.com/foaf/0.1/knows>, (null))
< 0: (<http://xmlns.com/foaf/0.1/mbox>, (null))
< 0: (<http://xmlns.com/foaf/0.1/name>, (null))
< 0: (<mailto:alice@example.org>, (null))
< 0: (_:mrbnode_-459157714_-2124521357_bnode1, (null))
< 0: (_:mrbnode_-459157714_-2124521360_bnode1, (null))
> (0, 617)


Second MapReduce job:

The map function of the second MapReduce jobs, read the input data again,
parses the N-Triples or N-Quads files, and it emits (key, value) pairs where 
the key is the RDF node and the value is a unique identifier for that 
triple|quad concatenated with the position of the RDF node itself in the 
triple|quad:


< (0, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (<http://example.org/alice/foaf.rdf#me>, e106576560405e9c91aa4a28d1b35b34|S)
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, e106576560405e9c91aa4a28d1b35b34|P)
> (<http://xmlns.com/foaf/0.1/Person>, e106576560405e9c91aa4a28d1b35b34|O)
> (<http://example.org/alice/foaf.rdf>, e106576560405e9c91aa4a28d1b35b34|G)

< (162, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Alice"])
> (<http://example.org/alice/foaf.rdf#me>, 48bc9707b09e5574ba50d4f58f0e5fec|S)
> (<http://xmlns.com/foaf/0.1/name>, 48bc9707b09e5574ba50d4f58f0e5fec|P)
> ("Alice", 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (<http://example.org/alice/foaf.rdf>, 48bc9707b09e5574ba50d4f58f0e5fec|G)

< (324, [http://example.org/alice/foaf.rdf http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/knows mrbnode_-459157714_-2124521360_bnode1])
> (<http://example.org/alice/foaf.rdf#me>, 2d2095ff97c0cbf8fd210db91877cd8b|S)
> (<http://xmlns.com/foaf/0.1/knows>, 2d2095ff97c0cbf8fd210db91877cd8b|P)
> (_:mrbnode_-459157714_-2124521360_bnode1, 2d2095ff97c0cbf8fd210db91877cd8b|O)
> (<http://example.org/alice/foaf.rdf>, 2d2095ff97c0cbf8fd210db91877cd8b|G)

< (486, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (_:mrbnode_-459157714_-2124521360_bnode1, 5aaffc31a108b4c91177a86ef614e479|S)
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, 5aaffc31a108b4c91177a86ef614e479|P)
> (<http://xmlns.com/foaf/0.1/Person>, 5aaffc31a108b4c91177a86ef614e479|O)
> (<http://example.org/alice/foaf.rdf>, 5aaffc31a108b4c91177a86ef614e479|G)

< (648, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://xmlns.com/foaf/0.1/name "Bob"])
> (_:mrbnode_-459157714_-2124521360_bnode1, 36abd40cbf6d7f33ae6814797a2a16a1|S)
> (<http://xmlns.com/foaf/0.1/name>, 36abd40cbf6d7f33ae6814797a2a16a1|P)
> ("Bob", 36abd40cbf6d7f33ae6814797a2a16a1|O)
> (<http://example.org/alice/foaf.rdf>, 36abd40cbf6d7f33ae6814797a2a16a1|G)

< (810, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://xmlns.com/foaf/0.1/homepage http://example.org/bob/])
> (_:mrbnode_-459157714_-2124521360_bnode1, a9752b388fdb17a7fec198626199d1f4|S)
> (<http://xmlns.com/foaf/0.1/homepage>, a9752b388fdb17a7fec198626199d1f4|P)
> (<http://example.org/bob/>, a9752b388fdb17a7fec198626199d1f4|O)
> (<http://example.org/alice/foaf.rdf>, a9752b388fdb17a7fec198626199d1f4|G)

< (972, [http://example.org/alice/foaf.rdf mrbnode_-459157714_-2124521360_bnode1 http://www.w3.org/2000/01/rdf-schema#seeAlso http://example.org/bob/foaf.rdf])
> (_:mrbnode_-459157714_-2124521360_bnode1, ede8d3b91138dc5f5ee6b0aada620769|S)
> (<http://www.w3.org/2000/01/rdf-schema#seeAlso>, ede8d3b91138dc5f5ee6b0aada620769|P)
> (<http://example.org/bob/foaf.rdf>, ede8d3b91138dc5f5ee6b0aada620769|O)
> (<http://example.org/alice/foaf.rdf>, ede8d3b91138dc5f5ee6b0aada620769|G)

< (1133, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://xmlns.com/foaf/0.1/Person])
> (<http://example.org/bob/foaf.rdf#me>, 4c39149e9ecced8fcd10fac086ec9c4c|S)
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, 4c39149e9ecced8fcd10fac086ec9c4c|P)
> (<http://xmlns.com/foaf/0.1/Person>, 4c39149e9ecced8fcd10fac086ec9c4c|O)
> (<http://example.org/bob/foaf.rdf>, 4c39149e9ecced8fcd10fac086ec9c4c|G)

< (1293, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Bob"])
> (<http://example.org/bob/foaf.rdf#me>, 6e03d3250935a6bc8dcb64f14fa5972d|S)
> (<http://xmlns.com/foaf/0.1/name>, 6e03d3250935a6bc8dcb64f14fa5972d|P)
> ("Bob", 6e03d3250935a6bc8dcb64f14fa5972d|O)
> (<http://example.org/bob/foaf.rdf>, 6e03d3250935a6bc8dcb64f14fa5972d|G)

< (1453, [http://example.org/bob/foaf.rdf http://example.org/bob/foaf.rdf#me http://xmlns.com/foaf/0.1/homepage http://example.org/bob/])
> (<http://example.org/bob/foaf.rdf#me>, 8b16f433141d4e73c9af1483ab32fcf4|S)
> (<http://xmlns.com/foaf/0.1/homepage>, 8b16f433141d4e73c9af1483ab32fcf4|P)
> (<http://example.org/bob/>, 8b16f433141d4e73c9af1483ab32fcf4|O)
> (<http://example.org/bob/foaf.rdf>, 8b16f433141d4e73c9af1483ab32fcf4|G)

< (0, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/mbox mailto:alice@example.org])
> (<http://example.org/alice/foaf.rdf#me>, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (<http://xmlns.com/foaf/0.1/mbox>, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (<mailto:alice@example.org>, 3e3e4c277b7db437f5cdf9bae65a02bc|O)

< (119, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/mbox mailto:alice@example.org])
> (<http://example.org/alice/foaf.rdf#me>, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (<http://xmlns.com/foaf/0.1/mbox>, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (<mailto:alice@example.org>, 3e3e4c277b7db437f5cdf9bae65a02bc|O)

< (238, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/name "Alice"])
> (<http://example.org/alice/foaf.rdf#me>, 979a843d6b468cfa7787b361a7cfbee9|S)
> (<http://xmlns.com/foaf/0.1/name>, 979a843d6b468cfa7787b361a7cfbee9|P)
> ("Alice", 979a843d6b468cfa7787b361a7cfbee9|O)

< (337, [urn:x-arq:DefaultGraphNode http://example.org/alice/foaf.rdf#me http://xmlns.com/foaf/0.1/knows mrbnode_-459157714_-2124521357_bnode1])
> (<http://example.org/alice/foaf.rdf#me>, 4f5e5bc047a1d8e5928ed8a6f8297dcf|S)
> (<http://xmlns.com/foaf/0.1/knows>, 4f5e5bc047a1d8e5928ed8a6f8297dcf|P)
> (_:mrbnode_-459157714_-2124521357_bnode1, 4f5e5bc047a1d8e5928ed8a6f8297dcf|O)


The reduce function of the second job adds the RDF node received as key to the
node table and it emits a (key, value) pair with the node id, adjusted with the
offsets of partitions with an id lower than the current, and the same value
it has received:


< ("Alice", 979a843d6b468cfa7787b361a7cfbee9|O)
> (0, 979a843d6b468cfa7787b361a7cfbee9|O)

< ("Alice", 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (0, 48bc9707b09e5574ba50d4f58f0e5fec|O)

< ("Bob", 36abd40cbf6d7f33ae6814797a2a16a1|O)
> (11, 36abd40cbf6d7f33ae6814797a2a16a1|O)

< ("Bob", 6e03d3250935a6bc8dcb64f14fa5972d|O)
> (11, 6e03d3250935a6bc8dcb64f14fa5972d|O)

< (<http://example.org/alice/foaf.rdf#me>, 2d2095ff97c0cbf8fd210db91877cd8b|S)
> (20, 2d2095ff97c0cbf8fd210db91877cd8b|S)

< (<http://example.org/alice/foaf.rdf#me>, e106576560405e9c91aa4a28d1b35b34|S)
> (20, e106576560405e9c91aa4a28d1b35b34|S)

< (<http://example.org/alice/foaf.rdf#me>, 48bc9707b09e5574ba50d4f58f0e5fec|S)
> (20, 48bc9707b09e5574ba50d4f58f0e5fec|S)

< (<http://example.org/alice/foaf.rdf#me>, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (20, 3e3e4c277b7db437f5cdf9bae65a02bc|S)

< (<http://example.org/alice/foaf.rdf#me>, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (20, 3e3e4c277b7db437f5cdf9bae65a02bc|S)

< (<http://example.org/alice/foaf.rdf#me>, 979a843d6b468cfa7787b361a7cfbee9|S)
> (20, 979a843d6b468cfa7787b361a7cfbee9|S)

< (<http://example.org/alice/foaf.rdf#me>, 4f5e5bc047a1d8e5928ed8a6f8297dcf|S)
> (20, 4f5e5bc047a1d8e5928ed8a6f8297dcf|S)

< (<http://example.org/alice/foaf.rdf>, 36abd40cbf6d7f33ae6814797a2a16a1|G)
> (62, 36abd40cbf6d7f33ae6814797a2a16a1|G)

< (<http://example.org/alice/foaf.rdf>, e106576560405e9c91aa4a28d1b35b34|G)
> (62, e106576560405e9c91aa4a28d1b35b34|G)

< (<http://example.org/alice/foaf.rdf>, 48bc9707b09e5574ba50d4f58f0e5fec|G)
> (62, 48bc9707b09e5574ba50d4f58f0e5fec|G)

< (<http://example.org/alice/foaf.rdf>, 2d2095ff97c0cbf8fd210db91877cd8b|G)
> (62, 2d2095ff97c0cbf8fd210db91877cd8b|G)

< (<http://example.org/alice/foaf.rdf>, ede8d3b91138dc5f5ee6b0aada620769|G)
> (62, ede8d3b91138dc5f5ee6b0aada620769|G)

< (<http://example.org/alice/foaf.rdf>, a9752b388fdb17a7fec198626199d1f4|G)
> (62, a9752b388fdb17a7fec198626199d1f4|G)

< (<http://example.org/alice/foaf.rdf>, 5aaffc31a108b4c91177a86ef614e479|G)
> (62, 5aaffc31a108b4c91177a86ef614e479|G)

< (<http://example.org/bob/>, 8b16f433141d4e73c9af1483ab32fcf4|O)
> (101, 8b16f433141d4e73c9af1483ab32fcf4|O)

< (<http://example.org/bob/>, a9752b388fdb17a7fec198626199d1f4|O)
> (101, a9752b388fdb17a7fec198626199d1f4|O)

< (<http://example.org/bob/foaf.rdf#me>, 8b16f433141d4e73c9af1483ab32fcf4|S)
> (130, 8b16f433141d4e73c9af1483ab32fcf4|S)

< (<http://example.org/bob/foaf.rdf#me>, 4c39149e9ecced8fcd10fac086ec9c4c|S)
> (130, 4c39149e9ecced8fcd10fac086ec9c4c|S)

< (<http://example.org/bob/foaf.rdf#me>, 6e03d3250935a6bc8dcb64f14fa5972d|S)
> (130, 6e03d3250935a6bc8dcb64f14fa5972d|S)

< (<http://example.org/bob/foaf.rdf>, 8b16f433141d4e73c9af1483ab32fcf4|G)
> (170, 8b16f433141d4e73c9af1483ab32fcf4|G)

< (<http://example.org/bob/foaf.rdf>, ede8d3b91138dc5f5ee6b0aada620769|O)
> (170, ede8d3b91138dc5f5ee6b0aada620769|O)

< (<http://example.org/bob/foaf.rdf>, 4c39149e9ecced8fcd10fac086ec9c4c|G)
> (170, 4c39149e9ecced8fcd10fac086ec9c4c|G)

< (<http://example.org/bob/foaf.rdf>, 6e03d3250935a6bc8dcb64f14fa5972d|G)
> (170, 6e03d3250935a6bc8dcb64f14fa5972d|G)

< (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, 4c39149e9ecced8fcd10fac086ec9c4c|P)
> (207, 4c39149e9ecced8fcd10fac086ec9c4c|P)

< (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, 5aaffc31a108b4c91177a86ef614e479|P)
> (207, 5aaffc31a108b4c91177a86ef614e479|P)

< (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, e106576560405e9c91aa4a28d1b35b34|P)
> (207, e106576560405e9c91aa4a28d1b35b34|P)

< (<http://www.w3.org/2000/01/rdf-schema#seeAlso>, ede8d3b91138dc5f5ee6b0aada620769|P)
> (260, ede8d3b91138dc5f5ee6b0aada620769|P)

< (<http://xmlns.com/foaf/0.1/Person>, 5aaffc31a108b4c91177a86ef614e479|O)
> (310, 5aaffc31a108b4c91177a86ef614e479|O)

< (<http://xmlns.com/foaf/0.1/Person>, 4c39149e9ecced8fcd10fac086ec9c4c|O)
> (310, 4c39149e9ecced8fcd10fac086ec9c4c|O)

< (<http://xmlns.com/foaf/0.1/Person>, e106576560405e9c91aa4a28d1b35b34|O)
> (310, e106576560405e9c91aa4a28d1b35b34|O)

< (<http://xmlns.com/foaf/0.1/homepage>, 8b16f433141d4e73c9af1483ab32fcf4|P)
> (348, 8b16f433141d4e73c9af1483ab32fcf4|P)

< (<http://xmlns.com/foaf/0.1/homepage>, a9752b388fdb17a7fec198626199d1f4|P)
> (348, a9752b388fdb17a7fec198626199d1f4|P)

< (<http://xmlns.com/foaf/0.1/knows>, 2d2095ff97c0cbf8fd210db91877cd8b|P)
> (388, 2d2095ff97c0cbf8fd210db91877cd8b|P)

< (<http://xmlns.com/foaf/0.1/knows>, 4f5e5bc047a1d8e5928ed8a6f8297dcf|P)
> (388, 4f5e5bc047a1d8e5928ed8a6f8297dcf|P)

< (<http://xmlns.com/foaf/0.1/mbox>, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (425, 3e3e4c277b7db437f5cdf9bae65a02bc|P)

< (<http://xmlns.com/foaf/0.1/mbox>, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (425, 3e3e4c277b7db437f5cdf9bae65a02bc|P)

< (<http://xmlns.com/foaf/0.1/name>, 979a843d6b468cfa7787b361a7cfbee9|P)
> (461, 979a843d6b468cfa7787b361a7cfbee9|P)

< (<http://xmlns.com/foaf/0.1/name>, 6e03d3250935a6bc8dcb64f14fa5972d|P)
> (461, 6e03d3250935a6bc8dcb64f14fa5972d|P)

< (<http://xmlns.com/foaf/0.1/name>, 48bc9707b09e5574ba50d4f58f0e5fec|P)
> (461, 48bc9707b09e5574ba50d4f58f0e5fec|P)

< (<http://xmlns.com/foaf/0.1/name>, 36abd40cbf6d7f33ae6814797a2a16a1|P)
> (461, 36abd40cbf6d7f33ae6814797a2a16a1|P)

< (<mailto:alice@example.org>, 3e3e4c277b7db437f5cdf9bae65a02bc|O)
> (497, 3e3e4c277b7db437f5cdf9bae65a02bc|O)

< (<mailto:alice@example.org>, 3e3e4c277b7db437f5cdf9bae65a02bc|O)
> (497, 3e3e4c277b7db437f5cdf9bae65a02bc|O)

< (_:mrbnode_-459157714_-2124521357_bnode1, 4f5e5bc047a1d8e5928ed8a6f8297dcf|O)
> (527, 4f5e5bc047a1d8e5928ed8a6f8297dcf|O)

< (_:mrbnode_-459157714_-2124521360_bnode1, 36abd40cbf6d7f33ae6814797a2a16a1|S)
> (572, 36abd40cbf6d7f33ae6814797a2a16a1|S)

< (_:mrbnode_-459157714_-2124521360_bnode1, ede8d3b91138dc5f5ee6b0aada620769|S)
> (572, ede8d3b91138dc5f5ee6b0aada620769|S)

< (_:mrbnode_-459157714_-2124521360_bnode1, a9752b388fdb17a7fec198626199d1f4|S)
> (572, a9752b388fdb17a7fec198626199d1f4|S)

< (_:mrbnode_-459157714_-2124521360_bnode1, 5aaffc31a108b4c91177a86ef614e479|S)
> (572, 5aaffc31a108b4c91177a86ef614e479|S)


< (_:mrbnode_-459157714_-2124521360_bnode1, 2d2095ff97c0cbf8fd210db91877cd8b|O)
> (572, 2d2095ff97c0cbf8fd210db91877cd8b|O)


Previously, only a single reducer was allowed in this step and it was problem,
this constraint has now been removed (kudos to Justin @ Talis for the idea).

The construction of the TDB node table fragments happens without checking if 
an RDF node exists or not, since the reducer will receive as key each RDF node 
only once. Therefore each RDF node can simply be appended to the object file,
no hash->id B+Tree indexes are built at this stage.


Third MapReduce job:

The map function of the third job simply swaps the unique identifier for each
triple|quad with its node id:


< (0, 979a843d6b468cfa7787b361a7cfbee9|O)
> (979a843d6b468cfa7787b361a7cfbee9, 0|O)

< (0, 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (48bc9707b09e5574ba50d4f58f0e5fec, 0|O)

< (11, 36abd40cbf6d7f33ae6814797a2a16a1|O)
> (36abd40cbf6d7f33ae6814797a2a16a1, 11|O)

< (11, 6e03d3250935a6bc8dcb64f14fa5972d|O)
> (6e03d3250935a6bc8dcb64f14fa5972d, 11|O)

< (20, 2d2095ff97c0cbf8fd210db91877cd8b|S)
> (2d2095ff97c0cbf8fd210db91877cd8b, 20|S)

< (20, e106576560405e9c91aa4a28d1b35b34|S)
> (e106576560405e9c91aa4a28d1b35b34, 20|S)

< (20, 48bc9707b09e5574ba50d4f58f0e5fec|S)
> (48bc9707b09e5574ba50d4f58f0e5fec, 20|S)

< (20, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 20|S)

< (20, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 20|S)

< (20, 979a843d6b468cfa7787b361a7cfbee9|S)
> (979a843d6b468cfa7787b361a7cfbee9, 20|S)

< (20, 4f5e5bc047a1d8e5928ed8a6f8297dcf|S)
> (4f5e5bc047a1d8e5928ed8a6f8297dcf, 20|S)

< (62, 36abd40cbf6d7f33ae6814797a2a16a1|G)
> (36abd40cbf6d7f33ae6814797a2a16a1, 62|G)

< (62, e106576560405e9c91aa4a28d1b35b34|G)
> (e106576560405e9c91aa4a28d1b35b34, 62|G)

< (62, 48bc9707b09e5574ba50d4f58f0e5fec|G)
> (48bc9707b09e5574ba50d4f58f0e5fec, 62|G)

< (62, 2d2095ff97c0cbf8fd210db91877cd8b|G)
> (2d2095ff97c0cbf8fd210db91877cd8b, 62|G)

< (62, ede8d3b91138dc5f5ee6b0aada620769|G)
> (ede8d3b91138dc5f5ee6b0aada620769, 62|G)

< (62, a9752b388fdb17a7fec198626199d1f4|G)
> (a9752b388fdb17a7fec198626199d1f4, 62|G)

< (62, 5aaffc31a108b4c91177a86ef614e479|G)
> (5aaffc31a108b4c91177a86ef614e479, 62|G)

< (101, 8b16f433141d4e73c9af1483ab32fcf4|O)
> (8b16f433141d4e73c9af1483ab32fcf4, 101|O)

< (101, a9752b388fdb17a7fec198626199d1f4|O)
> (a9752b388fdb17a7fec198626199d1f4, 101|O)

< (130, 8b16f433141d4e73c9af1483ab32fcf4|S)
> (8b16f433141d4e73c9af1483ab32fcf4, 130|S)

< (130, 4c39149e9ecced8fcd10fac086ec9c4c|S)
> (4c39149e9ecced8fcd10fac086ec9c4c, 130|S)

< (130, 6e03d3250935a6bc8dcb64f14fa5972d|S)
> (6e03d3250935a6bc8dcb64f14fa5972d, 130|S)

< (170, 8b16f433141d4e73c9af1483ab32fcf4|G)
> (8b16f433141d4e73c9af1483ab32fcf4, 170|G)

< (170, ede8d3b91138dc5f5ee6b0aada620769|O)
> (ede8d3b91138dc5f5ee6b0aada620769, 170|O)

< (170, 4c39149e9ecced8fcd10fac086ec9c4c|G)
> (4c39149e9ecced8fcd10fac086ec9c4c, 170|G)

< (170, 6e03d3250935a6bc8dcb64f14fa5972d|G)
> (6e03d3250935a6bc8dcb64f14fa5972d, 170|G)

< (207, 4c39149e9ecced8fcd10fac086ec9c4c|P)
> (4c39149e9ecced8fcd10fac086ec9c4c, 207|P)

< (207, 5aaffc31a108b4c91177a86ef614e479|P)
> (5aaffc31a108b4c91177a86ef614e479, 207|P)

< (207, e106576560405e9c91aa4a28d1b35b34|P)
> (e106576560405e9c91aa4a28d1b35b34, 207|P)

< (260, ede8d3b91138dc5f5ee6b0aada620769|P)
> (ede8d3b91138dc5f5ee6b0aada620769, 260|P)

< (310, 5aaffc31a108b4c91177a86ef614e479|O)
> (5aaffc31a108b4c91177a86ef614e479, 310|O)

< (310, 4c39149e9ecced8fcd10fac086ec9c4c|O)
> (4c39149e9ecced8fcd10fac086ec9c4c, 310|O)

< (310, e106576560405e9c91aa4a28d1b35b34|O)
> (e106576560405e9c91aa4a28d1b35b34, 310|O)

< (348, 8b16f433141d4e73c9af1483ab32fcf4|P)
> (8b16f433141d4e73c9af1483ab32fcf4, 348|P)

< (348, a9752b388fdb17a7fec198626199d1f4|P)
> (a9752b388fdb17a7fec198626199d1f4, 348|P)

< (388, 2d2095ff97c0cbf8fd210db91877cd8b|P)
> (2d2095ff97c0cbf8fd210db91877cd8b, 388|P)

< (388, 4f5e5bc047a1d8e5928ed8a6f8297dcf|P)
> (4f5e5bc047a1d8e5928ed8a6f8297dcf, 388|P)

< (425, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 425|P)

< (425, 3e3e4c277b7db437f5cdf9bae65a02bc|P)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 425|P)

< (461, 979a843d6b468cfa7787b361a7cfbee9|P)
> (979a843d6b468cfa7787b361a7cfbee9, 461|P)

< (461, 6e03d3250935a6bc8dcb64f14fa5972d|P)
> (6e03d3250935a6bc8dcb64f14fa5972d, 461|P)

< (461, 48bc9707b09e5574ba50d4f58f0e5fec|P)
> (48bc9707b09e5574ba50d4f58f0e5fec, 461|P)

< (461, 36abd40cbf6d7f33ae6814797a2a16a1|P)
> (36abd40cbf6d7f33ae6814797a2a16a1, 461|P)

< (497, 3e3e4c277b7db437f5cdf9bae65a02bc|O)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 497|O)

< (497, 3e3e4c277b7db437f5cdf9bae65a02bc|O)
> (3e3e4c277b7db437f5cdf9bae65a02bc, 497|O)

< (527, 4f5e5bc047a1d8e5928ed8a6f8297dcf|O)
> (4f5e5bc047a1d8e5928ed8a6f8297dcf, 527|O)

< (572, 36abd40cbf6d7f33ae6814797a2a16a1|S)
> (36abd40cbf6d7f33ae6814797a2a16a1, 572|S)

< (572, ede8d3b91138dc5f5ee6b0aada620769|S)
> (ede8d3b91138dc5f5ee6b0aada620769, 572|S)

< (572, a9752b388fdb17a7fec198626199d1f4|S)
> (a9752b388fdb17a7fec198626199d1f4, 572|S)

< (572, 5aaffc31a108b4c91177a86ef614e479|S)
> (5aaffc31a108b4c91177a86ef614e479, 572|S)

< (572, 2d2095ff97c0cbf8fd210db91877cd8b|O)
> (2d2095ff97c0cbf8fd210db91877cd8b, 572|O)


On the reduce function, each triple|quad can be reconstructed using ids
(duplicates are correctly eliminated):


< (2d2095ff97c0cbf8fd210db91877cd8b, 388|P)
< (2d2095ff97c0cbf8fd210db91877cd8b, 20|S)
< (2d2095ff97c0cbf8fd210db91877cd8b, 62|G)
< (2d2095ff97c0cbf8fd210db91877cd8b, 572|O)
> ((null), {20, 388, 572, 62, null})

< (36abd40cbf6d7f33ae6814797a2a16a1, 461|P)
< (36abd40cbf6d7f33ae6814797a2a16a1, 572|S)
< (36abd40cbf6d7f33ae6814797a2a16a1, 11|O)
< (36abd40cbf6d7f33ae6814797a2a16a1, 62|G)
> ((null), {572, 461, 11, 62, null})

< (3e3e4c277b7db437f5cdf9bae65a02bc, 497|O)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 497|O)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 20|S)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 20|S)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 425|P)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 425|P)
> ((null), {20, 425, 497, -1, null})

< (48bc9707b09e5574ba50d4f58f0e5fec, 0|O)
< (48bc9707b09e5574ba50d4f58f0e5fec, 20|S)
< (48bc9707b09e5574ba50d4f58f0e5fec, 62|G)
< (48bc9707b09e5574ba50d4f58f0e5fec, 461|P)
> ((null), {20, 461, 0, 62, null})

< (4c39149e9ecced8fcd10fac086ec9c4c, 207|P)
< (4c39149e9ecced8fcd10fac086ec9c4c, 130|S)
< (4c39149e9ecced8fcd10fac086ec9c4c, 170|G)
< (4c39149e9ecced8fcd10fac086ec9c4c, 310|O)
> ((null), {130, 207, 310, 170, null})

< (4f5e5bc047a1d8e5928ed8a6f8297dcf, 527|O)
< (4f5e5bc047a1d8e5928ed8a6f8297dcf, 20|S)
< (4f5e5bc047a1d8e5928ed8a6f8297dcf, 388|P)
> ((null), {20, 388, 527, -1, null})

< (5aaffc31a108b4c91177a86ef614e479, 207|P)
< (5aaffc31a108b4c91177a86ef614e479, 572|S)
< (5aaffc31a108b4c91177a86ef614e479, 310|O)
< (5aaffc31a108b4c91177a86ef614e479, 62|G)
> ((null), {572, 207, 310, 62, null})

< (6e03d3250935a6bc8dcb64f14fa5972d, 170|G)
< (6e03d3250935a6bc8dcb64f14fa5972d, 11|O)
< (6e03d3250935a6bc8dcb64f14fa5972d, 130|S)
< (6e03d3250935a6bc8dcb64f14fa5972d, 461|P)
> ((null), {130, 461, 11, 170, null})

< (8b16f433141d4e73c9af1483ab32fcf4, 348|P)
< (8b16f433141d4e73c9af1483ab32fcf4, 170|G)
< (8b16f433141d4e73c9af1483ab32fcf4, 130|S)
< (8b16f433141d4e73c9af1483ab32fcf4, 101|O)
> ((null), {130, 348, 101, 170, null})

< (979a843d6b468cfa7787b361a7cfbee9, 20|S)
< (979a843d6b468cfa7787b361a7cfbee9, 461|P)
< (979a843d6b468cfa7787b361a7cfbee9, 0|O)
> ((null), {20, 461, 0, -1, null})

< (a9752b388fdb17a7fec198626199d1f4, 62|G)
< (a9752b388fdb17a7fec198626199d1f4, 348|P)
< (a9752b388fdb17a7fec198626199d1f4, 572|S)
< (a9752b388fdb17a7fec198626199d1f4, 101|O)
> ((null), {572, 348, 101, 62, null})

< (e106576560405e9c91aa4a28d1b35b34, 310|O)
< (e106576560405e9c91aa4a28d1b35b34, 207|P)
< (e106576560405e9c91aa4a28d1b35b34, 62|G)
< (e106576560405e9c91aa4a28d1b35b34, 20|S)
> ((null), {20, 207, 310, 62, null})

< (ede8d3b91138dc5f5ee6b0aada620769, 572|S)
< (ede8d3b91138dc5f5ee6b0aada620769, 170|O)
< (ede8d3b91138dc5f5ee6b0aada620769, 260|P)
< (ede8d3b91138dc5f5ee6b0aada620769, 62|G)
> ((null), {572, 260, 170, 62, null})


Fourth MapReduce job:

The map function of the fourth (and last) job emits only keys. For each triple
(or quad) three (or six) keys are emitted. The key contains the ids in the
correct order which need to be used for the index they belong to. 
The MapReduce framework will do the sort (instead of the external UNIX sort as 
in tdbloader2) and on the reduce side we can generate the B+Tree indexes (SPO, 
POS, OSP, SPOG, etc.) as it is done for tdbloader2. 

This job is using 9 reducers (i.e. one per index). This could be a problem for
massive datasets. If this turn out to be a big scalability issue, we could do
a total sort using N reducer, a custom partitioner and a sampling scan over
the keys first. This is the input/output for the map function:


< ((null), {20, 388, 572, 62, null})
> ({20, 388, 572, 62, SPOG}, (null))
> ({388, 572, 20, 62, POSG}, (null))
> ({572, 20, 388, 62, OSPG}, (null))
> ({62, 20, 388, 572, GSPO}, (null))
> ({62, 388, 572, 20, GPOS}, (null))
> ({62, 572, 20, 388, GOSP}, (null))

< ((null), {572, 461, 11, 62, null})
> ({572, 461, 11, 62, SPOG}, (null))
> ({461, 11, 572, 62, POSG}, (null))
> ({11, 572, 461, 62, OSPG}, (null))
> ({62, 572, 461, 11, GSPO}, (null))
> ({62, 461, 11, 572, GPOS}, (null))
> ({62, 11, 572, 461, GOSP}, (null))

< ((null), {20, 425, 497, -1, null})
> ({20, 425, 497, -1, SPO}, (null))
> ({425, 497, 20, -1, POS}, (null))
> ({497, 20, 425, -1, OSP}, (null))

< ((null), {20, 461, 0, 62, null})
> ({20, 461, 0, 62, SPOG}, (null))
> ({461, 0, 20, 62, POSG}, (null))
> ({0, 20, 461, 62, OSPG}, (null))
> ({62, 20, 461, 0, GSPO}, (null))
> ({62, 461, 0, 20, GPOS}, (null))
> ({62, 0, 20, 461, GOSP}, (null))

< ((null), {130, 207, 310, 170, null})
> ({130, 207, 310, 170, SPOG}, (null))
> ({207, 310, 130, 170, POSG}, (null))
> ({310, 130, 207, 170, OSPG}, (null))
> ({170, 130, 207, 310, GSPO}, (null))
> ({170, 207, 310, 130, GPOS}, (null))
> ({170, 310, 130, 207, GOSP}, (null))

< ((null), {20, 388, 527, -1, null})
> ({20, 388, 527, -1, SPO}, (null))
> ({388, 527, 20, -1, POS}, (null))
> ({527, 20, 388, -1, OSP}, (null))

< ((null), {572, 207, 310, 62, null})
> ({572, 207, 310, 62, SPOG}, (null))
> ({207, 310, 572, 62, POSG}, (null))
> ({310, 572, 207, 62, OSPG}, (null))
> ({62, 572, 207, 310, GSPO}, (null))
> ({62, 207, 310, 572, GPOS}, (null))
> ({62, 310, 572, 207, GOSP}, (null))

< ((null), {130, 461, 11, 170, null})
> ({130, 461, 11, 170, SPOG}, (null))
> ({461, 11, 130, 170, POSG}, (null))
> ({11, 130, 461, 170, OSPG}, (null))
> ({170, 130, 461, 11, GSPO}, (null))
> ({170, 461, 11, 130, GPOS}, (null))
> ({170, 11, 130, 461, GOSP}, (null))

< ((null), {130, 348, 101, 170, null})
> ({130, 348, 101, 170, SPOG}, (null))
> ({348, 101, 130, 170, POSG}, (null))
> ({101, 130, 348, 170, OSPG}, (null))
> ({170, 130, 348, 101, GSPO}, (null))
> ({170, 348, 101, 130, GPOS}, (null))
> ({170, 101, 130, 348, GOSP}, (null))

< ((null), {20, 461, 0, -1, null})
> ({20, 461, 0, -1, SPO}, (null))
> ({461, 0, 20, -1, POS}, (null))
> ({0, 20, 461, -1, OSP}, (null))

< ((null), {572, 348, 101, 62, null})
> ({572, 348, 101, 62, SPOG}, (null))
> ({348, 101, 572, 62, POSG}, (null))
> ({101, 572, 348, 62, OSPG}, (null))
> ({62, 572, 348, 101, GSPO}, (null))
> ({62, 348, 101, 572, GPOS}, (null))
> ({62, 101, 572, 348, GOSP}, (null))

< ((null), {20, 207, 310, 62, null})
> ({20, 207, 310, 62, SPOG}, (null))
> ({207, 310, 20, 62, POSG}, (null))
> ({310, 20, 207, 62, OSPG}, (null))
> ({62, 20, 207, 310, GSPO}, (null))
> ({62, 207, 310, 20, GPOS}, (null))
> ({62, 310, 20, 207, GOSP}, (null))

< ((null), {572, 260, 170, 62, null})
> ({572, 260, 170, 62, SPOG}, (null))
> ({260, 170, 572, 62, POSG}, (null))
> ({170, 572, 260, 62, OSPG}, (null))
> ({62, 572, 260, 170, GSPO}, (null))
> ({62, 260, 170, 572, GPOS}, (null))
> ({62, 170, 572, 260, GOSP}, (null))


The reduce function does not emit any (key, value) pairs, it just creates 
the TDB B+Tree indexes.

tdbloader3 will also copy the TDB indexes locally and reconstruct the 
node2id.dat|idn B+Tree index for the node table from the nodes.dat.
This is the only step done serially. It should not be a scalability problem.
However, if this is an issue, a similar technique as the other B+Tree
indexes can be used (but one or two additional MapReduce jobs might be
necessary).


Acknowledgments
---------------

Thanks to my employer which allows me to be creative, work on interesting
stuff and share it with the rest of the world. Thanks to Sam (my boss) who
gives me freedom, encouragement and (once I have a prototype) time to work
on this stuff. Thanks to Justin for the offset ideas. Charles for a possibly
good optimisation which I've not implemented yet. Alan, Tom and all my other
colleagues @ Talis who from time to time patiently listened me trying to
explain (and probably failed to clearly do so) what I was trying to do.


                                         -- Paolo Castagna, Talis Systems Ltd.
