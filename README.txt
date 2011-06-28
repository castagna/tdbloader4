TDB Loader 3
------------

This is a small prototype to investigate whether it is possible (and faster 
or cheaper) to use MapReduce to create TDB indexes.

In theory (see an message on the old jena-dev: [1]) this is possible. 
However, in practice, it's not done yet. This is another small step
towards practice. 

You can run tdbloader3 locally, see the Eclipse launcher.

If you want to override the output path use: -D overrideOutput=true.
When you run locally, if you want to validate the output use: -D verify=true.

Or, you can use your own Hadoop cluster or Apache Whirr to start one on EC2:

To start up an Hadoop cluster using Apache Whirr:

  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  cd /opt/
  curl -O http://www.apache.org/dist/incubator/whirr/whirr-0.5.0-incubating/whirr-0.5.0-incubating.tar.gz 
  tar zxf whirr-0.5.0-incubating.tar.gz
  ssh-keygen -t rsa -P '' -f ~/.ssh/whirr
  export PATH=$PATH:/opt/whirr-0.5.0-incubating/bin/
  whirr version
  whirr launch-cluster --config hadoop-ec2.properties --private-key-file ~/.ssh/whirr
  . ~/.whirr/hadoop/hadoop-proxy.sh
  # Proxy PAC configuration here: http://apache-hadoop-ec2.s3.amazonaws.com/proxy.pac

To copy stuff from your local disk or S3 into your HDFS cluster running on EC2:

  hadoop --config ~/.whirr/hadoop fs -mkdir /user/castagna/input
  hadoop --config ~/.whirr/hadoop fs -put src/test/resources/input/* /user/castagna/input

  hadoop distcp s3n://$AWS_ACCESS_KEY_ID_LIVE:$AWS_SECRET_ACCESS_KEY_LIVE@{bucketname} hdfs://{hostname}:8020/user/castagna/input

To launch the tdbloader3 MapReduce jobs:

  mvn hadoop:pack
  hadoop --config ~/.whirr/hadoop jar target/hadoop-deploy/tdbloader3-hdeploy.jar com.talis.labs.tdb.tdbloader3.tdbloader3 -D overrideOutput=true -D copyToLocal=true input target/output

To shutdown the cluster:

  whirr destroy-cluster --config hadoop-ec2.properties


How it works
------------

TDB Loader 3 is a sequence of three MapReduce jobs. 

The first MapReduce job builds the node table and produces the input for the 
next job. The second MapReduce job generates the input data using node ids 
rather than URIs, blank nodes or literals. The last MapReduce job reuses the 
same BPlusTreeRewriter [ ] which is used in tdbloader2 to finally create TDB 
indexes (i.e. SPO, POS, OSP, GSPO, etc.).

First MapReduce job:

The map function of the first job parses N-Triples or N-Quads files using RIOT
and it emits (key, value) pairs where the key is the RDF node and the value is
a unique identifier for that triple|quad concatenated with the position of the
RDF node itself in the triple|quad. For example, these are input (<) and output
(>) for two quads:

< (0, <http://example.org/alice/foaf.rdf#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/alice/foaf.rdf> .)
> (<http://example.org/alice/foaf.rdf#me>, e106576560405e9c91aa4a28d1b35b34|S)
> (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, e106576560405e9c91aa4a28d1b35b34|P)
> (<http://xmlns.com/foaf/0.1/Person>, e106576560405e9c91aa4a28d1b35b34|O)
> (<http://example.org/alice/foaf.rdf>, e106576560405e9c91aa4a28d1b35b34|G)

< (162, <http://example.org/alice/foaf.rdf#me> <http://xmlns.com/foaf/0.1/name> "Alice" <http://example.org/alice/foaf.rdf> .)
> (<http://example.org/alice/foaf.rdf#me>, 48bc9707b09e5574ba50d4f58f0e5fec|S)
> (<http://xmlns.com/foaf/0.1/name>, 48bc9707b09e5574ba50d4f58f0e5fec|P)
> ("Alice", 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (<http://example.org/alice/foaf.rdf>, 48bc9707b09e5574ba50d4f58f0e5fec|G)

The reduce function of the first job adds the RDF node received as key to the
node table and it emits a (key, value) pair with the node id and the same value
it has received. For example:

< ("Alice", 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (0, 48bc9707b09e5574ba50d4f58f0e5fec|O)
< ("Bob", 5a1522c8f0bb1b775146e3f0b8066c4d|O)
> (11, 5a1522c8f0bb1b775146e3f0b8066c4d|O)
< ("Bob", 6e03d3250935a6bc8dcb64f14fa5972d|O)
> (11, 6e03d3250935a6bc8dcb64f14fa5972d|O)
< (<http://example.org/alice/foaf.rdf#me>, e106576560405e9c91aa4a28d1b35b34|S)
> (20, e106576560405e9c91aa4a28d1b35b34|S)
< (<http://example.org/alice/foaf.rdf#me>, 48bc9707b09e5574ba50d4f58f0e5fec|S)
> (20, 48bc9707b09e5574ba50d4f58f0e5fec|S)
< (<http://example.org/alice/foaf.rdf#me>, 9fa6ff77d07e40a805d19be921a79e7d|S)
> (20, 9fa6ff77d07e40a805d19be921a79e7d|S)
< (<http://example.org/alice/foaf.rdf#me>, 3e3e4c277b7db437f5cdf9bae65a02bc|S)
> (20, 3e3e4c277b7db437f5cdf9bae65a02bc|S)

Only a single reducer is allowed in the first step (and this can be a problem).

The advantage is that the construction of the node table can happen without
checking if an RDF node exists or not, since the reducer will receive as key
each RDF node only once. Therefore each RDF node can simply be appended to the
object file and its id added to the hash->id B+Tree.

Second MapReduce job:

The map function of the second job swaps the unique identifier for each
triple|quad with the node id:

< (0, 0 48bc9707b09e5574ba50d4f58f0e5fec|O)
> (48bc9707b09e5574ba50d4f58f0e5fec, 0|O)
< (37, 11   5a1522c8f0bb1b775146e3f0b8066c4d|O)
> (5a1522c8f0bb1b775146e3f0b8066c4d, 11|O)
< (75, 11   6e03d3250935a6bc8dcb64f14fa5972d|O)
> (6e03d3250935a6bc8dcb64f14fa5972d, 11|O)

On the reduce function, each triple|quad can be reconstructed using ids:

< (07a91c76410d888875ace43331a11fcd, 348|P)
< (07a91c76410d888875ace43331a11fcd, 527|S)
< (07a91c76410d888875ace43331a11fcd, 101|O)
< (07a91c76410d888875ace43331a11fcd, 62|G)
> ((null), 527 348 101 62)

< (3e3e4c277b7db437f5cdf9bae65a02bc, 20|S)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 497|O)
< (3e3e4c277b7db437f5cdf9bae65a02bc, 425|P)
> ((null), 20 425 497)

Third MapReduce job:

The map function of the third (and last) job emits only keys. For each triple
three keys are emitted; while for each quad four keys are emitted. The key 
contains the ids in hex format (exactly the same format used by tdbloader2). 
The MapReduce framework will do the sort (instead of the external UNIX sort as 
in tdbloader2) and on the reduce side we can generate the B+Tree indexes (SPO, 
POS, OSP, SPOG, etc.) as it is done for tdbloader2. 

Currently only one reducer is used, but 9 reducers (i.e. one per index) should 
be used instead. This is not possible with the current Hadoop SNAPSHOTs since 
there seems to be a bug when multiple reducers are used.

This is an example of input/output for the map function:

< (0, 527 348 101 62)
> (000000000000020F 000000000000015C 0000000000000065 000000000000003E|SPOG, (null))
> (000000000000020F 000000000000015C 0000000000000065 000000000000003E|SPOG, (null))
> (000000000000015C 0000000000000065 000000000000020F 000000000000003E|POSG, (null))
> (0000000000000065 000000000000020F 000000000000015C 000000000000003E|OSPG, (null))
> (000000000000003E 000000000000020F 000000000000015C 0000000000000065|GSPO, (null))
> (000000000000003E 000000000000015C 0000000000000065 000000000000020F|GPOS, (null))
> (000000000000003E 0000000000000065 000000000000020F 000000000000015C|GOSP, (null))
< (15, 20 425 497)
> (0000000000000014 00000000000001A9 00000000000001F1|SPO, (null))
> (00000000000001A9 00000000000001F1 0000000000000014|POS, (null))
> (00000000000001F1 0000000000000014 00000000000001A9|OSP, (null))

The reduce function does not emit any (key, value) pairs, it just creates the
B+Tree indxes.



                                                          -- Paolo Castagna

 [1] http://tech.groups.yahoo.com/group/jena-dev/message/46040
