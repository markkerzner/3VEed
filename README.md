3VEed
=====

This project is for processing modern-day document streams, characterized by 3V's: variety, velocity and volume. It represents the first step in the IOT architecture, doing the document processing in real time.

It solves the following problems:

* Many document processing systems are distributed (think Hadoop), but this distribution is by batches. So if one node gets a 
large file to process (think PST), then it will be working for hours after the cluster is done. We need a system
that can bite a chunk of work and give the rest to its peer. This will achieve true distribution and load balancing.
* Dynamic data sources cannot be integrated into Hadoop or Hive. We need not only to add data sources with ease, 
but also accept new documents coming in real time (think emails and MS Exchange).

The 3VEed project aims to achieve these goals.

It is based on Apache Storm. The first practical use case is eDiscovery, where you have files of all sizes and types that do not fit batch paradigm.
Hence the name, which stands for 3V Electronic Evidence Discovery. However, it will work for multiple types of loads and applications. 
The batch version of the same is called FreeEed and is found right here, https://github.com/markkerzner/FreeEed.

Capabilities (current and future)

* processing 1400+ file types using Tika
* indexing text extracted from Office and other formats
* OCR
* search with Solr and Lucene

