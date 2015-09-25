# How to build
* Install movem [https://maven.apache.org/install.html]
* Clone de repository [https://github.com/mateusfreira/streaming-aggregation]
* In the project path type 
```
mvn package
```
in terminal you will  see between other things the folow mensage ``BUILD SUCCESS``

* After that you need to install the plugin...

```bash
sudo /path_to_elastichsearch/bin/plugin --remove streaming-aggregation 
```

```bash
sudo /path_to_elastichsearch/bin/plugin --url file:///path_to_the_project/target/releases/streaming-aggregation-1.0-SNAPSHOT.zip --install streaming-aggregation 
```
Restart the elasticseach : 
```bash
sudo service elasticsearch restart
```
#How to use
* First you need to create a 'query' with aggregations like the folow with the parameter 'createContext=true'
```bash
curl -XGET 'http://localhost:9200/index_name/_streaming_aggregation?createContext=true' -d '
{ 
    "size": 0,
    "query" : { 
        "filtered" : {
            "query" : { "match_all" : {}},
            "filter" : {
                    "range" : { 
                    "postDate" : { "from" : "2011-12-10", "to" : "2011-12-12" } 
                } 
            }
          }
    },
    "aggs" : {
        "sum_price" : { "sum" : { "field" : "price" }}
    } 
}'
```
* This request will return a ID like the folow
``` 
search1443144481695
```
All others request will need this ID

To get the aggregated(actual) value you can make a GET request
``` 
curl -XGET 'http://localhost:9200/index_name/_streaming_aggregation?pretty=true&id=search1443144481695'
``` 
Response sample:
```
{
  "took" : 3,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "failed" : 0
  },
  "hits" : {
    "total" : 8,
    "max_score" : 0.0,
    "hits" : [ ]
  },
  "aggregations" : {
    "min_price" : {
      "value" : 201.0,
      "value_as_string" : "201.0"
    }
  }
}
```
To each new document (or on some change in a document) you shuld  call the follow request
```bash 
curl -XPUT 'http://localhost:9200/index_name/p/2/_streaming_aggregation?newDocument=true&id=search1443144481695' -d '{
    "user": "dilbert", 
    "postDate": "2011-12-10", 
    "price": 60.00, 
    "body": "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat" ,
    "title": "Lorem ipsum"
}'
```
* Then you can call the GET request again to recive the new aggregated value...

``` 
curl -XGET 'http://localhost:9200/index_name/_streaming_aggregation?pretty=true&id=search1443144481695'
``` 
