import org.elasticsearch.bootstrap.Elasticsearch;

public class ElasticSearchService {

    // Samples:
    //
    // http://localhost:9200/_search?q=*
    // http://localhost:9200/_search?q=lang:no
    // http://localhost:9200/twitter/tweet/dc524383-f6c9-4a76-b311-f0b92e5a0338

    /*
    POST http://localhost:9200/_search
    {
        "facets" : {
            "tags" : { "terms" : {"field" : "place.country_code"} }
        },
        "size" : 0
    }
    */


    /*
    POST http://localhost:9200/_search
    {
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "place.country_code" }
                }
            }
        }
    }
    */

    public static void main(String[] args) {
        System.setProperty("es.http.cors.enabled", "true");
        System.setProperty("es.http.cors.allow-origin", "*");
        Elasticsearch.main(new String[0]);
    }

}
