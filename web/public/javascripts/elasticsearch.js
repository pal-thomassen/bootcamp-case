(function () {

    function createQuery(fromTimestamp, toTimestamp) {
        return {
            query: {
                filtered: {
                    filter: {
                        range: {
                            timestamp_ms: {
                                gte: fromTimestamp,
                                lte: toTimestamp
                            }
                        }
                    }
                }
            },
            facets: {
                tags: { terms: {field: "place.country_code"} }
            },
            size: 0
        }
    }

    function parseActivity(response) {
        var terms = response.facets.tags.terms;
        var activity = {};
        terms.forEach(function (term) {
            activity[term.term] = term.count;
        });
        return activity;
    }

    function findActivityInCountriesBetween(fromTimestamp, toTimestamp, callback) {
        $.ajax({
            type: "POST",
            url: "http://localhost:9200/_search",
            contentType: "application/json",
            dataType: "json",
            data: JSON.stringify(createQuery(fromTimestamp, toTimestamp)),
            success: function (a) {
                callback(parseActivity(a));
            }
        });
    }

    // "exports"
    var ob = window.ob = window.ob || {};
    ob.findActivityInCountriesBetween = findActivityInCountriesBetween;

})();

