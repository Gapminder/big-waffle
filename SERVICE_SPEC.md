# DDF Service HTTP Protocol Specification

## Introduction

A DDF Service processes [DDF Query Language](link) requests for one or more DDF datasets. Typically such requests are made by visualisation software such as [Vizabi](https://github.com/vizabi), but of course other clients and usage is perfectly possible.

The DDF Query Language specification defines the structure and semantics of queries but does not explain how to submit requests and what responses should look like. This document specifies a protocol for working with DDF datasets over HTTP. As such it specifies requirements for DDF Service implementations and how clients should interact with such implementations.

[Gapminder](www.gapminder.org) develops and maintains the [BigWaffle](https://github.com/Gapminder/big-waffle) software that is an implementation of this specification. Gapminder also operates a publicly available DDF Service at big-waffle.gapminder.org, that hosts several of the datasets of the [Open Numbers](https://open-numbers.github.io/) initiative.

## Foundations

A HTTP DDF Service MUST support the [HTTP 1.1 protocol](https://tools.ietf.org/html/rfc7230). It SHOULD support secure transport, [TLS 1.2](https://tools.ietf.org/html/rfc5246). The service SHOULD be available on standard ports: 80 for non-secure connections, and 443 for secure connections.

## List datasets

A DDF Service MUST offer an endpoint that responds with a list of the available datasets. This endpoint SHOULD be the root of the service, i.e. `http://<domainname>:<port>`**/**

To obtain a list with the available datasets a client issues a GET request for the URL of the endoint, "/". 
The service MUST respond with a HTTP response with the status code 200, and the ["application/json" Content-Type](https://www.iana.org/assignments/media-types/application/json). 
The body of the response MUST be the [JSON](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf) representation of an array with one or more objects, **exactly one** for each version of each dataset. Each such object MUST have the following properties:

- **name**: a string with the name of the dataset
- **version**: a string to indicate a particular version of the dataset

In addition each object in the response MAY have any of the following properties:

- **default**: a boolean, "true" indicates that the service will use this version of the named dataset for queries that do not indicate a version. For each dataset there MUST NOT be more than one version marked as default.
- **description**: a string with a short (max. 1000 chars) description of (this version of) the dataset
- **href**: a URL (string) to a human readable page with more information about the datatset (and version)

### Example


    GET / HTTP/1.1


    HTTP/1.1 200 OK
    Cache-Control: no-cache, no-store, must-revalidate
    Content-Type: application/json; charset=utf-8
    
    [
        {
            "name":"SG",
            "version":"2019022801",
            "default":true
        },
        {
            "name":"SG",
            "version":"2018113001"
        },
        {
            "name":"population",
            "version":"2018123101",
            "default":true
        }
    ]

## Query a dataset

A DDF Service MUST provide an endpoint that will process [DDF queries]() and respond with the results. This endpoint SHOULD be at `/<dataset name>/<dataset version>`. The dataset name and version should correspond to an entry in the list of datasets.
The DDF query is added to the URL as a query string (see below), and the method of the HTTP request MUST be a GET.

### DDF query encoding

The actual DDF query MUST be given as a [query string](https://tools.ietf.org/html/rfc3986#section-3.4). The query string is formed in two steps: 

1. The DDF query object MUST be encoded into JSON.
2. The JSON string MUST be encoded for inclusion into the URL of the HTTP request according to standard [URI percent encoding](https://tools.ietf.org/html/rfc3986#section-2). 

In *addition* a DDF Service MAY support [URL Object Notation](https://github.com/cerebral/urlon) (urlon encoding). The benefit is that the resulting query string is shorter and somewhat easier to read. 
If used, urlon encoding is applied in step 1 above, instead of JSON. Note that step 2 should still be applied, albeit that few, if any, characters will have to be percent encoded.

### DDF query respsonse

The response to a DDF Query is a JSON encoded object that MUST have the following properties:

- **header**: an array with the names of the keys and values in the _select_ property of the DDF query. Note that the order of the names in this header may be different from the order in the query.
- **rows**: an array with rows where each row is an array of values. The order of values in each row MUST correspond to the values in the _header_. The number of values in each row MUST be equal to the number of names in the _header_, and absent values in the dataset must be indicated by **null**. The response MUST NOT include any rows containing exclusively null values, i.e. no "empty" rows are allowed.
- **version**: a string indicating which version of the dataset was used to respond to the query. This MUST be string that can be used in DDF query requests as a version, and likewise must correspond to one of the versions in the list of datasets. Note that the version must always be included, even if the request explicitly indicated a version.

In addition the response object MAY have additional properties to convey information about the response. It is RECOMMENDED that the naming of such properties follows those of common practice in logging, e.g. an **info** property and/or a **warn** property could be included. The idea is to have a means to convey _useful_ information to a client developer.

### Error responses

The service MUST use standard HTTP error status codes. The Content-Type of error responses MUST be *text/html* or *text/plain*. The following error situations are particularly relevant:

- In case of syntactic or semantic errors in the issued DDF query the response MUST be **400** _Bad Request_. It is RECOMMENDED that the response body contains a one sentence explanation, in English, of the error. For example: `Query does not have 'select:'``

- In case of a query for a dataset or version that is not (or no longer) available the response MUST be **404** _Not Found_.

- In case the service is momentarily too busy, or overloaded, the service SHOULD respond with **503** _Service Unavailable_. As DDF datasets can be very large and queries rather complex it is quite possible that a service is temporarily too busy to respond to a new query in a timely manner, that is before the HTTP request times out at the client. Hence it is RECOMMENDED that a DDF Service responds with a 503 in case it is unlikely that the query will be answered within 30 seconds.

### Optional dataset version

Clients MAY omit the dataset version from the URL and a DDF Service MUST accept such requests, using whatever version of the targetted dataset it deems appropriate. However, the service MUST use the **default version** of the dataset targetted in the query, if the list of datasets includes a version marked as such.

It is RECOMMENDED that the DDF Service responds to such a request with a HTTP 302 redirect to the corresponding URL for the version of the dataset that will be used to answer the query. This way it is sraightfoward to manage the caching of query results as the results are always, and only, in a HTTP response for a specific version of a dataset.

Clients of the service SHOULD include the version in the request URL for related queries. This as the another version can become the default at any moment. So clients SHOULD use the **version** as given in a DDF query response as the version to use in subsequent, related, queries for targetted at the same dataset.

In practice a client often first issues a query to obtain information about e.g. the schema and/or entities of the dataset and only then issues a query for actual data. In such scenario the first request could be without a version. The response then includes the version used and the client uses that version to make the second query.

### Example

Here is an annotated example:

1. Client issues query for the Russian names of the concepts in the "SG" dataset. Note that no specific version is requested.

        GET /SG?_language%3Dru-RU%26from%3Dconcepts%26select_key%40%3Dconcept%3B%26value%40%3Dname%3B%3B%26order%2F_by%40%3Dname HTTP/1.1

2. Service responds with a 302 redirect, to essentially the same URL but with the version, "2019022801", included.

        HTTP/1.1 302 Found
        Location: /SG/2019022801?_language%3Dru-RU%26from%3Dconcepts%26select_key%40%3Dconcept%3B%26value%40%3Dname%3B%3B%26order%2F_by%40%3Dname

3. The client now issues a GET to the URL with the version

        GET /SG/2019022801?_language%3Dru-RU%26from%3Dconcepts%26select_key%40%3Dconcept%3B%26value%40%3Dname%3B%3B%26order%2F_by%40%3Dname HTTP/1.1

4. And the service responds with the results. Note the Cache-Control header and the version property in the response.

        HTTP/1.1 200 OK
        Content-Type: application/json; charset=utf-8
        Cache-Control: public, max-age=31536000, immutable
        Transfer-Encoding: chunked

        {
        "version":"2019022801",
        "header":["concept","name"],
        "rows": [
            ["broadband_subscribers_per_100_people","Broadband subscribers (per 100 people)"],
            ["time","Время"],
            ["co2_emissions_tonnes_per_person","Выбросы CO2 (тонн на человека)"],
            ["g77_and_oecd_countries","Группа 77 и стран ОЭСР"]
        ]}

## Assets

The [DDFcsv format](link here) allows for the author of a dataset to include _assets_. These are files with e.g images, vector maps, etc., that are impractical to include as values in the dataset proper. Instead the author uses references to such assets in the dataset and then adds the assets to the DDFcsv package.
Note that neither the DDF core specification, nor the DDF Query Language specification have the notion of _asset_. Hence, client software needs to know about assets through other means. Typically by direct communication between the author of the dataset and the developer of the visualisation that needs the assets.

So, it is RECOMMENDED that a DDF Service offers an endpoint to retrieve assets at: 

`/<dataset name>/<dataset version>/assets/<asset name>`

The HTTP request method MUST be a GET.

As with DDF queries the service MUST accept requests for assets that omit the dataset version. using whatever version of the targetted dataset it deems appropriate. However, the service MUST use the **default version** of the dataset targetted in the query, if the list of datasets includes a version marked as such.

The service MAY respond to an asset request with a 302 redirect, even if a version number is included. This makes it possible for a DDF Service to use a 3rd party service, such as e.g. Amazon S3, for serving assets.

## Directory

A DDF Service that for some reason cannot offer the List, Query and Asset endpoinst at the specified URLs SHOULD offer an endpoint that will respond to a directory request. It is RECOMMENDED that this endpoint is at `/ddf-service-directory`. It is quite likely though that if the DDF Service cannot adhere to the recommended URLs for the other endpoints it will also be impossible to follow the recommendation to this endpoint. In that case it is RECOMMENDED to ensure that the DNS entry for the service domain includes a TXT record with an attribute for `ddf-service-directory` with as value the path to the directory endpoint. For example:

    api.interesting-data.org   IN   TXT   "ddf-service-directory=/ddf/directory"

The HTTP method for a directory request MUST be `GET`.

The response MUST have the `application/json` Content-Type and as body the JSON encoding of an object that MUST have the following properties:

- **list**: string with the full path to the endpoint of for List requests.

- **query**: string with the full path to the endpoint for DDF Query requests, without any query string, but with uppercase "DATASET" and "VERSION" to indicate where in the URL a client should include the dataset resp. version in actual DDF Query requests.

- **assets**: string with the full path to the endpoint for Assets requests with uppercase "DATASET", "VERSION" and "ASSET" to indicate where in the URL a client should include the dataset, the version, and the asset name in actual asset requests. If the DDF Service does not offer an Asset endpoint the value of this property MUST be `null`.

### Example

    GET /ddf-service-directory HTTP/1.1
 

    HTTP/1.1 200 OK
    Content-Type: application/json; charset=utf-8
    
    {
        "list":"/",
        "query":"/DATASET/VERSION",
        "assets":"/DATASET/VERSION/assets/ASSET"
    }

## Caching

Most DDF datasets do not change very often, typically between once a month and once per year. At the same time the dataset can be very large and the queries complex. Hence it can be very effective to cache DDF query results. A DDF Service can use dataset versions to facilitate such caching. Whenever the dataset changes, or even if only the DDF service implementation changes such that results sets will change, the Service can allocate a new version to the dataset. Cached result sets will be for other versions and will not have to be invalidated, but simply will expire or will be pushed out.
This approach works if query responses are cached only if they are for requests with explicit dataset versions. Therefor this specification allows, and recommends, that the service responds to queries without a version with a redirect to a request with a version.

It is important to notice that the _version_ notion in this specification is, or at least can be, distinct from any versioning information that the author assigns to a dataset. DDF currently has no explicit versioning notion but it is likely that a future version of DDF will address that issue. For example a dataset could have an "edition" (similar to books) and perhaps a "revision". In analogy with books the _version_ in this specification is then like the hardcover, pocket or e-book version of a book: excactly the same content, but variations in the packing and presentation.

## Security considerations

Implementors and operators of a DDF Service should make sure that common best practices for web service security are adhered to. 
As DDF query processing may demand signifcant computational resources it is RECOMMENDED to validate DDF queries before embarking on what may be a relative expensive operation.
In case the DDF Service offers a sensitive dataset it SHOULD of course require appropriate authentication and authorization, and only allow requests over TLS.
