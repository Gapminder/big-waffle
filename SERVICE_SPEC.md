# DDF Service HTTP Protocol Specification

## Introduction

A DDF Service processes [DDF Query Language](link) requests for one or more DDF datasets. Typically such requests are made by visualisation software such a VIzabi tools, but of course other clients and usage is perfectly possible.

The DDF Query Language specification defines the structure and semantics of queries but does not explain how to submit requests and what responses should look like. This document specifies a protocol for working with DDF datasets over HTTP. As such it specifies requirements for DDF Service implementations and how clients should interact with such implementations.

[Gapminder](www.gapminder.org) develops and maintains the [BigWaffle](https://github.com/Gapminder/big-waffle) software that is an implementation of this specification. Gapminder also operates a publicly available DDF Service at big-waffle.gapminder.org, that hosts several of the datasets of the [Open Numbers](https://open-numbers.github.io/) initiative.

## Foundations

A HTTP DDF Service MUST support the [HTTP 1.1 protocol](https://tools.ietf.org/html/rfc7230). It SHOULD support secure transport, [TLS 1.2](https://tools.ietf.org/html/rfc5246). The service SHOULD be available on standard ports: 80 for non-secure connections, and 443 for secure connections.

## List datasets

A DDF Service MUST offer an endpoint that responds with a list of the available datasets. This endpoint SHOULD be the root of the service, i.e. http://\<domainname\>:\<port\>**/**

To obtain a list with the available datasets a client issues a GET request for the URL of the endoint, "/". 
The service MUST respond with a HTTP response with the status code 200, and the ["application/json" Content-Type](https://www.iana.org/assignments/media-types/application/json). 
The body of the response MUST be the [JSON](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf) representation of an array with one or more objects, **exactly one** for each version of each dataset. Each such object MUST have the following properties:

- name: a string with the name of the dataset
- version: a string to indicate a particular version of the dataset

In addition each object in the response MAY have any of the following properties:

- default: a boolean, "true" indicates that the service will use this version of the named dataset for queries that do not indicate a version. For each dataset there MUST NOT be more than one version marked as default.
- description: a string with a short (max. 1000 chars) description of (this version of) the dataset
- href: a URL (string) to a human readable page with more information about the datatset (and version)

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

## Assets

## Caching

## Security considerations

