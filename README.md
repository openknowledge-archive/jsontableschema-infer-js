jts-infer
=========

Infer a
[JSON Table Schema](http://dataprotocols.org/json-table-schema/) from
a readable stream of a
[CSV](http://en.wikipedia.org/wiki/Comma-separated_values) source


[![NPM](https://nodei.co/npm/jts-infer.png)](https://nodei.co/npm/jts-infer/)


#Usage ```jtsInfer(readableStream, [options], callback)```


    var jtsInfer = require('jts-infer)
      , fs = require('fs);

    jstInfer(fs.createReadStream('path/to/data.csv'), function(err, schema, scores){
      //to something with schema [and score]
    });


##Options

An options hash can be specified

- separator: separator to separate cells in a row (default to ',')
- newline: separator to separate different rows (default to '\n')
- nSample: if specified only the ```nSample``` first rows of the source will be used to infer the types otherwise all the rows are used


#Tests

    npm test


#License

MIT
