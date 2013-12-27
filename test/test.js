var util = require('util')
  , assert = require('assert')
  , path = require('path')
  , Readable = require('stream').Readable
  , jtsInfer = require('..');


var fixture = [
  ['a', 'b', 'c', 'd', 'e', 'f'],
  ['2013-01-01', '2013-12-27T20:58:23.768Z', 'papaya', 10, 0.1, true],
  ['2013-01-02', '2013-12-28T20:58:23.768Z', 'lemon', 15, 0.2, false],
  ['2013-01-03', '2013-12-29T20:58:23.768Z', 'lemon', 15, 1e-3, true]
].map(function(x){return x.join(',')}).join('\n');

describe('jts-infer', function(){

  var s;

  beforeEach(function(){
    s = new Readable();
    s.push(fixture);
    s.push(null);    
  });
  
  it('should infer the JSON Table Schema', function(done){

    var expectedSchema =   { 
      fields: [
        { name: 'a', type: 'date' },
        { name: 'b', type: 'datetime' },
        { name: 'c', type: 'string' },
        { name: 'd', type: 'integer' },
        { name: 'e', type: 'number' },
        { name: 'f', type: 'boolean' } 
      ] 
    };
    
    var expectedScores = [
      { string: 0, number: 0, integer: 0, date: 3, datetime: 0, boolean: 0 },
      { string: 0, number: 0, integer: 0, date: 0, datetime: 3, boolean: 0 },
      { string: 3, number: 0, integer: 0, date: 0, datetime: 0, boolean: 0 },
      { string: 0, number: 0, integer: 3, date: 0, datetime: 0, boolean: 0 },
      { string: 0, number: 3, integer: 0, date: 0, datetime: 0, boolean: 0 },
      { string: 3, number: 0, integer: 0, date: 0, datetime: 0, boolean: 3 } 
    ];

    jtsInfer(s, function(err, schema, scores){
      if(err) throw err;  
      assert.deepEqual(schema, expectedSchema);
      assert.deepEqual(scores, expectedScores);
      done();
    });

  });

  it('should infer the JSON Table Schema using only nSample samples', function(done){
    jtsInfer(s, {nSample: 2}, function(err, schema, scores){
      if(err) throw err;        
      var expectedScores = [
        { string: 0, number: 0, integer: 0, date: 2, datetime: 0, boolean: 0 },
        { string: 0, number: 0, integer: 0, date: 0, datetime: 2, boolean: 0 },
        { string: 2, number: 0, integer: 0, date: 0, datetime: 0, boolean: 0 },
        { string: 0, number: 0, integer: 2, date: 0, datetime: 0, boolean: 0 },
        { string: 0, number: 2, integer: 0, date: 0, datetime: 0, boolean: 0 },
        { string: 2, number: 0, integer: 0, date: 0, datetime: 0, boolean: 2 } 
      ];

      assert.deepEqual(scores, expectedScores);
      done();      
      
    });

  });

});
