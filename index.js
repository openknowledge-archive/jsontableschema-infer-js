var binaryCSV = require('binary-csv')
  , once = require('once');

module.exports = function(s, opts, callback){

  if(arguments.length === 2){
    callback = opts;
    opts = {};
  }

  if('nSample' in opts){
    opts.nSample = (opts.nSample > 0) ? opts.nSample: 1;
  }

  callback = once(callback);
  
  var parser = binaryCSV(opts)
    , cnt = 0
    , headers = []
    , scores = [];

  s.pipe(parser)
    .on('data', function(line){
      var cells = parser.line(line).map(function(cell) {
        cell = parser.cell(cell);
        var out = cell.toString();
        // browser case - in browser we will have decoded to an array for some reason
        if (out == '[object Uint8Array]') {
          out = String.fromCharCode.apply(null, cell);
        }
        return out;
      });
      if(cnt === 0){

        cells.forEach(function(x){
          headers.push(x);

          scores.push({
            string: 0,
            number: 0,
            integer: 0,
            date: 0,
            datetime: 0,
            boolean: 0
          });
        });

      } else {

        cells.forEach(function(x, i){
          
          if( (x === 'true') || (x == '1') || (x == 'false') || (x == '0') ) {
            scores[i]['boolean']++;
          }
          
          if (/^(?:-?(?:[0-9]+))?(?:\.[0-9]*)?(?:[eE][\+\-]?(?:[0-9]+))?$/.test(x)){
            x = parseFloat(x, 10);
            if (x % 1 === 0) {
              scores[i]['integer']++;
            } else {
              scores[i]['number']++;
            }
          } else if (/^\d{4}-[01]\d-[0-3]\d$/.test(x)){
            scores[i]['date']++;
          } else if (/^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$/.test(x)){       //from http://www.pelagodesign.com/blog/2009/05/20/iso-8601-date-validation-that-doesnt-suck/
            scores[i]['datetime']++;           
          } else{
            scores[i]['string']++;
          }
          
        });

        if(cnt === opts.nSample){
          try{
            s.destroy();
          } catch(e){
            //console.error(e);
          };
          parser.end();
        }

      }

      cnt++;

    })
    .on('error', callback)
    .on('end', function(){

      var fields = [];

      headers.forEach(function(h, i){
        var score = scores[i];
        var mainType = 'string';

        Object.keys(score).forEach(function(t){
          if(score[t] > score[mainType]){
            mainType = t;
          }
        });

        if(mainType === 'integer' && score['number']){
          mainType = 'number';
        } else if( (mainType === 'string') && (score['string'] === score['boolean']) ){
          mainType = 'boolean';
        } else if( (mainType === 'integer') && (score['integer'] === score['boolean']) ){
          mainType = 'boolean';
        }

        fields.push({
          name: h,
          type: mainType
        });

      });
      
      callback(null, {fields: fields}, scores);
    });
  
};
