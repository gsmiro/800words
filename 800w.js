"use strict"
var htmlparser = require('htmlparser2');
var fs = require('fs');
var mongo = require('mongodb').MongoClient;
var urlp = require('url');
var http = require('http');
var https = require('https');
var logger = new (require('bunyan'))({
  name: '800w'
});

var mongourl = 'mongodb://localhost:27017/800w';
var abc = 'abcdeèfgijklmnoòprstuvwyz';

function queue(msr,end){
  var _q = [];
  this.state = 'running'
  var _self = this;
  var _count = 0;
  var _fail = 0;
  var _srs = msr;
  this.stop = function stop_queue(){
    _self.state = 'stopped';
    _q = []
  }

  this.pause = function pause_queue(){
    _self.state = 'paused';
  }

  this.resume = function resume_queue(){
    _self.state = 'running';

    return run();
  }

  this.enqueue = function(f){
    _q.push(f);
    if(_self.state == 'running'){
      return run();
    }
  }

  function run(){
    if(_self.state == 'running' && _q.length && _count < _srs){
      logger.info('state:'+_self.state+' srs:'+_srs +' length:'+_q.length + ' count:' + _count );
      for(;_count < _srs && _q.length;_count++){
        (function call(f){
          f(function _succ(){
            _count--;
            if(_count == 0){
              _srs = _srs - _fail;
              if(_srs < 1){
                _srs = 1;
              }else if(_fail == 0){
                _srs++;
                if(_srs > msr)_srs = msr;
              }

              return run();
            }
          },function _err(){
            _count--;
            _fail++;
            _self.enqueue(f);
            if(_count == 0){
              _srs = _srs - _fail;
              if(_srs < 1){
                _srs = 1;
              }else if(_fail == 0){
                _srs++;
                if(_srs > msr)_srs = msr;
              }

              return run();
            }
          })
        })(_q.pop());
      }return true;
    }
    return false;
  }

}

function connect(cb){
  mongo.connect(mongourl,function(err,db){
    if(err != null) throw err;
    if(db != null){
        db.on('error',function(e){
          logger.info('Erro de Banco')
        })
        cb(db,function(){
          logger.info('closing...')
          db.close();
        });
    }
  })
}

function request(base,url,succ,err,redir,maxRedir){
  err = typeof err == 'function' && err || function(res){
    throw res.statusCode + ' ' + res.statusMessage ;
  }
  logger.info('request '+ url);
  maxRedir = Number(maxRedir) || 10;
  redir = Number(redir) || 0;
  var proto = url.match(/https:.*/)?https:http;
  proto.get(url,function(res){
    if(res.statusCode === 200)succ(res);
    else if(res.statusCode > 300 && res.statusCode < 400 && res.headers && res.headers.location && redir < maxRedir){
      if(res.headers.location.indexOf('/') == 0){
        var parsed = urlp.parse(base);
        request(base,parsed.protocol + '//' + parsed.host + res.headers.location,succ,err,redir + 1,maxRedir);
      }else if(res.headers.location.indexOf(base) == 0)
        request(base,res.headers.location,succ,err,redir + 1,maxRedir);
    }
  }).on('error',function(e){
    logger.warn(e);

    if(e.code == "ECONNRESET")
      err();
    else {
      succ();
    }
  });
}

function scrape(base,db,q,url,abc,s,e,end){
  logger.info('scrape ' + url);
  var urls = []
  var stats = {};
  request(base,url,function(res){
    if(res == null){
      s();
      return;
    }
    var parser = new htmlparser.Parser({
      ontext: function parseText(text){
          var words = text.split(" ");
          var rg = new RegExp('['+abc+']+','g');
          for(let w of words){
            var matches =  w.toLowerCase().match(rg);
            if(matches)
              for(let m of matches)
                stats[m] = stats.hasOwnProperty(m)?stats[m] + 1:1;
          }
          return stats;
      },
      onopentag: function(tag,attr){
        if(attr.hasOwnProperty('href')){
            var purl = urlp.resolve(base,attr.href);
            if(purl.indexOf(base) == 0)
              urls = urls.concat(purl);
        }
      }
    }, {decodeEntities: true});

    res.on('data',function(data){parser.write(data); }).on('end',function(){
      parser.end();
      logger.info('parsed ' + url);
      db.collection('urls').updateOne({'url':url},{$set: {'urls':urls}},{upsert:true},function(err,r){
        if(err){
          console.log(e)
          return e();
        }
        let blk = db.collection('words').initializeOrderedBulkOp();
        for(let w in stats){
          blk.find({'w':w}).upsert().updateOne({$inc:{'count':stats[w]}})
        }
        blk.execute(function(err,r){
          if(err){
            console.log(e)
            return e();
          }

          check(base,db,q,urls,end,abc);
          s();
        })

      });

    })
  },e);
}

function check(base,db,q,urls,end,abc){
  urls = urls || [base];
  db.collection('urls').find({'url':{$in : urls}}).each(function(err,furl){
    if(err) throw err;
    if(furl != null){
      urls.splice(urls.indexOf(furl),1);
    } else {
      for(let url of urls)
        q.enqueue(
          function(succ,err){
            scrape(base,db,q,url,abc,succ,err,end)
          }
        );
    }
  });
}


connect(function(db,close){
  check('http://wol.jw.org/ht',db,new queue(100),null,function(){
      db.collection('words').find().sort({'count': -1}).limit(800)
        .toArray(function(err,docs){
          logger.info(arguments);
          close();
        });
      },abc);
});
/*
var q = new queue(5,function(){
  logger.info('Finish');
});
q.pause();
for(let j = 0;j< 10;j++){
  (function a(j){
    q.enqueue(function(s,e){
      logger.info('get '+j)
      request('http://www.jw.org','http://www.jw.org'+(j % 3 == 0?j:'') ,s,e);
    })
  })(j);
}
q.resume();
*/
