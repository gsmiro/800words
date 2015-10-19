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
  var _pending = 0;
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
    logger.info('resume');
    return run();
  }

  this.enqueue = function(f){
    _q.push(f);
  }

  function run(){
    logger.info(`state:${_self.state} srs:${_srs} length:'${_q.length } pending: ${ _pending}` );
    if(_self.state == 'running' && _q.length && _pending <= _srs){
      for(;_pending < _srs && _q.length;_pending++){
        (function call(f){
          f(function _succ(){
            _pending--;
            if(_pending == 0){
              _srs = _srs - _fail;
              if(_srs < 1){
                _srs = 1;
              }else if(_fail == 0){
                _srs++;
                if(_srs > msr)_srs = msr;
              }
              return run();
            }
          },function _err(requeue){
            _fail++;
            if(requeue)_self.enqueue(f);
            _pending--;
            if(_pending == 0){
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
    } else if(_q.length == 0 && _pending == 0)end();
    else logger.info(`${ _self.state} ${_q.length} ${_pending}`);
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

function request(base,url,succ,err,maxRetry,maxRedir){
  err = typeof err == 'function' && err || function(res){
    throw res.statusCode + ' ' + res.statusMessage ;
  }
  maxRetry = Number(maxRetry) || 10;
  var retry = Number(retry) || 0;
  maxRedir = Number(maxRedir) || 10;
  var redir = Number(redir) || 0;

  function _r(base,url){
    var proto = url.match(/https:.*/)?https:http;
    proto.get(url,function(res){
      if(res.statusCode === 200)succ(res);
      else if(res.statusCode > 300
        && res.statusCode < 400 && res.headers && res.headers.location
        && redir < maxRedir && retry < maxRetry){

        redir++
        retry = 0;
        var rurl = urlp.resolve(base,res.headers.location);
        if(rurl.indexOf(base) == 0){
          logger.info('redirect '+ url + ' to ' + rurl + ' redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
          _r(base,rurl);
        }
      } else if(maxRedir == redir){
        err('MAX_REDIR',redir,retry);
      } else if(maxRetry == retry){
        err('MAX_RETRY',redir,retry);
      } else err(res,redir,retry);
    }).on('error',function(e){
      if(e.code == 'ECONNRESET' && retry < maxRetry){
        retry++;
        logger.info('retry redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
        _r(base,url);
      }else err(e,redir,retry);
    });
  }
  logger.info('request '+ url +' redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
  _r(base,url);

}

function scrape(base,db,q,url,abc,s,e){
  logger.info(`scrape ${base} ${url}`);
  var urls = []
  var stats = {};
  request(base,url,function(res){
    if(res == null){
      s();
      return;
    }
    var parser = new htmlparser.Parser({
      ontext: function parseText(text){
        if(this.parseText){
          var words = text.split(" ");
          var rg = new RegExp(`^[${abc}]+$`,'g');
          for(let w of words){
            var matches =  w.toLowerCase().match(rg);
            if(matches)
              for(let m of matches)
                stats[m] = stats.hasOwnProperty(m)?stats[m] + 1:1;
          }
        }
        return stats;
      },
      onopentag: function(tag,attr){
        if(attr.hasOwnProperty('href')){
            this.parseText = false;
            var purl = urlp.resolve(base,attr.href);
            if(purl.indexOf(base) == 0)
              urls = urls.concat(purl);
        }else this.parseText = true;
      }
    }, {decodeEntities: true});

    res.on('data',function(data){parser.write(data); }).on('end',function(){
      parser.end();
      logger.info(`parsed ${base} ${url}`);
      db.collection('urls').updateOne({'url':url},{$set: {'urls':urls}},{upsert:true},function(err,r){
        if(err) throw err;
        logger.info(`updated ${base} ${url}`);
        let blk = db.collection('words').initializeOrderedBulkOp();
        var exec = false;
        for(let w in stats){
          blk.find({'w':w}).upsert().updateOne({$inc:{'count':stats[w]}})
          exec = true;
        }
        if(exec)
          blk.execute(function(err,r){
            if(err) throw err;
            logger.info(`upsert ${base} ${url.length}`);
            check(base,db,q,abc,urls,s);
          })
      });

    })
  },function(err){
    e(false)
  });
}

function check(base,db,q,abc,urls,after){
  urls = urls || [base];
  db.collection('urls').find({'url':{$in : urls}}).each(function(err,o){
    if(err) throw err;
    var furl = o && o.url
    if(furl != null){
      let idx =  urls.indexOf(furl);
      if(idx >= 0){
        urls.splice(urls.indexOf(furl),1);
      }
    } else {
      for(let url of urls){
        //logger.info(`check ${base} ${url} ${urls.length}`);
        q.enqueue(
          function(succ,err){
            scrape(base,db,q,url,abc,succ,err)
          }
        );
      }
      if(typeof after == 'function'){
        logger.info(`call after ${base} ${urls.length}`);
        after();
      }
      q.resume();
    }
  });
}


connect(function(db,close){
  check('http://wol.jw.org/ht',db,new queue(100,function(){
      db.collection('words').find().sort({'count': -1}).limit(800)
        .toArray(function(err,docs){
          if(err) throw err;
          logger.info(arguments);
          close();
        });
      }),abc);
});


/*var q = new queue(5,function(){
  logger.info('Finish');
});
for(let j = 0;j< 10;j++){
  (function a(j){
    q.enqueue(function(s,e){
      logger.info('get '+j)
      var url = 'http://www.jw.org'+(j % 3 == 0?j:'')
      request('http://www.jw.org',url ,function(res){
        logger.info('parsed '+ url);
        s();
      },function(err){
        logger.info(err);
        e(false);
      });
    })
  })(j);
}
q.resume();
*/
