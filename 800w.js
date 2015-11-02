"use strict"
var htmlparser = require('htmlparser2');
var fs = require('fs');
var crypto = require('crypto');
var mongo = require('mongodb').MongoClient;
var urlp = require('url');
var http = require('http');
var https = require('https');
var logger = new (require('bunyan'))({
  name: '800w',
  streams: [
    {
      stream:process.stdout,
      level:'info'
    },
    {
      path:'800w.log',
      level:'trace'
    }
  ]
});

var mongourl = 'mongodb://localhost:27017/800w';

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
    logger.trace(`stop state:${_self.state} srs:${_srs} length:'${_q.length } pending: ${ _pending}` );
  }

  this.pause = function pause_queue(){
    _self.state = 'paused';
    logger.trace(`pause state:${_self.state} srs:${_srs} length:'${_q.length } pending: ${ _pending}` );
  }

  this.resume = function resume_queue(){
    _self.state = 'running';
    logger.trace(`resume state:${_self.state} srs:${_srs} length:'${_q.length } pending: ${ _pending}` );
    return run();
  }

  this.enqueue = function(f){
    _q.push(f);
    logger.trace(`enqueue state:${_self.state} srs:${_srs} length:'${_q.length } pending: ${ _pending}` );
  }

  function run(){
    logger.debug(`state:${_self.state} srs:${_srs} length:${_q.length } pending: ${ _pending}` );
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
                //if(_srs > msr)_srs = msr;
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
                //if(_srs > msr)_srs = msr;
              }

              return run();
            }
          })
        })(_q.shift());
      }return true;
    } else if(_q.length == 0 && _pending == 0)end();
      else logger.warn(`still running ${ _self.state} ${_q.length} ${_pending}`);
    return false;
  }

}

function connect(cb){
  mongo.connect(mongourl,function(err,db){
    if(err != null) throw err;
    if(db != null){
        db.on('error',function(e){
          logger.error('Cannot connect to database')
        })
        cb(db,function(){
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
          logger.debug('redirect '+ url + ' to ' + rurl + ' redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
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
        logger.debug('retry redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
        _r(base,url);
      }else err(e,redir,retry);
    });
  }
  logger.trace('request '+ url +' redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
  _r(base,url);

}

function scrape(base,db,q,url,abc,s,e){
  logger.info(`scrape ${base} ${url}`);
  var urls = []
  var stats = {};
  var _alphabet = abc.alphabet? new RegExp(`^[${abc.alphabet}]+$`,'g'): new RegExp('.+');
  var _lang = abc.lang?new RegExp(abc.lang): new RegExp(/.*/);
  var _accept = typeof abc.accepturl === 'function'? abc.accepturl:function(url){return url.indexOf(base) == 0;};
  request(base,url,function(res){
    if(res == null){
      s();
      return;
    }
    var hash = crypto.createHash('sha256');
    var parser = new htmlparser.Parser({
      ontext: function parseText(text){
        if(this.parseText){
          hash.update(text);
          var words = text.split(" ");
          for(let w of words){
            var matches =  w.toLowerCase().match(_alphabet);
            if(matches)
              for(let m of matches)
                stats[m] = stats.hasOwnProperty(m)?stats[m] + 1:1;
          }
        }
        return stats;
      },
      onopentag: function(tag,attr){
        if(tag === 'html')this.langParse = _lang.test(attr.lang || '');
        else if(this.langParse){
          if(attr.href){
              this.parseText = false;
              var purl = urlp.resolve(base,attr.href);
              var htag = purl.indexOf('#')
              if(htag > 0)purl = purl.substring(0,htag);
              if(purl != base && _accept(purl))
                urls = urls.concat(purl);
              else logger.trace(`Ignoring ${base} ${purl}`)
          }else this.parseText = true
        }
      }
    }, {decodeEntities: true});

    res.on('data',function(data){parser.write(data); }).on('end',function(){
      parser.end();
      var pagehash = hash.digest('hex');

      logger.trace(`finished parsing ${base} ${url} ${pagehash}`);

      db.collection('urls').findOneAndUpdate(
        {hash:pagehash},
        {
          $set:{url:url},
          $addToSet: {urls:{$each: urls}}
        },
        {upsert:true},
        function(err,r){
          if(err) throw err;
          if(r.value == null){
            // only process urls for a page which content was not processed before
            logger.info(`update stats ${base} ${url} ${pagehash}`);
            let blk = db.collection('words').initializeOrderedBulkOp();
            var exec = false;
            for(let w in stats){
              blk.find({w:w}).upsert().updateOne({$inc:{count:stats[w]}})
              exec = true;
            }
            if(exec)
              blk.execute(function(err,res){
                if(err) throw err;
                  check(base,db,q,abc,urls,s);
              })
            else s();
          }else{
             logger.trace(`found hash for ${base} ${url} ${pagehash}`);
             s();
          }

        }
      );

    })
  },function(err){
    e(false)
  });
}

function check(base,db,q,abc,urls,after){
  urls = urls || [base];
  logger.trace(`check base:${base} urls:${urls.length}`);
  var col = db.collection('urls')
  var blk = col.initializeOrderedBulkOp();
  for(let url of urls){
    blk.find({url:url}).upsert().updateOne({$set:{time:new Date()}});
  }
  blk.execute(function(err,r){
      if(err)throw err;
      logger.trace(`check updated to pending ${r.getUpsertedIds()}`);
      col.find({url:{$in:urls},status:{$ne:'checking'}}).toArray(function(err,items){
        if(err)throw err;
        logger.trace(`check found ${items.length}`);
        var lnt = items.length
        var checking = false;
        var enqueue = false;
        if(lnt){
          for(let u of items){
            col.findOneAndUpdate({_id:u._id},{$set:{status:'checking',time:new Date()}},function(err,r){
              if(err)throw err;
              r = r.value
              logger.trace(`check base:${base} url:${r.url} hash:${r.hash} proc:${lnt}`);
              if(r.hash){//already parsed
                if(r.urls && r.urls.length){
                  check(base,db,q,abc,r.urls,after);
                  checking = true;
                }
              }else{
                q.enqueue(function(succ,err){
                  scrape(base,db,q,r.url,abc,succ,err)
                })
                enqueue = true;
              }
              lnt--;
              if(lnt == 0){
                if(!checking && typeof after === 'function'){
                  logger.trace(`check call after base:${base} url:${r.url} hash:${r.hash} proc:${lnt}`);
                  after();
                }
                if(enqueue){
                  logger.trace(`check resume queue base:${base} url:${r.url} hash:${r.hash} proc:${lnt}`);
                  q.resume();
                }
              }
            });
          }
        }else if(typeof after === 'function'){
          logger.trace(`check call after base:${base} url:${r.url} hash:${r.hash} proc:${lnt}`);
          after();
        }
      });
    }
  )
}


connect(function(db,close){
  db.collection('urls').updateMany({},{$set:{status:null,time:new Date()}},function(err,r){
    if(err)throw err;
    check('http://www.jw.org/ht',db,new queue(100,function(){
        db.collection('words').find({stop:{$eq:false}}).sort({'count': -1}).limit(800)
          .toArray(function(err,docs){
            if(err) throw err;
            logger.info(docs);
            close();
          });
        }),{lang:'ht',alphabet:'abcdeèfgijklmnoòprstuvwyz',accepturl:function(url){
          return url.indexOf('http://www.jw.org/ht') == 0 || url.indexOf('http://wol.jw.org/ht') == 0
        }});
  })
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
