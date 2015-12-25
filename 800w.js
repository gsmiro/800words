"use strict"
const util = require('util');
const EventEmitter = require('events');
const htmlparser = require('htmlparser2');
const fs = require('fs');
const crypto = require('crypto');
const mongo = require('mongodb').MongoClient;
const urlp = require('url');
const http = require('http');
const https = require('https');
const heapdump = require('heapdump');
const logger = new (require('bunyan'))({
  name: '800w',
  streams: [
    {
      stream:process.stdout,
      level:'trace'
    },
    {
      path:'800w.log',
      level:'info'
    }
  ]
});

const mongourl = 'mongodb://localhost:27017/800w';

function MongoBackend(db,opts){
  const _col = opts && opts.collection || 'queue'

  function _q(f,v){
    let _q = {};
    _q[f] = v;
    return _q;
  }

  function _shnd(it){};
  function _ehnd(err){ throw err; };

  const _self = this;

  this.start = function(sh,eh){
    const _eh = typeof eh === 'function'?eh:_ehnd;
    db.collection(_col,function(err,col){
      if(err)_eh(err)
      var run = true;
      col.find({}).limit(1).maxScan(1).forEach(function(it){
          run = false;
          col.drop(function(err,rep){
            if(err)_eh(err);
            sh(_self,rep);
          });
      },function(err){
        if(err)_eh(err)
        if(run)sh(_self,null);
      })
    })
  }

  this.enqueue = function(item,sh,eh){
    const _sh = typeof sh === 'function'?sh:_shnd;
    const _eh = typeof eh === 'function'?eh:_ehnd;
    var _item = [];
    if(item && item.length){
      var blk = db.collection(_col).initializeOrderedBulkOp();
      for(let i of item){
        blk.find({_id:i}).upsert().updateOne({$set:{time:new Date()}});
      }
      blk.execute(function(err,res){
          if(err)_eh(err)
          _sh(res);
        }
      )
    }
  }
  this.stop = function(sh,eh){
    const _sh = typeof sh === 'function'?sh:_shnd;
    const _eh = typeof eh === 'function'?eh:_ehnd;
    db.collection(_col).drop(function(err,res){
      if(err)_eh(err);
      _sh(res);
    })
  }

  this.request = function(num,func,eh){
    let col = db.collection(_col);
    const _eh = typeof eh === 'function'?eh:_ehnd;
    col.find({}).sort({time:-1}).limit(num).forEach(function(item){
      if(item){
        col.deleteOne({_id:item._id},function(err,res){
          if(err)_eh(err);
          func(item._id);
        })
      }else{
        func(null)
      }
    },function(err){
      if(err)_eh(err);
      func(null);
    });
  }
}

function Queue(msr,bck,f){
  EventEmitter.call(this);
  this.state = 'running'
  const _self = this;
  var _pending = 0;
  var _srs = msr;
  this.stop = function stop_queue(){
    bck.stop(function(r){
      _self.state = 'stopped';
      _self.emit('stop',r);
      logger.trace(`stop state:${_self.state} srs:${_srs} pending:${ _pending}` );
    })
  }

  this.pause = function pause_queue(){
    _self.state = 'paused';
    _self.emit('pause',_pending)
    logger.trace(`pause state:${_self.state} srs:${_srs} pending:${ _pending}` );
  }

  this.resume = function resume_queue(){
    _self.state = 'running';
    logger.trace(`resume state:${_self.state} srs:${_srs} pending:${ _pending}` );
    _self.emit('resume',_pending);
    return run();
  }

  this.enqueue = function(it,cb){
    bck.enqueue(it,function(ops){
      logger.trace(`enqueue state:${_self.state} srs:${_srs} pending:${ _pending}` );
      _self.emit('enqueue',ops);
      if(cb)cb(ops);
    },function(err){
      throw err;
    })
  }
  function run(){
    if(_self.state == 'running'){
      var req = _srs - _pending
      if(req > 0){
        bck.request(req,function(it){
          if(it == null){
            if(!_pending)run();
            return;
          }
          if(_pending > _srs){
            logger.debug(`requeue requested:${req} state:${_self.state} srs:${_srs} pending:${ _pending} diff:${_srs - _pending}` );
            _self.enqueue([it]);
          }else{
            logger.debug(`run requested:${req} state:${_self.state} srs:${_srs} pending:${ _pending} diff:${_srs - _pending}` );
            _pending++;
            f(it,function(){
              _pending--;
              logger.trace(`success state:${_self.state} srs:${_srs} pending:${ _pending}`);
              run();
            },function(requeue){
              _pending--;
              logger.trace(`fail state:${_self.state} srs:${_srs} pending:${ _pending} requeue:${requeue}`);
              if(requeue)_self.enqueue([it]);
              run();
            });
          }
        });
      }else logger.debug(`queue waiting requested:${req} state:${_self.state} srs:${_srs} pending:${ _pending} diff:${_srs - _pending}` );
    } else logger.info(`queue paused ${ _self.state} ${_pending}`);
  }
  bck.start(function(bck){
    logger.trace('backend started');
    _self.emit('start',_self);
  });

}
util.inherits(Queue,EventEmitter);
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

function request(url,acceptsurl,succ,err,maxRetry,maxRedir){
  var _succ = succ;
  succ = function(res){
    logger.trace(`success from request ${url}`);
    _succ(res);
  }
  err = typeof err == 'function' && err || function(res){
    throw res.statusCode + ' ' + res.statusMessage ;
  }
  maxRetry = Number(maxRetry) || 10;
  var retry = 0;
  maxRedir = Number(maxRedir) || 10;
  var redir = 0;

  function _r(url,acceptsurl){
    logger.info('request '+ url +' redir:'+ redir + ' maxRedir:'+maxRedir + ' retry:'+retry +' maxRetry:'+maxRetry);
    var proto = url.match(/https:.*/)?https:http;
    proto.get(url,function(res){
      if(res.statusCode === 200)succ(res);
      else if(res.statusCode > 300
        && res.statusCode < 400 && res.headers && res.headers.location
        && redir < maxRedir && retry < maxRetry){
        redir++
        retry = 0;
        var purl = urlp.parse(url);
        var rurl = urlp.resolve(purl.protocol + '//' + purl.host,res.headers.location);
        if(acceptsurl(rurl)){
          logger.debug(`redirect ${url} to ${rurl} redir:${redir} maxRedir:${maxRedir} retry:${retry} maxRetry:${maxRetry}`);
          _r(rurl,acceptsurl);
        }else {
          logger.trace(`${url} to ${rurl} not accepted redir:${redir} maxRedir:${maxRedir} retry:${retry} maxRetry:${maxRetry}`);
          succ(null);
        }
      } else if(maxRedir == redir){
        err('MAX_REDIR',redir,retry);
      } else if(maxRetry == retry){
        err('MAX_RETRY',redir,retry);
      } else err(res,redir,retry);
    }).on('error',function(e){
      if(e.code == 'ECONNRESET' && retry < maxRetry){
        retry++;
        logger.debug(`retry ${url} redir: ${redir} maxRedir: ${maxRedir} retry:${retry} maxRetry:${maxRetry}`);
        _r(url,acceptsurl);
      }else err(e,redir,retry);
    });
  }
  _r(url,acceptsurl);

}

function scrape(url,db,q,abc,s,e){
  logger.debug(`scrape ${url}`);
  var urls = []
  var stats = {};
  var nwords = 0;
  const _alphabet = abc.alphabet? new RegExp(`^[${abc.alphabet}]+$`,'g'): new RegExp('.+');
  const _lang = abc.lang?new RegExp(abc.lang): new RegExp(/.*/);
  const _accept = typeof abc.accepturl === 'function'? abc.accepturl:function(purl){return purl.indexOf(url) == 0;};
  request(url,_accept,function(res){
    if(res == null){
      logger.trace(`${url} success from scrape  - null response`)
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
              for(let m of matches){
                if(stats.hasOwnProperty(m)){
                    stats[m]++
                }else{
                  nwords++;
                  stats[m] = 1;
                }

              }
          }
        }
      },
      onopentag: function(tag,attr){
        if(tag === 'html')this.langParse = _lang.test(attr.lang || '');
        else if(this.langParse){
          if(attr.href){
              this.parseText = false;
              var purl = urlp.resolve(url,attr.href);
              var htag = purl.indexOf('#')
              if(htag > 0)purl = purl.substring(0,htag);
              if(purl != url && _accept(purl))
                urls = urls.concat(purl);
          }else this.parseText = true
        }
      }
    }, {decodeEntities: true});

    res.on('data',function(data){parser.write(data); }).on('end',function(){
      parser.end();
      var pagehash = hash.digest('hex');

      logger.trace(`${url} finished parsing ${pagehash}`);
      const col = db.collection('urls');
      col.updateMany(
        {hash:pagehash},
        {$addToSet: {same:url}},
        {multi:true},
        function(err,r){
          if(err) throw err;
          logger.trace(`${url} found ${r.matchedCount} pages with same hash ${pagehash}`)
          const hashash = r.matchedCount
          col.updateOne({_id:url},
            {
              $set:{hash:pagehash},
              $addToSet: {urls:{$each: urls}}
            },
            function(err,r){
              if(err)throw err;
              if(nwords && !hashash && r.matchedCount){
                 // only process urls for a page which content was not processed before

                   logger.trace(`${url} process words ${pagehash}`);
                   let blk = db.collection('words').initializeOrderedBulkOp();
                   for(let w in stats){
                     blk.find({_id:w}).upsert().updateOne({$inc:{count:stats[w]}})
                   }
                   blk.execute(function(err,res){
                     if(err) throw err;
                     if(urls.length){
                       q.enqueue(urls,function(ops){
                         logger.debug(`${url} scraped and enqueue ${urls.length} urls ${pagehash}`);
                       });
                     }
                     s();
                   })
              }else {
                if(!r.matchedCount)logger.warn(`${url} scraped but not found`)
                logger.trace(`${url} success from scrape - words:${nwords} hashash:${hashash} modified:${r.matchedCount}`)
                s();
              }
            });
        }
      );

    })
  },function(err,redir,retry){
    logger.warn(err.statusCode?`${url} statusCode ${err.statusCode}`:err);
    e(false)
  });
}

function check(url,db,q,abc,s,e){
  const col = db.collection('urls')
  logger.trace(`checking ${url}`)
  col.findOneAndUpdate(
    {_id:url},
    {$set:{status:'check'}},
    {upsert:true,
     returnOriginal:true},
    function(err,r){
      if(err)throw err;
      r = r.value;
      if(r == null){
        logger.trace(`${url} not found`);
        scrape(url,db,q,abc,s,e);
      }else if(r.status != 'check'){
        if(r.hash){//already parsed
          if(r.urls && r.urls.length){
            q.enqueue(r.urls,function(ops){
              logger.debug(`${r._id} checked and enqueue ${r.urls && r.urls.length || 0} ${r.hash}`)
              s();
            });
          }else{
            logger.trace(`${url} had no urls to enqueue`);
            s();
          }
        }else {
          logger.trace(`${url} found but not checked status:${r.status} hash:${r.hash}`);
          scrape(url,db,q,abc,s,e);
        }
      }else {
        logger.trace(`${url} success from check - already checking`);
        s();
      }
    }
  )
}


connect(function(db,close){
  const abc = {
      lang:'ht',
      alphabet:'abcdeèfghijklmnoòprstuvwyz',
      accepturl:function(url){
        return (url.indexOf('http://www.jw.org/ht') == 0
          || url.indexOf('http://wol.jw.org/ht') == 0)
          && url.indexOf('.gif') == -1
          && url.indexOf('.jpeg') == -1
          && url.indexOf('.jpg') == -1
          && url.indexOf('.mp3') == -1
          && url.indexOf('.mp4') == -1
          && url.indexOf('.mov') == -1
          && url.indexOf('.aac') == -1
          && url.indexOf('.pdf') == -1
          && url.indexOf('.GIF') == -1
          && url.indexOf('.JPEG') == -1
          && url.indexOf('.JPG') == -1
          && url.indexOf('.MP3') == -1
          && url.indexOf('.MP4') == -1
          && url.indexOf('.MOV') == -1
          && url.indexOf('.AAC') == -1
          && url.indexOf('.PDF') == -1
          && url.indexOf('choose-language') == -1;
      }
    }
  db.collection('urls').updateMany({},{$set:{status:null,time:new Date()}},function(err,r){
    logger.info('cleaned urls');
    if(err)throw err;
    var q = new Queue(100,new MongoBackend(db)
    ,function(it,s,e){
      check(it,db,q,abc,s,e)
    });
    q.on('start',function(q){
      logger.info('starting');
      q.enqueue(['http://www.jw.org/ht','http://wol.jw.org/ht'],
        function(ops){
          q.resume();
        }
      );
    });
  })
});
