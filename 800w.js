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
const logger = new (require('bunyan'))({
  name: '800w',
  streams: [
    {
      stream:process.stdout,
      level:'info'
    },
    {
      path:'800w.log',
      level:'debug'
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
    let col = db.collection(_col);
    const _eh = typeof eh === 'function'?eh:_ehnd;
    db.collection(_col).remove({},function(err,res){
      if(err)_eh(err);
      sh(_self,res.result);
    })
  }

  this.enqueue = function(item,sh,eh){
    const _sh = typeof sh === 'function'?sh:_shnd;
    const _eh = typeof eh === 'function'?eh:_ehnd;
    var _item = [];
    if(item && item.length){
      for(let i of item){
        _item.push({o:i});
      }
      db.collection(_col).insertMany(
        _item,
        function(err,res){
          if(err)_eh(err)
          for(let i of res.ops){
            _sh(i.o);
          }

        }
      )
    }
  }
  this.stop = function(sh,eh){
    const _sh = typeof sh === 'function'?sh:_shnd;
    const _eh = typeof eh === 'function'?eh:_ehnd;
    db.collection(_col).remove({},function(err,res){
      if(err)_eh(err);
      _sh(res.result);
    })
  }
  this.request = function(num,func,eh){
    let col = db.collection(_col);
    const _eh = typeof eh === 'function'?eh:_ehnd;
    col.find({}).limit(num).toArray(function(err,items){
      if(err)_eh(err);
      let ids = [];
      for(let it of items)ids.push(it._id);
      if(ids.length){
        col.deleteMany({_id:{$in:ids}},function(err,res){
          if(err)_eh(err);
          for(let it of items){
            func(it.o);
          }
        })
      }else{

      }
    });
  }
}

function Queue(msr,bck,f,fact){
  EventEmitter.call(this);
  this.state = 'running'
  const _self = this;
  var _pending = 0;
  var _srs = msr;
  const _fact = typeof fact == 'undefined' || 0.1;
  this.stop = function stop_queue(){

    bck.stop(function(r){
      _self.state = 'stopped';
      _self.emit('stop',r);
      logger.trace(`stop state:${_self.state} srs:${_srs} pending:${ _pending}` );
    })

  }

  this.pause = function pause_queue(){
    _self.state = 'paused';
    _self.emit('pause')
    logger.trace(`pause state:${_self.state} srs:${_srs} pending:${ _pending}` );
  }

  this.resume = function resume_queue(){
    _self.state = 'running';
    logger.trace(`resume state:${_self.state} srs:${_srs} pending:${ _pending}` );
    _self.emit('resume');
    return run();
  }

  this.enqueue = function(it){
    bck.enqueue(it,function(ops){
      logger.trace(`enqueue state:${_self.state} srs:${_srs} pending:${ _pending}` );
      _self.emit('enqueue',ops);
    },function(err){
      throw err;
    })
  }

  function run(){
    logger.debug(`run state:${_self.state} srs:${_srs} pending:${ _pending}` );
    if(_self.state == 'running'){
      var req = _srs - _pending;
      bck.request(req,function(it){
        if(it === null){
          run();
          return;
        }
        if(_pending > _srs){
          _self.enqueue([it]);
        }else{
          _pending++;
          f(it,function(){
            _pending--;
            _srs = _srs + _fact;
            if(_srs > Number.MAX_VALUE)_srs = _srs = Number.MAX_VALUE;
            logger.trace(`success state:${_self.state} srs:${_srs} pending:${ _pending}`);
            run();
          },function(requeue){
            _pending--;
            _srs = _srs - _fact;
            if(_srs == 0)_srs = 1;
            logger.trace(`fail state:${_self.state} srs:${_srs} pending:${ _pending} requeue:${requeue}`);
            if(requeue)_self.enqueue([it]);
            run();
          });
        }
      });
    } else logger.info(`paused queue ${ _self.state} ${_pending}`);
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
        var rurl = urlp.resolve(purl.host,res.headers.location);
        if(acceptsurl(rurl)){
          logger.debug(`redirect ${url} to ${rurl} redir:${redir} maxRedir:${maxRedir} retry:${retry} maxRetry:${maxRetry}`);
          _r(rurl,acceptsurl);
        }
      } else if(maxRedir == redir){
        err('MAX_REDIR',redir,retry);
      } else if(maxRetry == retry){
        err('MAX_RETRY',redir,retry);
      } else err(res,redir,retry);
    }).on('error',function(e){
      if(e.code == 'ECONNRESET' && retry < maxRetry){
        retry++;
        logger.debug(`retry url:${url} redir: ${redir} maxRedir: ${maxRedir} retry:${retry} maxRetry:${maxRetry}`);
        _r(url,acceptsurl);
      }else err(e,redir,retry);
    });
  }
  _r(url,acceptsurl);

}

function scrape(url,db,q,abc,s,e){
  logger.info(`scrape ${url}`);
  var urls = []
  var stats = {};
  var nwords = 0;
  const _alphabet = abc.alphabet? new RegExp(`^[${abc.alphabet}]+$`,'g'): new RegExp('.+');
  const _lang = abc.lang?new RegExp(abc.lang): new RegExp(/.*/);
  const _accept = typeof abc.accepturl === 'function'? abc.accepturl:function(purl){return purl.indexOf(url) == 0;};
  request(url,_accept,function(res){
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

      logger.trace(`finished parsing ${url} ${pagehash}`);
      const col = db.collection('urls');
      col.updateMany(
        {hash:pagehash},
        {$addToSet: {same:url}},
        {multi:true},
        function(err,r){
          if(err) throw err;
          logger.trace(`found ${r.matchedCount} pages with hash ${pagehash}`)
          const hashash = r.matchedCount
          col.updateOne({_id:url},
            {
              $set:{hash:pagehash},
              $addToSet: {urls:{$each: urls}}
            },
            function(err,r){
              if(err)throw err;
              if(nwords && !hashash && r.modifiedCount){
                 // only process urls for a page which content was not processed before

                   logger.info(`process words for ${url} ${pagehash}`);
                   let blk = db.collection('words').initializeOrderedBulkOp();
                   for(let w in stats){
                     blk.find({_id:w}).upsert().updateOne({$inc:{count:stats[w]}})
                   }
                   blk.execute(function(err,res){
                     if(err) throw err;
                     logger.debug(`scraped and enqueue urls for ${url} ${pagehash}`);
                     if(urls.length)
                      q.enqueue(urls);
                     s();
                   })
              }else {
                if(!r.matchedCount)logger.warn(`scraped url ${url} but not found it`)
                s();
              }
            });
        }
      );

    })
  },function(err,redir,retry){
    logger.debug(err.statusCode?`${err.statusCode} ${url}`:err);
    e(false)
  });
}

function check(urls,db,q,abc,s,e){
  const col = db.collection('urls')
  var blk = col.initializeOrderedBulkOp();
  for(let url of urls){
    blk.find({_id:url}).upsert().updateOne({$set:{time:new Date()}});
  }
  blk.execute(function(err,r){
      if(err)throw err;
      col.find({_id:{$in:urls},status:{$ne:'check'}}).toArray(function(err,items){
        if(err)throw err;
        logger.trace(`check found ${items.length}`);
        var lnt = items.length
        var enqueue = false;
        if(lnt){
          for(let u of items){
            col.findOneAndUpdate({_id:u._id},{$set:{status:'check',time:new Date()}},function(err,r){
              if(err)throw err;
              logger.trace(r.value);
              r = r.value
              logger.info(`checking url:${r._id} hash:${r.hash}`)
              if(r.hash){//already parsed
                if(r.urls && r.urls.length){
                  logger.debug(`checked and enqueue urls for url:${r._id} hash:${r.hash}`)
                  q.enqueue(r.urls);
                  enqueue = true;
                }
              }else{
                scrape(r._id,db,q,abc,s,e);
              }
              if(enqueue){
                logger.trace(`check resume queue url:${r._d} hash:${r.hash} proc:${lnt}`);
                q.resume();
              }
            });
          }

        }else s()
      });
    }
  )
}


connect(function(db,close){
  const abc = {
      lang:'ht',
      alphabet:'abcdeèfgijklmnoòprstuvwyz',
      accepturl:function(url){
        return (url.indexOf('http://www.jw.org/ht') == 0
          || url.indexOf('http://wol.jw.org/ht') == 0)
          && url.indexOf('.gif') == -1
          && url.indexOf('.jpeg') == -1
          && url.indexOf('.jpg') == -1
          && url.indexOf('choose-language') == -1;
          ;
      }
    }
  db.collection('urls').updateMany({},{$set:{status:null,time:new Date()}},function(err,r){
    logger.info('cleaned urls');
    if(err)throw err;
    var q = new Queue(100,new MongoBackend(db)
    ,function(it,s,e){
      check([it],db,q,abc,s,e)
    },0);
    q.on('start',function(q){
      logger.info('starting');
      q.enqueue(['http://www.jw.org/ht','http://wol.jw.org/ht']);
      q.resume();
    });
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
