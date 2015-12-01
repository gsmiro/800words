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

function MongoBackend(db,opts){
  var _col = opts && opts.collection || 'queue'

  function _q(f,v){
    let _q = {};
    _q[f] = v;
    return _q;
  }

  function _shnd(it){};
  function _ehnd(err){ throw err; };

  this.enqueue = function(item,sh,eh){
    var _sh = typeof sh === 'function'?sh:_shnd;
    var _eh = typeof eh === 'function'?eh:_ehnd;
    item = typeof item === 'object' && typeof item.length === 'number'?item:[item];
    var _item = [];
    for(let i of item){
      logger.trace(i)
      if(typeof i == 'string' || typeof i == 'number')
        _item.push({_id:i});
      else
        _item.push(i);
    }
    db.collection(_col).insertMany(
      _item,
      function(err,res){
        if(err)_eh(err)
        _sh(res.ops);
      }
    )
  }
  this.stop = function(sh,eh){
    var _sh = typeof sh === 'function'?sh:_shnd;
    var _eh = typeof eh === 'function'?eh:_ehnd;
    db.collection(_col).remove({},function(err,res){
      if(err)_eh(err);
      _sh(res.result);
    })
  }
  this.request = function(num,func,eh){
    let col = db.collection(_col);
    var _eh = typeof eh === 'function'?eh:_ehnd;
    col.find({}).limit(num).toArray(function(err,items){
      if(err)_eh(err);
      let ids = [];
      for(let it of items)ids.push(it._id);
      col.remove({_id:{$in:ids}},function(err,res){
        if(err)_eh(err);
        for(let it of items){
          func(it._id);
        }
      })
    });
  }
}

function Queue(msr,bck,f,end){
  this.state = 'running'
  var _self = this;
  var _pending = 0;
  var _srs = msr;
  this.stop = function stop_queue(){
    bck.stop(function(r){
      _self.state = 'stopped';
      logger.trace(`stop state:${_self.state} srs:${_srs} pending:${ _pending}` );
    })
  }

  this.pause = function pause_queue(){
    _self.state = 'paused';
    logger.trace(`pause state:${_self.state} srs:${_srs} pending:${ _pending}` );
  }

  this.resume = function resume_queue(){
    _self.state = 'running';
    logger.trace(`resume state:${_self.state} srs:${_srs} pending:${ _pending}` );
    return run();
  }

  this.enqueue = function(it){
    bck.enqueue(it,function(ops){
      logger.trace(`enqueued state:${_self.state} srs:${_srs} pending:${ _pending}` );
    },function(err){
      throw err;
    })
  }

  function _suc(){
    _pending--;
    logger.trace(`success state:${_self.state} srs:${_srs} pending:${ _pending}`);
  }

  function run(){
    logger.debug(`state:${_self.state} srs:${_srs} pending:${ _pending}` );
    if(_self.state == 'running' && _pending <= _srs){
      bck.request(_srs - _pending,function(it){
        f(it,_suc,function(requeue){
          _pending--;
          logger.trace(`fail state:${_self.state} srs:${_srs} pending:${ _pending} requeue:${requeue}`);
          if(requeue)bck.enqueue(it);
        });
      });
    } else logger.warn(`still running ${ _self.state} ${_pending}`);
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

function scrape(url,db,q,abc,s,e){
  logger.info(`scrape ${url}`);
  var urls = []
  var stats = {};
  var _alphabet = abc.alphabet? new RegExp(`^[${abc.alphabet}]+$`,'g'): new RegExp('.+');
  var _lang = abc.lang?new RegExp(abc.lang): new RegExp(/.*/);
  var _accept = typeof abc.accepturl === 'function'? abc.accepturl:function(purl){return purl.indexOf(url) == 0;};
  request(url,function(res){
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
              var purl = urlp.resolve(url,attr.href);
              var htag = purl.indexOf('#')
              if(htag > 0)purl = purl.substring(0,htag);
              if(purl != url && _accept(purl))
                urls = urls.concat(purl);
              else logger.trace(`Ignoring ${purl}`)
          }else this.parseText = true
        }
      }
    }, {decodeEntities: true});

    res.on('data',function(data){parser.write(data); }).on('end',function(){
      parser.end();
      var pagehash = hash.digest('hex');

      logger.trace(`finished parsing ${url} ${pagehash}`);

      db.collection('urls').findOneAndUpdate(
        {hash:pagehash},
        {
          $addToSet: {urls:{$each: urls}}
        },
        {upsert:true},
        function(err,r){
          if(err) throw err;
          if(r.value == null){
            // only process urls for a page which content was not processed before
            logger.info(`update stats ${url} ${pagehash}`);
            let blk = db.collection('words').initializeOrderedBulkOp();
            var exec = false;
            for(let w in stats){
              blk.find({w:w}).upsert().updateOne({$inc:{count:stats[w]}})
              exec = true;
            }
            if(exec)
              blk.execute(function(err,res){
                if(err) throw err;
                  check(urls,db,q,abc,s);
              })
            else s();
          }else{
             logger.trace(`found hash for ${url} ${pagehash}`);
             s();
          }

        }
      );

    })
  },function(err){
    e(false)
  });
}

function check(urls,db,q,abc,after){
  logger.trace(`check urls:${urls.length}`);
  var col = db.collection('urls')
  var blk = col.initializeOrderedBulkOp();
  for(let url of urls){
    blk.find({_id:url}).upsert().updateOne({$set:{time:new Date()}});
  }
  blk.execute(function(err,r){
      if(err)throw err;
      logger.trace(`check updated to pending ${r.getUpsertedIds()}`);
      col.find({_id:{$in:urls},status:{$ne:'checking'}}).toArray(function(err,items){
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
              logger.trace(`check url:${r.url} hash:${r.hash} proc:${lnt}`);
              if(r.hash){//already parsed
                if(r.urls && r.urls.length){
                  check(r.urls,db,q,abc,after);
                  checking = true;
                }
              }else{
                q.enqueue(r);
                enqueue = true;
              }
              lnt--;
              if(lnt == 0){
                if(!checking && typeof after === 'function'){
                  logger.trace(`check call after  url:${r.url} hash:${r.hash} proc:${lnt}`);
                  after();
                }
                if(enqueue){
                  logger.trace(`check resume queue url:${r.url} hash:${r.hash} proc:${lnt}`);
                  q.resume();
                }
              }
            });
          }
        }else if(typeof after === 'function'){
          logger.trace(`check call after url:${r.url} hash:${r.hash} proc:${lnt}`);
          after();
        }
      });
    }
  )
}


connect(function(db,close){
  db.collection('urls').updateMany({},{$set:{status:null,time:new Date()}},function(err,r){
    if(err)throw err;
    var q = new Queue(100,new MongoBackend(db)
    ,function(it,s,e){
      check(url,db,q,{
          lang:'ht',
          alphabet:'abcdeèfgijklmnoòprstuvwyz'
        },s,e)
    }
    ,function(){
      db.collection('words').find({stop:{$eq:false}}).sort({'count': -1}).limit(800)
      .toArray(function(err,docs){
        if(err) throw err;
        logger.info(docs);
        close();
      });
    });
    q.enqueue(['http://www.jw.org/ht','http://wol.jw.org/ht']);
    q.resume();
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
