"use strict"
var htmlparser = require('htmlparser2');
var fs = require('fs');
var mongo = require('mongodb').MongoClient;
var m = require('mustache');
var urlp = require('url');
var http = require('http');
var https = require('https');

var mongourl = 'mongodb://localhost:27017/800w';
var dict = 'abcdeèfgijklmnoòprstuvwyz';

function queue(msr,end){
  var _q = [];
  this.state = 'running'
  var _self = this;
  var _count = 0;
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
    run();
  }

  this.enqueue = function(f){
    _q.push(f);
    console.log('enqueue '+_q.length + ' state:' + _self.state)
    if(_self.state == 'running')run();
  }

  function run(){
    console.log('state:'+_self.state+' srs:'+_srs +' length:'+_q.length + ' count:' + _count );
    if(_self.state == 'running' && _q.length && _count < _srs){
      console.log('Adding to queue '+ (_srs - _count));
      var _fail = 0;
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
              run();
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
              run();
            }
          })
        })(_q.pop());
      }
    }else if(_q.length == 0){
      end();
    }
  }

}

function connect(cb){
  mongo.connect(mongourl,function(err,db){
    if(err != null) throw err;
    if(db != null){
        db.on('error',function(e){
          console.log('Erro de Banco')
        })
        cb(db,function(){
          db.close();
        });
    }
  })
}

function request(base,url,succ,err,redir,maxRedir){
  err = typeof err == 'function' && err || function(res){
    throw res.statusCode + ' ' + res.statusMessage ;
  }
  console.log('request '+ url);
  maxRedir = Number(maxRedir) || 10;
  redir = Number(redir) || 0;
  var proto = url.match(/https:.*/)?https:http;
  try{
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
      console.log(e);
      succ();
    });
  }catch(exc){
    console.log(exc);
    succ();
  }

}

function parseText(text,stats,dict){
    stats = stats || {}
    var words = text.split(" ");
    var rg = new RegExp('['+dict+']+','g');
    for(let w of words){
      var matches =  w.toLowerCase().match(rg);
      if(matches)
        for(let m of matches)
          stats[m] = stats.hasOwnProperty(m)?stats[m] + 1:1;
    }
    return stats;
}

function scrape(base,db,q,url,dict,s,e){
  console.log('scrape ' + url);
  var urls = []
  var stats = {};
  request(base,url,function(res){
    if(res == null){
      s();
      return;
    }
    var parser = new htmlparser.Parser({
      ontext: function(text){stats = parseText(text,stats,dict)},
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
      console.log('parsed ' + url);
      db.collection('urls').updateOne({'url':url},{$set: {'urls':urls}},{upsert:true},function(err,r){
        if(err)throw err;
        let blk = db.collection('words').initializeOrderedBulkOp();
        for(let w in stats){
          blk.find({'w':w}).upsert().updateOne({$inc:{'count':stats[w]}})
        }
        blk.execute(function(err,r){
          if(err) throw err;
          s();
          for(let url of urls){
            check(base,db,q,url);
          }
        })

      });

    })
  },e);
}

function check(base,db,q,url,dict){
  url = url || base;
  console.log('check ' + url + ' with base ' + base);
  db.collection('urls').find({'url':url}).limit(1).each(function(err,o){
    if(err) throw err;
    if(o != null && o.urls){
      db.collection('urls').find({'url':{$in : o.urls}},function(err,urls){
        if(err) throw err;
        if(urls.length == o.urls.length)cb(db);
        else {
          for(let url of o.urls){
            var found = false;
            for(let ourl in urls){
              if(ourl == url){
                found = true;
                break;
              }
            }
            if(!found) q.enqueue(
              function(succ,err){
                scrape(base,db,q,url,dict,succ,err)
              }
            );
          }
        }
      });
    }else {
      q.enqueue(
        function(succ,err){
          scrape(base,db,q,url,dict,succ,err)
        }
      );
    }
  });
}


connect(function(db,close){
  check('http://wol.jw.org/ht',db,new queue(100,function(){
      db.collection('words').find().sort([['count', -1]]).limit(800)
        .toArray(function(err,docs){
          console.log(docs);
          close();
        });
      }),null,dict);

});
/*
var q = new queue(5,function(){
  console.log('Finish');
});
q.pause();
for(let j = 0;j< 10;j++){
  (function a(j){
    q.enqueue(function(s,e){
      console.log('get '+j)
      request('http://www.jw.org','http://www.jw.org'+(j % 3 == 0?j:'') ,s,e);
    })
  })(j);
}
q.resume();
*/
