"use strict"
var htmlparser = require('htmlparser2');
var fs = require('fs');
var mongo = require('mongodb').MongoClient;
var m = require('mustache');
var urlp = require('url');
var http = require('http');
var https = require('https');

var mongourl = 'mongodb://localhost:27017/800w';

function queue(msr){
  var _q = [];
  this.state = 'paused'
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
  }

  function run(){
    if(_self.state == 'running' && _q.length && _count == 0){
      console.log('running with srs:'+_srs +' and length:'+_q.length);
      var err = 0;
      for(_count = 0 ; _count < _srs && _q.length;_count++){
        (function call(f){
          f(function _succ(){
            _count--;
            if(_count == 0){
              _srs = _srs - err;
              if(_srs < 1){
                _srs = 1;
              }else if(err == 0){
                _srs++;
                if(_srs > msr)_srs = msr;
              }
              run();
            }
          },function _err(){
            _count--;
            err++;
            _self.enqueue(f);
            if(_count == 0){
              _srs = _srs - err;
              if(_srs < 1){
                _srs = 1;
              }else if(err == 0){
                _srs++;
                if(_srs > msr)_srs = msr;
              }
              run();
            }
          })
        })(_q.pop());
      }
    }else if(_q.length == 0){
      _self.pause();
    }
  }

}

function connect(cb){
  mongo.connect(mongourl,function(err,db){
    if(err != null) throw err;
    if(db != null){
      try{
        cb(db,function(err,r){
          db.close();
        });
      }catch(error){
        try{
          console.log('Closing connection!');
          db.close();
        }catch(connerr){

        }
      }
    }
  })
}

function parseText(text,db){
    var words = text.split(" ");
    for(var i = 0;i< words.length;i++){
      var w = words[i].toLowerCase();
      if(w.length)
        db.collection('words').findOneAndUpdate(
          {'w':w},
          {$inc :{ 'count': 1}},
          {upsert:true},
          function(err,r){

          }
        );
    }
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
      else if(res.statusCode === 302 && res.headers && res.headers.location && redir < maxRedir){
        if(res.headers.location.indexOf('/') == 0){
          var parsed = urlp.parse(base);
          request(base,parsed.protocol + '//' + parsed.host + res.headers.location,succ,redir + 1,maxRedir);
        }else if(res.headers.location.indexOf(base) == 0)
          request(base,res.headers.location,succ,redir + 1,maxRedir);
      }
    }).on('error',function(e){
      console.log(e);
    });
  }catch(error){
    console.log(error);
  }
}

function scrape(base,db,q,cb,url){
  console.log('scrape ' + url);
  var urls = []
  request(base,url,function(res){
    var parser = new htmlparser.Parser({
      ontext: function(text){parseText(text,db)},
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
      console.log(urls);
      db.collection('urls').updateOne({'url':url},{$set: {'urls':urls}},{upsert:true});
      for(var i = 0; i < urls.length; i++){
        check(base,db,q,cb,urls[i]);
      }
    })
  });
}

function check(base,db,q,cb,url){
  if(typeof url == 'undefined'){
    url = base
  }
  console.log('check ' + url + ' with base ' + base);
  db.collection('urls').find({'url':url}).limit(1).each(function(err,o){
    if(err) throw err;
    if(o != null && o.urls){
      db.collection('urls').find({'url':{$in : o.urls}},function(err,urls){
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
                scrape(base,db,q,cb,url)
              }
            );
          }
        }
      });
    }else {
      q.enqueue(
        function(succ,err){
          scrape(base,db,q,cb,url)
        }
      );
    }
  });
}

/*
connect(function(db,close){
  check('http://wol.jw.org/ht',db,new queue(100),function(db){
      db.collection('words').find().sort([['count', -1]]).limit(800)
        .toArray(function(err,docs){
          console.log(docs);
          close();
        });
  });
});
*/
var q = new queue(5);
for(let j = 0;j< 20;j++){
  (function a(j){
    q.enqueue(function(s,e){
      console.log('get '+j)
      http.get('http://www.jw.org',function(res){
        console.log(j + ' '+res.statusCode);
        s();
      }).on('error',function(err){
        console.log(j + ' err');
        e();
      });
    })
  })(j);
}
q.resume();
