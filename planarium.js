const bcode = require('bcode')
const mkdir = require('make-dir');
const fs = require('fs');
const lmdb = require('node-lmdb');
var en = new lmdb.Env();
var db_mediatype
var db_b
var dbpath
var filepath
module.exports = {
  planarium: '0.0.1',
  query: {
    web: {
      "v": 3,
      "q": {
        "find": {},
        "project": { "c": 1, "tx": 1, "in.e": 1, "out.s3": 1, "out.s4": 1, "out.s5": 1 },
        "limit": 30
      },
      "r": {
        "f": "[ .[] | { \"C:// address\": \"c://\\(.c)\", \"B:// address\": \"b://\\(.tx.h)\", \"C:// URI over http\": \"https://data.bitdb.network/1KuUr2pSJDao97XM8Jsq8zwLS6W1WtFfLg/c/\\(.c)\", \"B:// URI over http\": \"https://b.bitdb.network#\\(.tx.h)\", out: .out }]"
      }
    },
    api: {
      timeout: 50000,
      sort: { "blk.i": -1 },
      concurrency: { aggregate: 7 },
      oncreate: async function(m) {
        // [1] LMDB Folder
        //  a. set global dbpath and filepath
        dbpath = m.fs.path + "/lmdb"
        //  b. create a directory at dbpath
        await mkdir(dbpath)
        //  c. Connect to DB
        en.open({ path: dbpath, mapSize: 2*1024*1024*1024, maxDbs: 3 });
        db_mediatype = en.openDbi({ name: "mediatype", create: true })
        db_b = en.openDbi({ name: "b", create: true })

        // [2] C:// Folder
        //  a. m.fs.path contains the path to the current state machine's root filesystem
        filepath = m.fs.path + "/c/"
      },
      routes: {
        "/c/:id": function(req, res) {
          // 1. Generate filename
          let filename = filepath + req.params.id;

          // 2. Get the content-type info for the hash from LMDB
          let txn = en.beginTxn()
          let value = txn.getString(db_mediatype, req.params.id)
          txn.commit()
          if (value) { res.setHeader('Content-type', value) }
          res.setHeader('bitcoin-address', process.env.ADDRESS)
          console.log("[serve]", req.params.id, value)

          // 3. set content length header
          fs.stat(filename, function(err, stat) {
            if (stat && stat.size) {
              res.setHeader('Content-Length', stat.size)
            }
            // 4. Send file
            let filestream = fs.createReadStream(filename)
            filestream.on("error", function(e) {
              res.send("")
            });
            filestream.pipe(res)
          })
        },
        "/b/:id": function(req, res) {
          const txn = en.beginTxn()
          const c_hash = txn.getString(db_b, req.params.id)
          if (!c_hash) {
            txn.commit()
            return res.status(404).send("")
          }
          const value = txn.getString(db_mediatype, c_hash)
          txn.commit()
          if (value) { res.setHeader('Content-type', value) }
          res.setHeader('bitcoin-address', process.env.ADDRESS)
          console.log("[serve]", req.params.id, value)

          let filename = filepath + c_hash;
          fs.stat(filename, function(err, stat) {
            if (stat && stat.size) {
              res.setHeader('Content-Length', stat.size)
            }
            // 4. Send file
            let filestream = fs.createReadStream(filename)
            filestream.on("error", function(e) {
              res.send("")
            });
            filestream.pipe(res)
          })
        }
      }
    },
    log: true
  },
  socket: {
    web: { v: 3, q: { find: {} } },
    api: {},
    topics: ["c", "u"]
  },
  transform: {
    request: bcode.encode,
    response: bcode.decode
  },
  url: "mongodb://localhost:27020",
  port: 3000,
}
