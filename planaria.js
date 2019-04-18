const mkdir = require('make-dir');
const fs = require('fs');
const crypto = require('crypto');
const lmdb = require('node-lmdb');
const fileType = require('file-type');
const { ungzip } = require('node-gzip');
var en = new lmdb.Env();
var db_mediatype
var db_b
var fspath
var B = function(o) {
  let outputs = []
  let isdata = false
  for(let i=0; i<o.out.length; i++) {
    let out = o.out[i]
    if (out.b0 && out.b0.op && out.b0.op === 106 && out.s1 === '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut') {
      isdata = true
      delete out.str
      delete out.e
      outputs.push(out)
    }
  }
  if (isdata) {
    return {
      in: o.in.map(function(i) { return { e: { a: i.e.a } } }),
      out: outputs
    }
  } else {
    return null
  }
}
var save = function(h, outs, env) {
  let ps = outs.map(function(out) {
    return new Promise(async function(resolve, reject) {
      let buf = null
      let hash
      if (out.lb2 && typeof out.lb2 === 'string') {
        buf = Buffer.from(out.lb2, 'base64');
        if (out.s4 === 'gzip') {
          buf = await ungzip(buf)
        }
        hash = crypto.createHash('sha256').update(buf).digest('hex');
      } else if (out.b2 && typeof out.b2 === 'string') {
        buf = Buffer.from(out.b2, 'base64');
        if (out.s4 === 'gzip') {
          buf = await ungzip(buf)
        }
        hash = crypto.createHash('sha256').update(buf).digest('hex');
      }
      console.log("hash = ", hash)
      if (buf) {
        let type;
        try {
          let detection = fileType(buf)
          if (detection) {
            type = detection.mime
            console.log("type detected from header = ", type)
          } else if (out.s3 && out.s3.length > 0) {
            type = out.s3
            console.log("type not detected but set from out.s3 = ", type)
          }
        } catch (e) {
          if (out.s3 && out.s3.length > 0) {
            type = out.s3
            console.log("type detected from out.s3 = ", type)
          }
        }
        if (type) {
          fs.writeFile(fspath + '/c/' + hash, buf, function(er) {
            if (er) {
              console.log("Error = ", er)
              reject()
            } else {
              console.log("[put]", hash, type)
              let txn = en.beginTxn();
              txn.putString(db_mediatype, hash, type)
              txn.putString(db_b, h, hash)
              txn.commit();
              resolve(hash)
            }
          })
        } else {
          resolve(hash)
        }
      } else {
        resolve(hash)
      }
    })
  })
  return Promise.all(ps)
}
// initialize LMDB
var initLMDB = function(m) {
  en.open({
    path: fspath + "/lmdb",
    mapSize: 2*1024*1024*1024,
    maxDbs: 3
  });
  db_mediatype = en.openDbi({ name: "mediatype", create: true })
  db_b = en.openDbi({ name: "b", create: true })
}
module.exports = {
  planaria: '0.0.1',
  from: 566470,
  name: 'C://',
  version: '0.0.5',
  description: 'Content Addressable Storage over Bitcoin',
  address: '1KuUr2pSJDao97XM8Jsq8zwLS6W1WtFfLg',
  index: {
    c: {
      keys: [
        'tx.h',
        'c',
        'in.e.a', 'in.e.h', 'in.e.i',
        'blk.i', 'blk.t', 'blk.h',
        "out.s3", "out.s4", "out.s5", "out.s6", "out.s7", "out.s8", "out.s9"
      ],
      unique: ['tx.h'],
      fulltext: ['out.s2', 'out.ls2']
    },
    u: {
      keys: [
        'tx.h',
        'c',
        'in.e.a', 'in.e.h', 'in.e.i',
        "out.s3", "out.s4", "out.s5", "out.s6", "out.s7", "out.s8", "out.s9"
      ],
      unique: ['tx.h'],
      fulltext: ['out.s2', 'out.ls2']
    }
  },
  oncreate: async function(m) {
    fspath = m.fs.path
    await mkdir(m.fs.path + "/c")
    await mkdir(m.fs.path + "/lmdb")
    initLMDB(m)
  },
  onmempool: async function(m) {
    let op_return = B(m.input)
    if (op_return) {
      let c = await save(m.input.tx.h, op_return.out, m.env)
      let tx = {
        tx: { h: m.input.tx.h },
        c: c[0],
        in: m.input.in,
        out: op_return.out.map(function(o) {
          let oo = {}
          if (o.s2) oo.s2 = o.s2
          if (o.ls2) oo.ls2 = o.ls2
          if (o.s3) oo.s3 = o.s3
          if (o.s4) oo.s4 = o.s4
          if (o.s5) oo.s5 = o.s5
          if (o.s6) oo.s6 = o.s6
          if (o.s7) oo.s7 = o.s7
          if (o.s8) oo.s8 = o.s8
          if (o.s9) oo.s9 = o.s9
          return oo
        }),
      }
      console.log("[mempool] inserting", tx.h)
      await m.state.create({ name: "u", data: tx }).catch(function(e) {
        if (e.code !== 11000) {
          console.log('## ERR ', e)
        } else {
          console.log("$ Error", e)
        }
      })
      console.log("Insert success")
      m.output.publish({name: "u", data: tx})
    }
  },
  onblock: async function(m) {
    console.log("## onblock", "block height = ", m.input.block.info.height, "block hash =", m.input.block.info.hash, "txs =", m.input.block.info.tx.length)
    let items = m.input.block.items
    let blktxs = []
    for(let i=0; i<items.length; i++) {
      let o = items[i]
      let op_return = B(o)
      if (op_return) {
        let c = await save(o.tx.h, op_return.out, m.env)
        blktxs.push({
          tx: { h: items[i].tx.h },
          in: o.in,
          c: c[0],
          out: op_return.out.map(function(o) {
            let oo = {}
            if (o.s2) oo.s2 = o.s2
            if (o.ls2) oo.ls2 = o.ls2
            if (o.s3) oo.s3 = o.s3
            if (o.s4) oo.s4 = o.s4
            if (o.s5) oo.s5 = o.s5
            if (o.s6) oo.s6 = o.s6
            if (o.s7) oo.s7 = o.s7
            if (o.s8) oo.s8 = o.s8
            if (o.s9) oo.s9 = o.s9
            return oo
          }),
          blk: items[i].blk,
        })
      }
    }
    console.log("[block] inserting", blktxs.length)
    await m.state.create({
      name: "c", data: blktxs
    }).catch(function(e) {
      if (e.code !== 11000) {
        console.log('## ERR ', e)
      } else {
        console.log("$ Error", e)
      }
    })
    console.log("Insert success")
    console.log("[block] resetting mempool")
    let memtxs = []
    for (let i=0; i<m.input.mempool.items.length; i++) {
      let o = m.input.mempool.items[i]
      let op_return = B(o)
      if (op_return) {
        let c = await save(o.tx.h, op_return.out, m.env)
        memtxs.push({
          tx: { h: o.tx.h },
          c: c[0],
          in: o.in,
          out: op_return.out.map(function(_o) {
            let oo = {}
            if (_o.s2) oo.s2 = _o.s2
            if (_o.ls2) oo.ls2 = _o.ls2
            if (_o.s3) oo.s3 = _o.s3
            if (_o.s4) oo.s4 = _o.s4
            if (_o.s5) oo.s5 = _o.s5
            if (_o.s6) oo.s6 = _o.s6
            if (_o.s7) oo.s7 = _o.s7
            if (_o.s8) oo.s8 = _o.s8
            if (_o.s9) oo.s9 = _o.s9
            return oo
          }),
        })
      }
    }
    console.log("Inserting", memtxs.length, "of", m.input.mempool.items.length)
    await m.state.delete({ name: "u", filter: { find: {} } })
    await m.state.create({ name: "u", data: memtxs, }).catch(function(e) {
      if (e.code != 11000) {
        console.log("# Error", e, m.input.block)
        process.exit()
      }
    })
    for(let i=0; i<blktxs.length; i++) {
      m.output.publish({name: "c", data: blktxs[i]})
    }
  },
  onrestart: async function(m) {
    fspath = m.fs.path
    await m.state.delete({
      name: 'c',
      filter: { find: { "blk.i": { $gt: m.clock.self.now } } }
    }).catch(function(e) {
      if (e.code !== 11000) {
        console.log('## ERR ', e, m.clock.self.now, m.clock.bitcoin.now)
        process.exit()
      }
    })
    await m.state.delete({ name: "u", filter: { find: {} } })
    initLMDB(m)
  }
}

