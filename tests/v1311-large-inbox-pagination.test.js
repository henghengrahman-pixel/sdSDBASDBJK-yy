import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { LiveChatClient } from '../src/livechat.js';

function listen(server){ return new Promise(resolve=>server.listen(0,'127.0.0.1',()=>resolve(server.address().port))); }
function close(server){ return new Promise(resolve=>server.close(resolve)); }

test('large inbox deep sync follows next_page_id and sends continuation as page_id only', async()=>{
  const bodies=[];
  const server=http.createServer((req,res)=>{
    let raw='';
    req.on('data',c=>raw+=c);
    req.on('end',()=>{
      const body=raw?JSON.parse(raw):{};
      bodies.push(body);
      res.setHeader('content-type','application/json');
      if(!body.page_id) return res.end(JSON.stringify({
        chats_summary:[{id:'c1',is_followed:true,last_thread_summary:{active:true}}],
        next_page_id:'p2'
      }));
      if(body.page_id==='p2') return res.end(JSON.stringify({
        chats_summary:[{id:'c2',is_followed:true,last_thread_summary:{active:true}}],
        next_page_id:'p3'
      }));
      if(body.page_id==='p3') return res.end(JSON.stringify({
        chats_summary:[{id:'c3',is_followed:true,last_thread_summary:{active:true}}]
      }));
      res.statusCode=400; res.end(JSON.stringify({error:{message:'bad cursor'}}));
    });
  });
  const port=await listen(server);
  try{
    const lc=new LiveChatClient({base:`http://127.0.0.1:${port}/v3.5/agent/action`,accountId:'a',pat:'p'});
    const list=await lc.listChats({forceDeep:true});
    assert.deepEqual(list._normalizedChats.map(x=>x.id),['c1','c2','c3']);
    assert.equal(list._inventoryComplete,true);
    assert.equal(list._pageCount,3);
    assert.deepEqual(bodies[1],{page_id:'p2'});
    assert.deepEqual(bodies[2],{page_id:'p3'});
  }finally{ await close(server); }
});

test('large inbox config keeps page size separate from total inventory cap', async()=>{
  const fs=await import('node:fs');
  const config=fs.readFileSync(new URL('../src/config.js',import.meta.url),'utf8');
  assert.match(config,/lcListLimit: Math\.min\(100/);
  assert.match(config,/lcInventoryLimit: Math\.max\(0/);
  assert.match(config,/LIVECHAT_INVENTORY_LIMIT/);
  assert.match(config,/LIVECHAT_POLL_CONCURRENCY/);
});
