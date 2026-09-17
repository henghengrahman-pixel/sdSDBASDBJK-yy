import http from 'node:http';
import {performance} from 'node:perf_hooks';
import {LiveChatClient} from '../src/livechat.js';
import {AsyncLimiter} from '../src/async-limiter.js';

const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function listen(s){return new Promise(r=>s.listen(0,'127.0.0.1',()=>r(s.address().port)))}
function close(s){return new Promise(r=>s.close(r))}
async function paginationCase(total){
  const pageSize=100,pages=Math.ceil(total/pageSize);let calls=0;
  const server=http.createServer((req,res)=>{let raw='';req.on('data',c=>raw+=c);req.on('end',()=>{calls++;const b=raw?JSON.parse(raw):{};const page=b.page_id?Number(String(b.page_id).slice(1)):1;const start=(page-1)*pageSize;const items=Array.from({length:Math.max(0,Math.min(pageSize,total-start))},(_,i)=>({id:`c${start+i+1}`,is_followed:true,last_thread_summary:{active:true}}));res.setHeader('content-type','application/json');res.end(JSON.stringify({chats_summary:items,...(page<pages?{next_page_id:`p${page+1}`}:{})}))})});
  const port=await listen(server);const lc=new LiveChatClient({base:`http://127.0.0.1:${port}`,accountId:'a',pat:'b',timeoutMs:5000});const mem0=process.memoryUsage().heapUsed,t0=performance.now();const out=await lc.listChats({forceDeep:true});const ms=performance.now()-t0,mem1=process.memoryUsage().heapUsed;await close(server);return{total,fetched:out._normalizedChats.length,pages:out._pageCount,calls,durationMs:+ms.toFixed(2),heapDeltaMB:+((mem1-mem0)/1048576).toFixed(2),complete:out._inventoryComplete};
}
async function burstCase(n,concurrency=6){let active=0,maxActive=0,done=0;const lim=new AsyncLimiter({name:'burst',concurrency});const t0=performance.now();await Promise.all(Array.from({length:n},(_,i)=>lim.run(async()=>{active++;maxActive=Math.max(maxActive,active);await sleep(i%7===0?2:1);done++;active--;})));return{jobs:n,done,maxActive,durationMs:+(performance.now()-t0).toFixed(2),metrics:lim.metrics()};}
const before=process.memoryUsage().heapUsed;const pagination=[];for(const n of [100,500,1000,5000])pagination.push(await paginationCase(n));const bursts=[await burstCase(500),await burstCase(1000)];const after=process.memoryUsage().heapUsed;console.log(JSON.stringify({pagination,bursts,peakApproxHeapMB:+(Math.max(before,after)/1048576).toFixed(2),finalHeapMB:+(after/1048576).toFixed(2)},null,2));
