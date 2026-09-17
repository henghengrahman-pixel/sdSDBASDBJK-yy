import { performance } from 'node:perf_hooks';
import { normalizeChatLifecycle } from '../src/chat-lifecycle.js';

const sizes=[100,1000,10000,35000];
const results=[];
for(const n of sizes){
  const rows=Array.from({length:n},(_,i)=>({id:`c${i}`,is_followed:true,last_thread_summary:{active:i%97!==0},routing_status:i%389===0?'archived':''}));
  const t0=performance.now();
  let active=0,closed=0,archived=0;
  for(const row of rows){const s=normalizeChatLifecycle(row);if(s.isArchived)archived++;else if(s.isClosed)closed++;else if(s.shouldBeVisibleInInbox)active++;}
  const ms=performance.now()-t0;
  results.push({summaries:n,active,closed,archived,durationMs:Number(ms.toFixed(3)),summariesPerSecond:Math.round(n/(ms/1000))});
}
console.log(JSON.stringify({ok:true,results},null,2));
