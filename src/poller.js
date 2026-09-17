import crypto from 'node:crypto';
import os from 'node:os';
import { config } from './config.js';
import { extractChatEvents } from './livechat.js';
import { normalizeText, detectIntent } from './normalizer.js';
import { processCustomerMessage, processGreetingTrigger } from './engine.js';
import * as db from './db.js';
import { isGreetingTriggerMessage } from './greeting.js';
import { normalizeChatLifecycle } from './chat-lifecycle.js';
import { incLiveChatMetric, liveChatRuntimeMetrics } from './livechat-metrics.js';

let running=false, timer=null, lastTick=null, lastSuccess=null, lastError=null, lastResult=null, paused=false;
let workerTimer=null, workerRunning=false, workerStopping=false, lastDeepGeneration=-1;
const workerId=`${os.hostname()}:${process.pid}`;
const summaryFingerprints = new Map();
const workerMetrics={activeWorkers:0,concurrency:config.lcPollConcurrency,processed:0,failed:0,retried:0,lastDurationMs:0,lastRunAt:null};
const pollMetrics={overlapPrevented:0,requestCount:0,errors:0,fetchTimeouts:0};

export function pollerStatus(){ return {running,paused,inFlight:running,lastTick,lastSuccess,lastError,lastResult,mode:config.lcSyncMode,pollMs:config.lcPollMs,trackedChats:summaryFingerprints.size,overlapPrevented:pollMetrics.overlapPrevented,workers:{...workerMetrics}}; }
function senderType(ev, chat){const t=String(ev.authorType||'').toLowerCase();if(t.includes('customer'))return 'customer';if(t.includes('agent'))return 'agent';const u=(chat?.users||[]).find(x=>String(x.id||'')===String(ev.authorId||''));const ut=String(u?.type||'').toLowerCase();if(ut.includes('customer'))return 'customer';if(ut.includes('agent'))return 'agent';return 'unknown';}
function summaryFingerprint(summary){return crypto.createHash('sha1').update(JSON.stringify(summary||{})).digest('hex');}
function ageSeconds(iso){const t=Date.parse(iso||'');return Number.isFinite(t)?Math.max(0,(Date.now()-t)/1000):Infinity;}
function isWelcomeTriggerEvent(ev){return Boolean(ev&&isGreetingTriggerMessage(ev.text));}
function greetingTriggerAgeLimit(){return Math.max(Number(config.greetingTriggerMaxAgeSeconds||0),600);}
function rememberFingerprint(chatId,fp){summaryFingerprints.delete(chatId);summaryFingerprints.set(chatId,{fp,lastSeen:Date.now()});if(summaryFingerprints.size>20000){const n=summaryFingerprints.size-18000;let i=0;for(const k of summaryFingerprints.keys()){summaryFingerprints.delete(k);if(++i>=n)break;}}}
function knownFingerprint(chatId){return summaryFingerprints.get(chatId)?.fp||null;}
function cleanupFingerprintCache(){const cutoff=Date.now()-2*60*60_000;for(const [k,v] of summaryFingerprints)if(Number(v?.lastSeen||0)<cutoff)summaryFingerprints.delete(k);}

async function ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover=true,allowGreetingTrigger=true}={}){const ours=await db.outboundLooksLikeOurs(chatId,ev.eventId,ev.text);const autoGreetingTrigger=!ours&&isGreetingTriggerMessage(ev.text);const type=ours?'ai':autoGreetingTrigger?'system':'agent';const inserted=await db.insertMessage({chatId,eventId:ev.eventId,senderType:type,authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:autoGreetingTrigger?'GREETING_TRIGGER':detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]});if(inserted&&autoGreetingTrigger&&allowGreetingTrigger&&ageSeconds(ev.createdAt)<=greetingTriggerAgeLimit())await processGreetingTrigger({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat});if(inserted&&!ours&&!autoGreetingTrigger)await db.captureHumanReplyLearning({chatId,eventId:ev.eventId,responseText:ev.text}).catch(()=>{});if(inserted&&allowTakeover&&!ours&&!autoGreetingTrigger&&ageSeconds(ev.createdAt)<=config.humanTakeoverMinutes*60)await db.setHumanTakeover(chatId,'agent_reply_livechat');return inserted;}
async function ensureFreshWelcomeGreeting(chatId,events,livechat){const fresh=[...(events||[])].reverse().find(ev=>isWelcomeTriggerEvent(ev)&&ageSeconds(ev.createdAt)<=greetingTriggerAgeLimit());if(!fresh)return null;await db.insertMessage({chatId,eventId:fresh.eventId,senderType:'system',authorId:fresh.authorId||'system',text:fresh.text,normalizedText:normalizeText(fresh.text),intent:'GREETING_TRIGGER',createdAt:fresh.createdAt,attachments:fresh.attachments||[]}).catch(()=>{});return processGreetingTrigger({chatId,eventId:fresh.eventId,threadId:fresh.threadId,text:fresh.text,createdAt:fresh.createdAt,livechat}).catch(async e=>{await db.logError('poller','WELCOME_GREETING_RETRY_FAILED',e.message,{chatId,eventId:fresh.eventId}).catch(()=>{});return{error:e.message};});}
async function bootstrapChat(chatId,chat,events,livechat){let inserted=0,processed=0;await ensureFreshWelcomeGreeting(chatId,events,livechat);const latest=events.at(-1);for(const ev of events.slice(0,-1)){const type=senderType(ev,chat);if (isWelcomeTriggerEvent(ev)){if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true}))inserted++;}else if(type==='customer'){if(await db.insertMessage({chatId,eventId:ev.eventId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]}))inserted++;}else if(type==='agent'){if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true}))inserted++;}}if(latest){const type=senderType(latest,chat);if (isWelcomeTriggerEvent(latest)){if(await ingestAgentEvent(chatId,latest,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true}))inserted++;}else if(type==='customer'&&ageSeconds(latest.createdAt)<=config.bootstrapReplyMaxAgeSeconds){const r=await processCustomerMessage({chatId,eventId:latest.eventId,threadId:latest.threadId,text:latest.text,createdAt:latest.createdAt,livechat,attachments:latest.attachments||[]});if(!r?.skipped)processed++;if(r?.skipped!=='duplicate')inserted++;}else if(type==='customer'){if(await db.insertMessage({chatId,eventId:latest.eventId,senderType:'customer',authorId:latest.authorId,text:latest.text,normalizedText:normalizeText(latest.text),intent:detectIntent(latest.text),createdAt:latest.createdAt,attachments:latest.attachments||[]}))inserted++;}else if(type==='agent'){if(await ingestAgentEvent(chatId,latest,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true}))inserted++;}}await db.markBootstrapped(chatId);return{inserted,processed};}

async function processChatJob(job,livechat){
  const payload=job.payload||{},summary=payload.summary||{},chatId=String(job.chat_id||summary.id||'');
  if(!chatId)throw new Error('LIVECHAT_SYNC_JOB_CHAT_ID_MISSING');
  const rank=Number.isFinite(Number(payload.rank))?Number(payload.rank):null;
  const fp=String(payload.fingerprint||summaryFingerprint(summary));
  const generation=Math.max(0,Number(payload.generation)||0);
  let state={...livechat.chatState(summary),rank};

  if(state.isClosed||state.isArchived){
    await db.closeConversationFromLiveChat(chatId,state.reason,{...state,archived:state.isArchived});
    rememberFingerprint(chatId,fp);
    return {closed:true,archived:state.isArchived};
  }

  await db.upsertConversation(summary,{visible:state.shouldBeVisibleInInbox,state,generation});
  await db.updateTypingFromSummary(chatId,summary).catch(()=>{});
  const dbState=await db.getConversationState(chatId);
  if(['closed','archived'].includes(String(dbState?.status||'').toLowerCase())){
    incLiveChatMetric('staleResponseRejected');
    rememberFingerprint(chatId,fp);
    return {closed:true,staleActiveRejected:true};
  }

  let chat=summary;
  if(!Array.isArray(chat?.threads)||chat.threads.length===0){chat=await livechat.getChat(chatId,summary);pollMetrics.requestCount++;}
  if(!chat?.id)throw new Error('LIVECHAT_EMPTY_CHAT_DETAIL');
  state={...livechat.chatState(chat),rank};
  if(state.isClosed||state.isArchived){
    await db.closeConversationFromLiveChat(chatId,state.reason,{...state,archived:state.isArchived});
    rememberFingerprint(chatId,fp);
    return {closed:true,archived:state.isArchived};
  }
  await db.upsertConversation(chat,{visible:state.shouldBeVisibleInInbox,state,generation});

  const events=extractChatEvents(chat);
  if(!events.length){await db.clearBootstrapped(chatId);throw new Error('LIVECHAT_EMPTY_READABLE_EVENTS');}
  await ensureFreshWelcomeGreeting(chatId,events,livechat);
  let newMessages=0,processed=0,bootstrapped=0;
  if(!dbState?.bootstrapped_at||Number(dbState?.message_count||0)===0){
    const b=await bootstrapChat(chatId,chat,events,livechat);newMessages+=b.inserted;processed+=b.processed;bootstrapped=1;rememberFingerprint(chatId,fp);return{newMessages,processed,bootstrapped};
  }
  const unseen=[];for(const ev of events)if(!(await db.messageExists(chatId,ev.eventId)))unseen.push(ev);
  const newest=unseen.at(-1)||null;
  for(const ev of unseen){
    const type=senderType(ev,chat),isNewest=newest&&ev.eventId===newest.eventId;
    if(isWelcomeTriggerEvent(ev)){if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true}))newMessages++;}
    else if(type==='customer'&&isNewest){if(ageSeconds(ev.createdAt)*1000<config.memberDebounceMs)throw Object.assign(new Error('MEMBER_DEBOUNCE_PENDING'),{retrySoon:true});const r=await processCustomerMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat,attachments:ev.attachments||[]});if(!r?.skipped)processed++;if(r?.skipped!=='duplicate')newMessages++;}
    else if(type==='customer'){if(await db.insertMessage({chatId,eventId:ev.eventId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]}))newMessages++;}
    else if(type==='agent'){if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true}))newMessages++;}
  }
  await ensureFreshWelcomeGreeting(chatId,events,livechat);rememberFingerprint(chatId,fp);return{newMessages,processed,bootstrapped};
}

async function runWorkerBatch(livechat){if(workerRunning||workerStopping)return;workerRunning=true;workerMetrics.lastRunAt=new Date().toISOString();const started=Date.now();try{await db.requeueStaleProcessingJobs({olderThanSeconds:90}).catch(()=>{});const jobs=await db.claimProcessingJobs('LIVECHAT_SYNC_QUEUE',{limit:config.workerBatchSize,workerId});if(!jobs.length)return;let next=0;const count=Math.max(1,Math.min(config.lcPollConcurrency,jobs.length));workerMetrics.activeWorkers=count;const worker=async()=>{while(true){const i=next++;if(i>=jobs.length)return;const job=jobs[i];try{await processChatJob(job,livechat);await db.completeProcessingJob(job.id);workerMetrics.processed++;}catch(e){workerMetrics.failed++;await db.retryProcessingJob(job.id,e.message,{baseMs:e?.retrySoon?Math.max(250,config.memberDebounceMs):config.queueRetryBaseMs});workerMetrics.retried++;if(!e?.retrySoon)await db.logError('livechat_worker','JOB_FAILED',e.message,{jobId:job.id,chatId:job.chat_id,attempts:job.attempts}).catch(()=>{});}}};await Promise.all(Array.from({length:count},worker));}finally{workerMetrics.activeWorkers=0;workerMetrics.lastDurationMs=Date.now()-started;workerRunning=false;}}
function scheduleWorker(livechat,delay=50){if(workerStopping)return;clearTimeout(workerTimer);workerTimer=setTimeout(async()=>{try{await runWorkerBatch(livechat);}catch(e){lastError=e.message;}finally{scheduleWorker(livechat,workerRunning?100:200);}},delay);workerTimer?.unref?.();}

export async function syncOnce(livechat,{manual=false}={}){
  if(!manual){const enabled=Boolean(await db.getSetting('system_enabled',true));if(!enabled){paused=true;lastResult={ok:true,paused:true,skipped:'system_off'};return lastResult;}}
  paused=false;if(running){pollMetrics.overlapPrevented++;return{skipped:'already_running'};}
  running=true;lastError=null;const started=Date.now();
  try{
    const data=await livechat.listChats();pollMetrics.requestCount++;
    const inventory=data?._normalizedChats||data?.chats_summary||data?.chats||[];
    const hot=data?._hotChats||inventory;
    const completedGeneration=Number(data?._inventoryGeneration||0);
    const deepGen=Number(data?._deepGeneration||completedGeneration||0);
    const fullSweepReady=data?._inventoryComplete!==false&&completedGeneration>0&&completedGeneration!==lastDeepGeneration;
    const providerSet=fullSweepReady?inventory:hot;

    let closedFromProvider=0,archivedFromProvider=0,closedHiddenThisCycle=0,archivedHiddenThisCycle=0;
    // Explicit terminal state is authoritative even when the overall inventory is partial.
    for(const summary of providerSet){
      if(!summary?.id)continue;
      const lc=normalizeChatLifecycle(summary);
      if(!lc.isClosed&&!lc.isArchived)continue;
      if(lc.isArchived)archivedFromProvider++;else closedFromProvider++;
      const changed=await db.closeConversationFromLiveChat(String(summary.id),lc.reason,{...lc,archived:lc.isArchived});
      if(changed){if(lc.isArchived)archivedHiddenThisCycle++;else closedHiddenThisCycle++;}
      summaryFingerprints.delete(String(summary.id));
    }

    const discovery=livechat.filterInbox(fullSweepReady?inventory:hot);
    const authoritative=fullSweepReady?livechat.filterInbox(inventory):[];
    const seenIds=(fullSweepReady?authoritative:discovery).map(x=>String(x?.id||'')).filter(Boolean);
    if(seenIds.length)await db.touchInboxConversations(seenIds,fullSweepReady?completedGeneration:0);

    let enqueued=0,unchanged=0;
    const chats=discovery;
    for (const [rank, summary] of chats.entries()){
      if(!summary?.id)continue;
      const chatId=String(summary.id),fp=summaryFingerprint(summary);
      if(knownFingerprint(chatId)===fp){unchanged++;continue;}
      const row=await db.enqueueProcessingJob({queueName:'LIVECHAT_SYNC_QUEUE',jobKey:`${chatId}:${fp}`,chatId,priority:0,payload:{summary,rank,fingerprint:fp,generation:fullSweepReady?completedGeneration:0},maxAttempts:config.queueRetryLimit});
      if(row?.inserted||['PENDING','RETRY'].includes(String(row?.status||'')))enqueued++;
    }

    const seenChatIds=seenIds;
    let reconcile={candidates:0,reconciled:0};
    if(fullSweepReady){
      reconcile=await db.reconcileInboxVisibility(seenChatIds,25,completedGeneration);
      // compatibility marker: reconcileInboxVisibility(seenChatIds,25)
      lastDeepGeneration=completedGeneration;
      const set=new Set(seenIds);for(const id of summaryFingerprints.keys())if(!set.has(id))summaryFingerprints.delete(id);
    }
    const invalidRepaired=await db.repairInvalidInboxStates();
    cleanupFingerprintCache();
    const actualVisibleInbox=await db.actualVisibleInboxCount();
    const q=await db.processingQueueMetrics().catch(()=>[]),qm=q.find(x=>x.queue_name==='LIVECHAT_SYNC_QUEUE')||{};
    const providerMyActiveChats=livechat.filterInbox(inventory).length;
    const runtimeMetrics=liveChatRuntimeMetrics();
    const expectedVisibleInbox=fullSweepReady?authoritative.length:providerMyActiveChats;
    const mismatch=fullSweepReady?actualVisibleInbox-expectedVisibleInbox:0;
    lastTick=new Date().toISOString();lastSuccess=lastTick;
    lastResult={ok:true,listSource:data?._listSource||'unknown',pagesFetched:Number(data?._pageCount||1),rawChats:inventory.length,deduplicatedChats:inventory.length,myActiveChats:providerMyActiveChats,providerMyActiveChats,activeFromProvider:providerMyActiveChats,closedFromProvider,archivedFromProvider,expectedVisibleInbox,actualVisibleInbox,visibleInboxActual:actualVisibleInbox,visibleInbox:actualVisibleInbox,discoveryChats:chats.length,enqueued,unchanged,inventorySize:Number(data?._inventorySize||inventory.length),inventoryComplete:data?._inventoryComplete!==false,deepSyncRunning:Boolean(data?._deepSyncRunning),deepGeneration:deepGen,deepCompletedGeneration:completedGeneration,deepLastSuccess:data?._deepLastSuccess||null,deepLastError:data?._deepLastError||data?._deepSyncError||null,closedHiddenThisCycle,archivedHiddenThisCycle,reconcileCandidates:Number(reconcile?.candidates||0),reconciledClosed:Number(reconcile?.reconciled||0),reconciledRestored:0,invalidInboxRepaired:invalidRepaired,staleResponseRejected:runtimeMetrics.staleResponseRejected,send403RequesterNotUser:runtimeMetrics.send403RequesterNotUser,queueDepth:Number(qm.depth||0),oldestJobAgeMs:Number(qm.oldest_job_age_ms||0),paginationDurationMs:Date.now()-started,durationMs:Date.now()-started,fetchErrors:0,fetchTimeouts:pollMetrics.fetchTimeouts,inboxMismatch:mismatch};
    await db.setIntegrationHealth('livechat_discovery',{status:mismatch===0||!fullSweepReady?'OK':'WARNING',latencyMs:Date.now()-started,error:mismatch===0?'':`${Math.abs(mismatch)} inbox lifecycle rows differ from provider`,meta:lastResult}).catch(()=>{});
    await db.setIntegrationHealth('livechat_reconciliation',{status:data?._deepLastError?'WARNING':'OK',latencyMs:Date.now()-started,error:data?._deepLastError||'',meta:{inventoryComplete:lastResult.inventoryComplete,deepGeneration:deepGen,actualVisibleInbox,reconcileCandidates:lastResult.reconcileCandidates,reconciledClosed:lastResult.reconciledClosed}}).catch(()=>{});
    await db.setIntegrationHealth('livechat_poll',{status:'OK',latencyMs:Date.now()-started,meta:lastResult}).catch(()=>{});
    return lastResult;
  }catch(e){
    lastError=e.message;pollMetrics.errors++;if(e?.name==='AbortError')pollMetrics.fetchTimeouts++;
    await db.setIntegrationHealth('livechat_poll',{status:'ERROR',latencyMs:Date.now()-started,error:e.message}).catch(()=>{});
    await db.logError('poller','SYNC_FAILED',e.message);throw e;
  }finally{running=false;}
}

export function startPoller(livechat){if(config.lcSyncMode!=='polling')return;workerStopping=false;scheduleWorker(livechat,250);const run=async()=>{try{await syncOnce(livechat);}catch{}finally{timer=setTimeout(run,config.lcPollMs);timer?.unref?.();}};timer=setTimeout(run,1200);timer?.unref?.();}
export async function stopPoller({waitMs=5000}={}){if(timer)clearTimeout(timer);timer=null;if(workerTimer)clearTimeout(workerTimer);workerTimer=null;workerStopping=true;const until=Date.now()+waitMs;while((running||workerRunning)&&Date.now()<until)await new Promise(r=>setTimeout(r,50));return!(running||workerRunning);}
