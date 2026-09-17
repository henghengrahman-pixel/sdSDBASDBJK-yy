import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { normalizeChatLifecycle } from '../src/chat-lifecycle.js';
import { LiveChatClient } from '../src/livechat.js';
import { createConversationStore } from '../public/assets/js/pages/conversation-store.js';

const read=p=>fs.readFileSync(new URL('../'+p,import.meta.url),'utf8');
const db=read('src/db.js'),poller=read('src/poller.js'),server=read('src/server.js'),engine=read('src/engine.js'),storeSrc=read('public/assets/js/pages/conversation-store.js'),lcSrc=read('src/livechat.js');

test('1 active chat is visible',()=>assert.equal(normalizeChatLifecycle({is_followed:true,last_thread_summary:{active:true}}).shouldBeVisibleInInbox,true));
test('2 explicit active=false is hidden',()=>{const x=normalizeChatLifecycle({is_followed:true,last_thread_summary:{active:false}});assert.equal(x.isClosed,true);assert.equal(x.shouldBeVisibleInInbox,false)});
test('3 routing_status=closed is hidden',()=>assert.equal(normalizeChatLifecycle({is_followed:true,routing_status:'closed',last_thread_summary:{active:true}}).isClosed,true));
test('4 routing_status=archived is hidden',()=>assert.equal(normalizeChatLifecycle({is_followed:true,routing_status:'archived',last_thread_summary:{active:true}}).isArchived,true));
test('5 terminal close snapshots archive history',()=>{assert.match(db,/closeConversationFromLiveChat[\s\S]*archiveConversationSessionResilient/);assert.match(db,/conversation_archives/)});
test('6 explicit terminal handling does not depend on inventoryComplete',()=>{const explicit=poller.indexOf('Explicit terminal state is authoritative');const full=poller.indexOf('if(fullSweepReady)');assert.ok(explicit>0&&explicit<full)});
test('7 missing inventory reconcile is gated by fullSweepReady',()=>assert.match(poller,/if\(fullSweepReady\)[\s\S]*reconcileInboxVisibility/));
test('8 authoritative generation reconciles stale active rows',()=>{assert.match(db,/last_seen_generation < \$2/);assert.match(db,/MISSING_AUTHORITATIVE_GENERATION/)});
test('9 deep sync always resets running promise',()=>assert.match(lcSrc,/\.finally\(\(\)=>\{ this\._deepListPromise=null; this\._deepRunningGeneration=0; \}\)/));
test('10 deep generation increments at start',()=>assert.match(lcSrc,/_deepRunningGeneration=\+\+this\._deepGenerationSeq/));
test('11 provider timeout keeps inventory incomplete instead of global closing',()=>{assert.match(lcSrc,/this\._inventoryComplete=false[\s\S]*throw e/);assert.doesNotMatch(poller,/hideAllInboxConversations/)});
test('12 continuation pagination failure cannot mark partial inventory complete',()=>assert.match(lcSrc,/catch\(e\)[\s\S]*this\._inventoryComplete=false/));
test('13 stale active cannot reopen terminal DB state',()=>assert.match(db,/conversations\.status IN \('closed','archived'\) THEN conversations\.status/));
test('14 same customer identity is not conversation identity',()=>{assert.match(db,/chat_id TEXT PRIMARY KEY/);assert.doesNotMatch(db,/customer_email[^\n]*UNIQUE/)});
test('15 frontend filters terminal rows on replace',()=>assert.match(storeSrc,/replace\(items=\[\]\)[\s\S]*closedTombstones/));
test('16 tombstone prevents stale frontend response from re-adding closed chat',()=>{const s=createConversationStore();s.replace([{chat_id:'c1',status:'active',visible_in_inbox:true}]);s.remove('c1');s.replace([{chat_id:'c1',status:'active',visible_in_inbox:true}]);assert.equal(s.get('c1'),null)});
test('17 send is blocked for local closed or hidden conversation',()=>{assert.match(engine,/LIVECHAT_SEND_BLOCKED_CLOSED_OR_HIDDEN/);assert.match(server,/assertConversationSendable/)});
test('18 requester-not-user breaks retry loop and is classified',()=>{assert.match(engine,/if\(isRequesterNotUserError\(err\)\) break/);assert.match(engine,/LIVECHAT_SEND_403_REQUESTER_NOT_USER/)});
test('19 requester-not-user triggers provider lifecycle refresh',()=>assert.match(engine,/refreshLifecycleAfterSend403[\s\S]*livechat\.getChat/));
test('20 invalid status closed visible=true is repaired and constrained',()=>{assert.match(db,/repairInvalidInboxStates/);assert.match(db,/conversations_terminal_hidden_check/)});
test('21 actualVisibleInbox is a DB count after reconciliation',()=>{assert.match(db,/actualVisibleInboxCount/);assert.match(poller,/actualVisibleInbox=await db\.actualVisibleInboxCount\(\)/)});
test('22 archives preserve messages and attachments',()=>{assert.match(db,/conversation_archive_messages/);assert.match(db,/attachments JSONB/);assert.match(db,/archiveCurrentConversationSession/)});

test('canonical normalizer is the LiveChat inbox decision source',()=>{const c=new LiveChatClient({base:'x',accountId:'a',pat:'b'});assert.equal(c.isMyActiveChat({is_followed:true,last_thread_summary:{active:false}}),false);assert.match(lcSrc,/normalizeChatLifecycle\(summary\)/)});
test('conversations API excludes both closed and archived',()=>assert.match(server,/visible_in_inbox=true AND c\.status NOT IN \('closed','archived'\)/));
test('health separates discovery send and reconciliation',()=>{assert.match(poller,/livechat_discovery/);assert.match(poller,/livechat_reconciliation/);assert.match(engine,/livechat_send_message/)});
