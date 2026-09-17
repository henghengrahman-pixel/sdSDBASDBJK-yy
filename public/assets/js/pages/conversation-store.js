export function createConversationStore(){
  const s={selectedChatId:'',conversationMap:new Map(),orderedIds:[],messageMap:new Map(),closedTombstones:new Set(),connectionState:'RECONNECTING'};
  return {
    state:s,
    replace(items=[]){const live=items.filter(x=>!s.closedTombstones.has(String(x.chat_id))&&x.visible_in_inbox!==false&&!['closed','archived'].includes(String(x.status||'').toLowerCase()));s.conversationMap=new Map(live.map(x=>[String(x.chat_id),x]));s.orderedIds=live.map(x=>String(x.chat_id));return this.items()},
    merge(items=[]){const live=items.filter(x=>!s.closedTombstones.has(String(x.chat_id))&&x.visible_in_inbox!==false&&!['closed','archived'].includes(String(x.status||'').toLowerCase()));for(const x of live)s.conversationMap.set(String(x.chat_id),x);for(const x of live){const id=String(x.chat_id);if(!s.orderedIds.includes(id))s.orderedIds.push(id)}return this.items()},
    items(){return s.orderedIds.map(id=>s.conversationMap.get(id)).filter(Boolean)},
    get(id){return s.conversationMap.get(String(id))||null},
    upsert(item){const id=String(item.chat_id);s.conversationMap.set(id,{...(s.conversationMap.get(id)||{}),...item});if(!s.orderedIds.includes(id))s.orderedIds.push(id);return s.conversationMap.get(id)},
    select(id){s.selectedChatId=String(id||'');return s.selectedChatId},
    remove(id){id=String(id);s.closedTombstones.add(id);s.conversationMap.delete(id);s.orderedIds=s.orderedIds.filter(x=>x!==id);if(s.selectedChatId===id)s.selectedChatId='';s.messageMap.delete(id)},
    reorder(ids){s.orderedIds=ids.filter(id=>s.conversationMap.has(String(id))).map(String);return this.items()},
    resetMessages(chatId,items=[]){const map=new Map();for(const item of items){const id=String(item.id||item.event_id||'');if(id)map.set(id,item)}s.messageMap.set(String(chatId),map);return map},
    hasMessage(chatId,messageId){return s.messageMap.get(String(chatId))?.has(String(messageId))||false},
    addMessage(chatId,item){const chatKey=String(chatId);let map=s.messageMap.get(chatKey);if(!map){map=new Map();s.messageMap.set(chatKey,map)}const id=String(item.id||item.event_id||'');if(!id||map.has(id))return false;map.set(id,item);return true},
    clearMessages(chatId){s.messageMap.delete(String(chatId))}
  };
}
