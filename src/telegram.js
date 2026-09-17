import { config } from './config.js';
import { AsyncLimiter } from './async-limiter.js';

const telegramLimiter=new AsyncLimiter({name:'telegram',concurrency:config.telegramWorkerConcurrency});
export function telegramLimiterMetrics(){return telegramLimiter.metrics();}
export class TelegramClient {
  constructor(token=''){ this.token=String(token||'').trim(); }
  ready(){ return /^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(this.token); }
  base(){ if(!this.ready()) throw new Error('TELEGRAM_BOT_TOKEN_INVALID'); return `https://api.telegram.org/bot${this.token}`; }
  async call(method,payload={},timeoutMs=30000){
    const priority=method==='answerCallbackQuery'?0:(method==='sendMessage'||method==='sendPhoto'?20:50);
    return telegramLimiter.run(async()=>{
      let lastErr=null;
      for(let attempt=0;attempt<3;attempt++){
        const ctrl=new AbortController(); const timer=setTimeout(()=>ctrl.abort(),timeoutMs);
        try{
          const r=await fetch(`${this.base()}/${method}`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload),signal:ctrl.signal});
          const d=await r.json().catch(()=>({}));
          if(r.ok&&d.ok)return d.result;
          const e=new Error(`TELEGRAM_${r.status||'ERROR'}: ${d.description||'Request failed'}`);e.status=r.status;e.telegram=d;lastErr=e;
          if(![429,500,502,503,504].includes(Number(r.status))||attempt>=2)throw e;
          const retryAfter=Number(d?.parameters?.retry_after||r.headers.get('retry-after')||0);const wait=retryAfter>0?retryAfter*1000:Math.min(5000,500*(2**attempt)+Math.floor(Math.random()*250));
          await new Promise(resolve=>setTimeout(resolve,wait));
        }catch(e){lastErr=e;if(e?.status||attempt>=2||e?.name==='AbortError')throw e;await new Promise(resolve=>setTimeout(resolve,500*(2**attempt)+Math.floor(Math.random()*250)));}
        finally{clearTimeout(timer)}
      }
      throw lastErr||new Error('TELEGRAM_REQUEST_FAILED');
    },{priority});
  }
  getMe(){ return this.call('getMe',{},15000); }
  async deleteWebhook(){ return this.call('deleteWebhook',{drop_pending_updates:false},15000); }
  async sendMessage(chatId,text,{topicId=null,replyTo=null,parseMode=null,replyMarkup=null}={}){
    const p={chat_id:String(chatId),text:String(text),disable_web_page_preview:true};
    if(topicId) p.message_thread_id=Number(topicId);
    if(replyTo) p.reply_parameters={message_id:Number(replyTo),allow_sending_without_reply:true};
    if(parseMode) p.parse_mode=parseMode;
    if(replyMarkup) p.reply_markup=replyMarkup;
    return this.call('sendMessage',p,20000);
  }
  async sendPhoto(chatId,photo,{caption='',topicId=null,replyTo=null,parseMode=null,replyMarkup=null}={}){
    const p={chat_id:String(chatId),photo:String(photo),caption:String(caption||'').slice(0,1024)};
    if(topicId) p.message_thread_id=Number(topicId);
    if(replyTo) p.reply_parameters={message_id:Number(replyTo),allow_sending_without_reply:true};
    if(parseMode) p.parse_mode=parseMode;
    if(replyMarkup) p.reply_markup=replyMarkup;
    return this.call('sendPhoto',p,30000);
  }
  async answerCallbackQuery(id,text=''){ return this.call('answerCallbackQuery',{callback_query_id:String(id),text:String(text||'').slice(0,180)},3000); }
  async editMessageText(chatId,messageId,text,{replyMarkup=null}={}){const p={chat_id:String(chatId),message_id:Number(messageId),text:String(text),disable_web_page_preview:true};if(replyMarkup)p.reply_markup=replyMarkup;return this.call('editMessageText',p,15000);}
  async clearMessageButtons(chatId,messageId){return this.call('editMessageReplyMarkup',{chat_id:String(chatId),message_id:Number(messageId),reply_markup:{inline_keyboard:[]}},10000);}
  async getUpdates({offset=0,timeout=20}={}){
    return this.call('getUpdates',{offset:Number(offset)||0,timeout,allowed_updates:['message','edited_message','callback_query']},(timeout+8)*1000);
  }
}
