const metrics={staleResponseRejected:0,send403RequesterNotUser:0};
export function incLiveChatMetric(name,n=1){if(Object.hasOwn(metrics,name))metrics[name]+=Number(n)||1;}
export function liveChatRuntimeMetrics(){return {...metrics};}
