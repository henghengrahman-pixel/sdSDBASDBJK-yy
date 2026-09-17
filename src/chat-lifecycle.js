const CLOSED = new Set(['closed','inactive','ended','resolved']);
const ARCHIVED = new Set(['archived','archive']);

function norm(v){ return String(v ?? '').trim().toLowerCase(); }
function bool(v){ return typeof v === 'boolean' ? v : null; }

export function normalizeChatLifecycle(chat={}) {
  const threads = Array.isArray(chat?.threads) ? chat.threads : [];
  const lastThread = chat?.last_thread_summary || chat?.last_thread || threads.at(-1) || {};
  const routingStatus = norm(chat?.routing_status || lastThread?.routing_status || chat?.routing?.status);
  const chatStatus = norm(chat?.status || chat?.state || lastThread?.status || lastThread?.state);
  const active = bool(lastThread?.active) ?? bool(chat?.active);
  const isFollowed = bool(chat?.is_followed);
  const archivedFlag = bool(chat?.archived) === true || bool(lastThread?.archived) === true;
  const explicitArchived = archivedFlag || ARCHIVED.has(routingStatus) || ARCHIVED.has(chatStatus);
  const explicitClosed = !explicitArchived && (active === false || CLOSED.has(routingStatus) || CLOSED.has(chatStatus));
  const explicitActive = !explicitClosed && !explicitArchived && (active === true || routingStatus === 'active' || chatStatus === 'active');

  let shouldBeVisibleInInbox = false;
  let reason = 'NOT_ACTIVE';
  if (explicitArchived) reason = 'EXPLICIT_ARCHIVED';
  else if (explicitClosed) reason = active === false ? 'EXPLICIT_ACTIVE_FALSE' : 'EXPLICIT_CLOSED';
  else if (isFollowed === false) reason = 'NOT_FOLLOWED';
  else if (explicitActive) { shouldBeVisibleInInbox = true; reason = isFollowed === true ? 'FOLLOWED_ACTIVE' : 'ACTIVE_FALLBACK'; }

  const updatedAt = chat?.updated_at || chat?.last_thread_summary?.updated_at || lastThread?.updated_at || lastThread?.created_at || null;
  return {
    isActive: explicitActive,
    isClosed: explicitClosed,
    isArchived: explicitArchived,
    isFollowed,
    routingStatus,
    chatStatus,
    shouldBeVisibleInInbox,
    reason,
    providerUpdatedAt: updatedAt ? new Date(updatedAt).toISOString() : null
  };
}

export function isExplicitTerminalLifecycle(x={}) {
  const s = x?.shouldBeVisibleInInbox === undefined ? normalizeChatLifecycle(x) : x;
  return Boolean(s.isClosed || s.isArchived);
}
