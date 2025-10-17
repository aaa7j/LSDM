function highlight(text, query) {
  if (!text || !query) return text || '';
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const tokens = Array.from(new Set(query.trim().toLowerCase().split(/\s+/).filter(Boolean)));
  if (!tokens.length) return text;
  const re = new RegExp('(' + tokens.map(esc).join('|') + ')', 'ig');
  return (text + '').replace(re, '<mark>$1</mark>');
}

function setLoading(el, on, text) {
  if (!el) return;
  const msg = (typeof text === 'string' && text) ? text : 'Searching…';
  el.innerHTML = on ? '<span class="spinner"></span> ' + msg : '';
}

// Button ripple (small touch)
document.addEventListener('click', (e) => {
  const btn = e.target.closest('.btn');
  if (!btn) return;
  const ripple = document.createElement('span');
  ripple.style.position = 'absolute';
  ripple.style.inset = '0';
  ripple.style.borderRadius = 'inherit';
  ripple.style.background = 'radial-gradient(circle at var(--rx,50%) var(--ry,50%), rgba(255,255,255,.35), transparent 40%)';
  ripple.style.pointerEvents = 'none';
  const rect = btn.getBoundingClientRect();
  ripple.style.setProperty('--rx', ((e.clientX-rect.left)/rect.width*100)+'%');
  ripple.style.setProperty('--ry', ((e.clientY-rect.top)/rect.height*100)+'%');
  btn.style.position = 'relative';
  btn.appendChild(ripple);
  setTimeout(() => ripple.remove(), 280);
}, true);
