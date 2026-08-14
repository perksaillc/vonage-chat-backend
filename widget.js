(function () {
  // 1. Persist a session ID across page reloads so the same thread continues
  let sessionId = localStorage.getItem('chatSessionId');
  if (!sessionId) {
    sessionId = 'sess_' + Math.random().toString(36).slice(2) + Date.now();
    localStorage.setItem('chatSessionId', sessionId);
  }

  // 2. Point this at your deployed Railway URL once you have it
  const BACKEND_URL = 'https://YOUR-RAILWAY-URL.up.railway.app';

  let visitorName = localStorage.getItem('chatVisitorName') || '';
  let ws;

  function connect() {
    const wsUrl = BACKEND_URL.replace('https', 'wss')
      + '/ws?sessionId=' + encodeURIComponent(sessionId)
      + '&name=' + encodeURIComponent(visitorName);
    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.from === 'agent') {
        appendMessage('agent', data.text);
      }
    };

    ws.onclose = () => {
      setTimeout(connect, 2000); // basic auto-reconnect
    };
  }
  connect();

  // 3. Call this when the visitor sets their name (optional, but helps the agent)
  function setName(name) {
    visitorName = name;
    localStorage.setItem('chatVisitorName', name);
  }

  // 4. Call this when the visitor hits "send" in your chat widget
  function sendMessage(message) {
    appendMessage('visitor', message);
    fetch(BACKEND_URL + '/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, name: visitorName, message }),
    });
  }

  // 5. Replace this with code that actually renders into your chat window's DOM
  function appendMessage(from, text) {
    console.log(from + ': ' + text);
    // Example:
    // const el = document.createElement('div');
    // el.className = from === 'agent' ? 'chat-msg agent' : 'chat-msg visitor';
    // el.textContent = text;
    // document.getElementById('chat-messages').appendChild(el);
  }

  // Expose so your widget's button onClick can call these
  window.vonageChat = { sendMessage, setName };
})();
