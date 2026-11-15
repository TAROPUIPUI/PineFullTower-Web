(() => {
  'use strict';
  const encoder = new TextEncoder();
  const queue = [];
  const peers = new Map();
  let socket, desired, room, local, token, reconnectTimer, disconnectAt, requestVersion = 0;
  let reconnecting = false;

  function enqueue(frame) {
    if (queue.length >= 256) queue.splice(queue.findIndex(item => item[0] === 2) < 0 ? 0 : queue.findIndex(item => item[0] === 2), 1);
    queue.push(frame);
  }
  function notify(data) {
    const raw = encoder.encode(JSON.stringify(data));
    const frame = new Uint8Array(1 + raw.length);
    frame[0] = 1;
    frame.set(raw, 1);
    enqueue(frame);
  }
  function dispatch(slot, data) {
    if (!(data instanceof ArrayBuffer) || data.byteLength < 1 || data.byteLength > 64000) return;
    const frame = new Uint8Array(2 + data.byteLength);
    frame[0] = 2;
    frame[1] = slot;
    frame.set(new Uint8Array(data), 2);
    enqueue(frame);
  }

  function endpoint() {
    const config = window.PineFullConfig || {};
    if (config.signaling) return new URL(config.signaling, location.href).href.replace(/^http/, 'ws');
    const url = new URL('signal', location.href);
    url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return url.href;
  }

  function sendSignal(payload) {
    if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(payload));
  }

  function open(host, code, name) {
    close();
    desired = { type: host ? 'create' : 'join', code, name };
    connect(++requestVersion);
  }
  function connect(version) {
    if (version !== requestVersion || !desired) return;
    try { socket = new WebSocket(endpoint()); }
    catch {
      notify({type:'error', message:'The signaling endpoint is invalid.'});
      return;
    }
    const current = socket;
    current.onopen = () => {
      if (current !== socket) return;
      sendSignal(token ? { type:'resume', code:room, token } : desired);
    };
    current.onmessage = event => {
      if (current !== socket || typeof event.data !== 'string' || event.data.length > 32768) return;
      let message;
      try { message=JSON.parse(event.data); } catch { return; }
      if (!message || typeof message.type !== 'string') return;
      if (message.type === 'room') {
        const resumed = reconnecting;
        room=message.code; token=message.token; local=message.slot;
        reconnecting=false;
        if (!resumed) notify({ type:'room', code:room, slot:local, members:message.members });
        else {
          notify({type:'roster', members:message.members});
          notify({type:'notice', message:'Signaling reconnected'});
          if (local !== 0 && !peers.get(0)?.connected) sendSignal({type:'renegotiate'});
        }
        return;
      }
      if (message.type === 'peer') {
        if (local === 0 && !peers.get(message.slot)?.connected) offer(message.slot);
        return;
      }
      if (message.type === 'renegotiate') {
        if (local === 0) offer(message.slot);
        return;
      }
      if (message.type === 'signal') { signal(message); return; }
      if (message.type === 'peer-left') {
        drop(message.slot, false);
        notify(message);
        return;
      }
      if (message.type === 'closed' || message.type === 'error') {
        close();
        notify(message);
        return;
      }
      if (message.type === 'roster') notify(message);
    };
    current.onclose = () => {
      if (current !== socket || !desired) return;
      if (!room) {
        notify({type:'error', message:'Could not reach the room server. Solo play is available from the title.'});
        return;
      }
      if (!reconnecting) {
        disconnectAt=Date.now();
        reconnecting=true;
        notify({type:'notice', message:'Room server interrupted. Reconnecting…'});
      }
      if (Date.now()-disconnectAt > 19000) {
        close();
        notify({type:'closed', message:'The room server stopped responding.'});
        return;
      }
      reconnectTimer=setTimeout(() => connect(version), 1300);
    };
    current.onerror = () => current.close();
  }

  function close() {
    desired=null; room=null; token=null; local=null;
    reconnecting=false;
    queue.length=0;
    clearTimeout(reconnectTimer);
    ++requestVersion;
    for (const slot of [...peers.keys()]) drop(slot, false);
    if (socket) {
      const current=socket;
      socket=null;
      if (current.readyState === WebSocket.OPEN) {
        current.send(JSON.stringify({type:'leave'}));
        current.close(1000, 'leave');
      } else if (current.readyState < WebSocket.CLOSING) current.close();
    }
  }

  function wireChannel(peer, channel) {
    if (channel.label !== 'reliable' && channel.label !== 'motion') { channel.close(); return; }
    peer[channel.label]=channel;
    channel.binaryType='arraybuffer';
    channel.onmessage=event => {
      if (peers.get(peer.slot) === peer) dispatch(peer.slot,event.data);
    };
    channel.onopen=() => {
      if (peers.get(peer.slot) !== peer || peer.connected || peer.reliable?.readyState !== 'open' || peer.motion?.readyState !== 'open') return;
      peer.connected=true;
      clearTimeout(peer.timeout);
      notify({type:'connected',slot:peer.slot});
    };
    channel.onclose=() => {
      if (peers.get(peer.slot) === peer && peer.connected) retry(peer);
    };
  }

  function drop(slot, tell) {
    const peer=peers.get(slot);
    if (!peer) return;
    peers.delete(slot);
    clearTimeout(peer.timeout);
    clearTimeout(peer.retryTimer);
    peer.pc.close();
    if (tell) notify({type:'peer-down',slot});
  }

  function retry(peer) {
    if (peer.retryTimer || peers.get(peer.slot) !== peer) return;
    peer.retryTimer=setTimeout(() => {
      if (peers.get(peer.slot) !== peer) return;
      drop(peer.slot, true);
      if (local === 0) offer(peer.slot);
      else sendSignal({type:'renegotiate'});
    }, 1200);
  }

  function createPeer(slot, gen) {
    drop(slot, true);
    const options = { iceServers: (window.PineFullConfig || {}).iceServers || [] };
    const pc=new RTCPeerConnection(options);
    const peer={slot,gen,pc,ice:[],chain:Promise.resolve(),connected:false,retryTimer:null};
    peers.set(slot,peer);
    peer.timeout=setTimeout(() => {
      if (!peer.connected && peers.get(slot) === peer) retry(peer);
    }, 10500);
    pc.onicecandidate=event => {
      if (event.candidate && peers.get(slot) === peer)
        sendSignal({type:'signal', to:slot, gen, candidate:event.candidate.toJSON()});
    };
    pc.onconnectionstatechange=() => {
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') retry(peer);
      else if (pc.connectionState === 'disconnected' && !peer.retryTimer) {
        peer.retryTimer=setTimeout(() => {
          peer.retryTimer=null;
          if (pc.connectionState === 'disconnected') retry(peer);
        }, 3800);
      }
    };
    pc.ondatachannel=event => wireChannel(peer,event.channel);
    return peer;
  }

  async function offer(slot) {
    if (!Number.isInteger(slot) || slot < 1 || slot > 7 || local !== 0 || socket?.readyState !== WebSocket.OPEN) return;
    let peer;
    try {
      const gen=Math.floor(Math.random()*0x7fffffff)+1;
      peer=createPeer(slot,gen);
      wireChannel(peer,peer.pc.createDataChannel('reliable',{ordered:true}));
      wireChannel(peer,peer.pc.createDataChannel('motion',{ordered:false,maxRetransmits:0}));
      const description=await peer.pc.createOffer();
      if (peers.get(slot)!==peer) return;
      await peer.pc.setLocalDescription(description);
      if (peers.get(slot)===peer)
        sendSignal({type:'signal',to:slot,gen,description:peer.pc.localDescription.toJSON()});
    } catch { if (peer) retry(peer); }
  }

  const preOfferIce = new Map();
  function signal(message) {
    const slot=message.from;
    if (!Number.isInteger(slot) || slot<0 || slot>7 || !Number.isInteger(message.gen)) return;
    const key=slot+':'+message.gen;
    let peer=peers.get(slot);
    if (local !== 0 && slot === 0 && message.description?.type === 'offer' && peer?.gen !== message.gen) {
      try { peer=createPeer(slot,message.gen); }
      catch {
        notify({type:'error',message:'This browser could not initialize the WebRTC connection.'});
        return;
      }
      peer.ice.push(...(preOfferIce.get(key)||[]));
      preOfferIce.clear();
    }
    if (!peer || peer.gen !== message.gen) {
      if (local !== 0 && slot === 0 && message.candidate && preOfferIce.size<4) {
        const list=preOfferIce.get(key)||[];
        if (list.length<24) list.push(message.candidate);
        preOfferIce.set(key,list);
      }
      return;
    }
    peer.chain=peer.chain.then(async () => {
      if (peers.get(slot)!==peer) return;
      if (message.description) {
        await peer.pc.setRemoteDescription(message.description);
        const pending=peer.ice.splice(0);
        for (const candidate of pending) await peer.pc.addIceCandidate(candidate).catch(() => {});
        if (message.description.type === 'offer') {
          await peer.pc.setLocalDescription(await peer.pc.createAnswer());
          sendSignal({type:'signal',to:slot,gen:peer.gen,description:peer.pc.localDescription.toJSON()});
        }
      }
      if (message.candidate) {
        if (peer.pc.remoteDescription) await peer.pc.addIceCandidate(message.candidate).catch(() => {});
        else if (peer.ice.length<32) peer.ice.push(message.candidate);
      }
    }).catch(() => retry(peer));
  }

  async function copy(code) {
    const url=new URL(location.href);
    url.searchParams.set('room',code);
    try {
      if (!navigator.clipboard) throw new Error('Clipboard unavailable');
      await navigator.clipboard.writeText(url.href);
      window.pineToast?.('Invite link copied');
    } catch {
      const field=document.createElement('textarea');
      field.value=url.href;
      field.style.cssText='position:fixed;opacity:0;pointer-events:none';
      document.body.appendChild(field);
      field.select();
      let copied=false;
      try { copied=document.execCommand('copy'); } catch {}
      field.remove();
      if (copied) window.pineToast?.('Invite link copied');
      else window.prompt('Copy this room invite',url.href);
    }
  }

  window.PineLinks = {
    open,close,copy,read:() => queue.shift(),
    send(slot,reliable,data) {
      const recipients=slot < 0 ? peers.values() : [peers.get(slot)];
      for (const peer of recipients) {
        const channel=reliable?peer?.reliable:peer?.motion;
        if (!peer?.connected || channel?.readyState !== 'open' || channel.bufferedAmount > (reliable?192000:23000)) continue;
        try { channel.send(data); } catch { retry(peer); }
      }
    },
    visibility(hidden) {
      if (hidden || !desired) return;
      if (socket?.readyState > WebSocket.OPEN && !reconnectTimer) connect(requestVersion);
      if (local !== 0 && peers.has(0) && !peers.get(0).connected) sendSignal({type:'renegotiate'});
    }
  };
  addEventListener('pagehide',close);
})();

