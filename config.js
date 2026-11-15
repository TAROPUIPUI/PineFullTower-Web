/* Same-origin /signal is the default. Supply ephemeral TURN credentials from your host if needed. */
window.PineFullConfig = {
  signaling: '',
  iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
};

