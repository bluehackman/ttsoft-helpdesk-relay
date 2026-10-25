const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// In-memory active connections
// id -> { ws, info, peer }
const hosts = new Map();
const viewers = new Map();

// 1. HTTP Server for Health Checks & Keep-Awake Cron
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health' || req.url === '/ping') {
        res.writeHead(200, { 
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify({ 
            status: 'online', 
            service: 'TTSoft Helpdesk Relay',
            activeHosts: hosts.size,
            uptime: Math.round(process.uptime()) + 's',
            timestamp: new Date().toISOString()
        }));
        return;
    }

    res.writeHead(404);
    res.end();
});

// 2. WebSocket Relay Server
const wss = new WebSocketServer({ server });

function generateId() {
    let id = '';
    do {
        const n = Math.floor(100000000 + Math.random() * 900000000);
        id = n.toString();
    } while (hosts.has(id));
    return id;
}

wss.on('connection', (ws, req) => {
    let clientRole = null; // 'host' or 'viewer'
    let clientId = null;
    let peerWs = null;

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (data, isBinary) => {
        // If already paired with a peer, pipe all traffic with maximum speed
        if (peerWs && peerWs.readyState === WebSocket.OPEN) {
            peerWs.send(data, { binary: isBinary });
            return;
        }

        // Before pairing, handle JSON control messages
        if (!isBinary) {
            try {
                const msg = JSON.parse(data.toString());

                // Host registration
                if (msg.type === 'register_host') {
                    clientRole = 'host';
                    clientId = msg.id || generateId();
                    hosts.set(clientId, { ws, name: msg.hostname || 'Bilinmeyen PC' });
                    
                    ws.send(JSON.stringify({
                        type: 'registered',
                        id: clientId,
                        message: 'Host basariyla kaydedildi'
                    }));
                    console.log(`[+] Host baglandi: ${clientId} (${msg.hostname || ''})`);
                    return;
                }

                // Viewer connect request
                if (msg.type === 'connect_viewer') {
                    clientRole = 'viewer';
                    const targetId = msg.targetId?.replace(/\s+/g, '');
                    console.log(`[?] Viewer baglanti istegi -> Hedef ID: ${targetId}`);

                    const hostEntry = hosts.get(targetId);
                    if (!hostEntry || hostEntry.ws.readyState !== WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            code: 'HOST_OFFLINE',
                            message: 'Hedef bilgisayar su anda cevrimdisi veya ID yanlis!'
                        }));
                        return;
                    }

                    // Pair Host and Viewer
                    peerWs = hostEntry.ws;
                    hostEntry.peer = ws;

                    // Tell Host a viewer has connected
                    hostEntry.ws.send(JSON.stringify({
                        type: 'incoming_connection',
                        viewerName: msg.viewerName || 'TTSoft Yetkilisi'
                    }));

                    // Tell Viewer the connection is established
                    ws.send(JSON.stringify({
                        type: 'connected',
                        hostName: hostEntry.name
                    }));

                    // Store mutual peer reference
                    ws.peerWs = hostEntry.ws;
                    hostEntry.ws.peerWs = ws;

                    console.log(`[OK] Eslesme tamamlandi: ${targetId} <---> Viewer`);
                    return;
                }
            } catch (err) {
                console.error('Mesaj parse hatasi:', err);
            }
        }
    });

    ws.on('close', () => {
        if (clientRole === 'host' && clientId) {
            hosts.delete(clientId);
            console.log(`[-] Host ayrildi: ${clientId}`);
        }

        // If paired peer exists, notify them
        const peer = ws.peerWs || peerWs;
        if (peer && peer.readyState === WebSocket.OPEN) {
            peer.send(JSON.stringify({
                type: 'peer_disconnected',
                message: 'Karsi taraf baglantiyi kapatti.'
            }));
            peer.peerWs = null;
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket hatasi:', err);
    });
});

// Keep-alive ping interval to prevent idle timeouts
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

wss.on('close', () => clearInterval(interval));

server.listen(PORT, () => {
    console.log(`===============================================`);
    console.log(`🚀 TTSoft Helpdesk Relay Server Calisiyor!`);
    console.log(`📡 Port: ${PORT}`);
    console.log(`🩺 Saglik Kontrolu (Health): http://localhost:${PORT}/health`);
    console.log(`===============================================`);
});
