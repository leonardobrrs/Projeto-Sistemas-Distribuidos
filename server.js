const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    const filePath = path.join(__dirname, 'client.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end('Not found');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocket.Server({ server });

const clients = new Map();
let clientCounter = 0;

function broadcast(data, excludeId = null) {
  const message = JSON.stringify(data);
  clients.forEach((info, ws) => {
    if (info.id !== excludeId && ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

function broadcastAll(data) {
  const message = JSON.stringify(data);
  clients.forEach((info, ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

function getConnectedUsers() {
  const users = [];
  clients.forEach((info) => {
    users.push({ id: info.id, name: info.name });
  });
  return users;
}

wss.on('connection', (ws) => {
  clientCounter++;
  const clientId = clientCounter;
  const clientName = `Usuário ${clientId}`;

  clients.set(ws, { id: clientId, name: clientName });

  console.log(`[CONEXÃO] ${clientName} conectado. Total: ${clients.size}`);

  ws.send(JSON.stringify({
    type: 'welcome',
    id: clientId,
    name: clientName,
    message: `Bem-vindo, ${clientName}! Você está conectado ao servidor WebSocket.`,
    users: getConnectedUsers()
  }));

  broadcast({
    type: 'user_joined',
    id: clientId,
    name: clientName,
    message: `${clientName} entrou na sala.`,
    users: getConnectedUsers()
  }, clientId);

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }

    const senderInfo = clients.get(ws);

    if (data.type === 'chat') {
      const payload = {
        type: 'chat',
        id: senderInfo.id,
        name: senderInfo.name,
        message: data.message,
        timestamp: new Date().toLocaleTimeString('pt-BR')
      };
      console.log(`[MSG] ${senderInfo.name}: ${data.message}`);
      broadcastAll(payload);

    } else if (data.type === 'rename') {
      const oldName = senderInfo.name;
      senderInfo.name = data.name || senderInfo.name;
      clients.set(ws, senderInfo);
      broadcastAll({
        type: 'rename',
        id: senderInfo.id,
        oldName,
        newName: senderInfo.name,
        message: `${oldName} agora se chama ${senderInfo.name}.`,
        users: getConnectedUsers()
      });

    } else if (data.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    clients.delete(ws);
    console.log(`[DESCONEXÃO] ${info.name}. Total: ${clients.size}`);
    broadcastAll({
      type: 'user_left',
      id: info.id,
      name: info.name,
      message: `${info.name} saiu da sala.`,
      users: getConnectedUsers()
    });
  });

  ws.on('error', (err) => {
    console.error(`[ERRO] ${err.message}`);
  });
});

server.listen(PORT, () => {
  console.log(`\n======================================`);
  console.log(`  Servidor WebSocket rodando!`);
  console.log(`  HTTP:      http://localhost:${PORT}`);
  console.log(`  WebSocket: ws://localhost:${PORT}`);
  console.log(`======================================\n`);
});
